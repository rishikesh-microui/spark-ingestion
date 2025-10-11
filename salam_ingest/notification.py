import json
import threading
import time
from datetime import datetime
from typing import Any, Dict, Iterable, Optional, Set, Tuple, Union

from pyspark.sql import SparkSession

from .common import PrintLogger, RUN_ID, Utils
from .io.filesystem import HDFSUtil


class Notifier:
    """Buffered notification sink with periodic flushes to storage or webhooks."""

    def __init__(
        self,
        spark: SparkSession,
        logger: PrintLogger,
        cfg: Dict[str, Any],
        interval_sec: int = 300,
    ) -> None:
        self.spark = spark
        self.logger = logger
        self.interval = max(30, int(interval_sec))
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._buf: list[Tuple[str, Dict[str, Any]]] = []
        self._lock = threading.Lock()
        runtime = cfg["runtime"]
        notify_cfg = runtime.get("notify", {})
        self.sink = notify_cfg.get("sink", "file")
        self.path = notify_cfg.get("path", "hdfs:///tmp/ingest-notify")
        self.webhook = notify_cfg.get("webhook")

    def emit(self, level: str, payload: Dict[str, Any]) -> None:
        with self._lock:
            self._buf.append((level, payload))

    def _flush(self) -> None:
        with self._lock:
            items = list(self._buf)
            self._buf.clear()
        if not items:
            return
        ts = datetime.now().astimezone().strftime("%Y%m%d-%H%M%S")
        content = "\n".join(json.dumps({"level": lvl, **pl}) for (lvl, pl) in items)
        try:
            HDFSUtil.write_text(
                self.spark,
                f"{self.path}/notify_{RUN_ID}_{ts}.jsonl",
                content,
            )
        except Exception as exc:
            self.logger.error("notify_flush_failed", err=str(exc))
        if self.webhook:
            # Placeholder for webhook dispatch should the environment permit it.
            pass

    def _run(self) -> None:
        while not self._stop.wait(self.interval):
            self._flush()

    def start(self) -> None:
        self._thread = threading.Thread(target=self._run, name="notifier", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._flush()
        if self._thread:
            self._thread.join(timeout=5)


class Heartbeat:
    """Background status reporter that emits regular progress messages."""

    def __init__(
        self,
        logger: PrintLogger,
        interval_sec: int = 120,
        time_sink: Optional[Any] = None,
    ) -> None:
        self.logger = logger
        self.interval = max(5, int(interval_sec))
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self.time_sink = time_sink
        self.total = 0
        self.done = 0
        self.failed = 0
        self.inflight = 0
        self.active_pools: Set[str] = set()

    def update(
        self,
        *,
        total: Optional[int] = None,
        done: Optional[int] = None,
        failed: Optional[int] = None,
        inflight: Optional[int] = None,
        active_pool: Optional[Union[Iterable[str], str]] = None,
    ) -> None:
        if total is not None:
            self.total = total
        if done is not None:
            self.done = done
        if failed is not None:
            self.failed = failed
        if inflight is not None:
            self.inflight = inflight
        if active_pool is not None:
            if isinstance(active_pool, str):
                self.active_pools.add(active_pool)
            else:
                self.active_pools |= set(active_pool)

    def _run(self) -> None:
        while not self._stop.wait(self.interval):
            try:
                snap = Utils.sample_spark(self.logger.spark) if hasattr(self.logger, "spark") else {}
            except Exception:
                snap = {}
            self.logger.info(
                "heartbeat",
                total=self.total,
                done=self.done,
                failed=self.failed,
                inflight=self.inflight,
                active_pools=sorted(self.active_pools),
                **snap,
            )
            if self.time_sink is not None:
                try:
                    self.time_sink.maybe_flush_by_age()
                except Exception:
                    pass

    def start(self) -> None:
        self._thread = threading.Thread(target=self._run, name="heartbeat", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=5)
