from __future__ import annotations

import argparse
import json
import os
import sys
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, TextIO


def _parse_event(line: str) -> Optional[Dict[str, Any]]:
    line = line.strip()
    if not line:
        return None
    try:
        rec = json.loads(line)
    except json.JSONDecodeError:
        return None
    if not isinstance(rec, dict) or "event" not in rec:
        return None
    return rec


@dataclass
class TableStatus:
    schema: str
    table: str
    mode: str = ""
    load_date: str = ""
    status: str = "pending"
    rows: Optional[int] = None
    duration_sec: Optional[float] = None
    slices_total: Optional[int] = None
    slices_done: int = 0
    last_event_ts: Optional[str] = None
    guardrail_actions: List[str] = field(default_factory=list)
    last_error: Optional[str] = None

    def update(self, event: Dict[str, Any]) -> None:
        self.mode = event.get("mode", self.mode)
        self.load_date = event.get("load_date", self.load_date)
        self.last_event_ts = event.get("ts", self.last_event_ts)
        name = event.get("event")
        if name == "ingest.table.start":
            self.status = "running"
        elif name == "ingest.table.success":
            self.status = "success"
            self.rows = event.get("rows", self.rows)
            self.duration_sec = event.get("duration_sec", self.duration_sec)
        elif name == "ingest.table.failure":
            self.status = "failed"
            self.last_error = event.get("message")
        elif name == "ingest.slice.progress":
            status = event.get("status")
            if status == "completed":
                self.slices_done += 1
            upper = event.get("upper")
            if upper:
                self.slices_total = max(self.slices_total or 0, self.slices_done)
        elif name == "guardrail.precision":
            action = event.get("status", "adjusted")
            self.guardrail_actions.append(action)


def _update_from_events(events: Iterable[Dict[str, Any]], tables: Dict[str, TableStatus]) -> None:
    for event in events:
        schema = event.get("schema")
        table = event.get("table")
        if not schema or not table:
            continue
        key = f"{schema}.{table}"
        if key not in tables:
            tables[key] = TableStatus(schema=schema, table=table)
        tables[key].update(event)


def _print_progress(tables: Dict[str, TableStatus]) -> None:
    headers = ["Table", "Mode", "Status", "Rows", "Slices", "Duration", "Guardrail"]
    lines = ["{:<32} {:<6} {:<9} {:>10} {:>10} {:>9} {:<20}".format(*headers)]
    success = failed = running = 0
    total_rows = 0
    for key in sorted(tables):
        entry = tables[key]
        if entry.status == "success":
            success += 1
        elif entry.status == "failed":
            failed += 1
        elif entry.status == "running":
            running += 1
        if entry.rows:
            total_rows += entry.rows
        slices = ""
        if entry.slices_total:
            slices = f"{entry.slices_done}/{entry.slices_total}"
        guardrail = ",".join(entry.guardrail_actions[-2:]) if entry.guardrail_actions else ""
        lines.append(
            "{:<32} {:<6} {:<9} {:>10} {:>10} {:>9} {:<20}".format(
                key,
                entry.mode or "",
                entry.status,
                entry.rows if entry.rows is not None else "-",
                slices or "-",
                f"{entry.duration_sec:.1f}" if entry.duration_sec else "-",
                guardrail,
            )
        )
        if entry.status == "failed" and entry.last_error:
            lines.append(f"    error: {entry.last_error}")
    lines.append("".ljust(80, "-"))
    summary = f"Completed: {success}  Running: {running}  Failed: {failed}  Rows: {total_rows}"
    lines.append(summary)
    output = "\n".join(lines)
    sys.stdout.write("\033c")  # clear screen
    sys.stdout.write(output + "\n")
    sys.stdout.flush()


def _read_events_from_stream(stream: TextIO) -> Iterable[Dict[str, Any]]:
    while True:
        line = stream.readline()
        if not line:
            break
        event = _parse_event(line)
        if event:
            yield event


def cmd_watch(args: argparse.Namespace) -> None:
    tables: Dict[str, TableStatus] = {}
    interval = float(args.interval)
    log_path = args.log_file
    if log_path:
        position = 0 if args.from_start else (os.path.getsize(log_path) if os.path.exists(log_path) else 0)
        while True:
            new_events: List[Dict[str, Any]] = []
            try:
                with open(log_path, "r", encoding="utf-8") as handle:
                    handle.seek(position)
                    for line in handle:
                        event = _parse_event(line)
                        if event:
                            new_events.append(event)
                    position = handle.tell()
            except FileNotFoundError:
                pass
            if new_events:
                _update_from_events(new_events, tables)
            _print_progress(tables)
            if args.once:
                break
            time.sleep(interval)
    else:
        events = list(_read_events_from_stream(sys.stdin))
        _update_from_events(events, tables)
        _print_progress(tables)


def _aggregate_failures(events: Iterable[Dict[str, Any]]) -> Dict[tuple, Dict[str, Any]]:
    groups: Dict[tuple, Dict[str, Any]] = {}
    for event in events:
        if event.get("event") != "ingest.table.failure":
            continue
        key = (event.get("error_type"), event.get("message"))
        group = groups.setdefault(
            key,
            {
                "count": 0,
                "first_ts": event.get("ts"),
                "tables": [],
                "error_type": event.get("error_type"),
                "message": event.get("message"),
                "error_hash": event.get("error_hash"),
            },
        )
        group["count"] += 1
        table = f"{event.get('schema')}.{event.get('table')}"
        if event.get("slice"):
            table += f" {event['slice']}"
        group["tables"].append(table)
    return groups


def cmd_failures(args: argparse.Namespace) -> None:
    events: List[Dict[str, Any]] = []
    if args.log_file:
        try:
            with open(args.log_file, "r", encoding="utf-8") as handle:
                for line in handle:
                    event = _parse_event(line)
                    if event:
                        events.append(event)
        except FileNotFoundError:
            sys.stderr.write(f"File not found: {args.log_file}\n")
            sys.exit(1)
    else:
        events.extend(_read_events_from_stream(sys.stdin))

    groups = _aggregate_failures(events)
    if not groups:
        print("No failures recorded.")
        return

    print("Error Type            Count  First Seen                 Message")
    print("--------------------  -----  --------------------------  -------")
    for (error_type, message), group in groups.items():
        first_ts = group.get("first_ts", "-")
        print(f"{error_type:<20}  {group['count']:<5}  {first_ts:<26}  {message}")
        if args.show_tables:
            for tbl in group["tables"]:
                print(f"    {tbl}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Telemetry monitoring utilities")
    sub = parser.add_subparsers(dest="command")

    watch = sub.add_parser("watch", help="Watch ingestion progress")
    watch.add_argument("--log-file", help="Path to structured log file to tail")
    watch.add_argument("--interval", default=5, help="Refresh interval in seconds")
    watch.add_argument("--from-start", action="store_true", help="Read log from beginning instead of tail")
    watch.add_argument("--once", action="store_true", help="Print once and exit")
    watch.set_defaults(func=cmd_watch)

    fails = sub.add_parser("failures", help="Summarize failures from telemetry logs")
    fails.add_argument("--log-file", help="Path to structured log file")
    fails.add_argument("--show-tables", action="store_true", help="Show tables associated with each failure")
    fails.set_defaults(func=cmd_failures)

    return parser


def main(argv: Optional[List[str]] = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    if not hasattr(args, "func"):
        parser.print_help()
        return
    args.func(args)


if __name__ == "__main__":
    main()
