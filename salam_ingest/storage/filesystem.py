from __future__ import annotations

import io
import os
import posixpath
from pathlib import Path
from typing import Iterable, Optional, Tuple

from pyspark.sql import SparkSession


class Filesystem:
    """Abstraction over filesystem protocols (local, HDFS today)."""

    def __init__(self, impl) -> None:
        self._impl = impl

    @property
    def root(self) -> str:
        return self._impl.root

    @classmethod
    def for_root(cls, root: str, spark: Optional[SparkSession] = None) -> "Filesystem":
        if root.startswith("hdfs://"):
            if spark is None:
                spark = SparkSession.getActiveSession()
            if spark is None:
                raise RuntimeError("Spark session required for HDFS filesystem access")
            impl = _HdfsStorage(root.rstrip("/"), spark)
        else:
            impl = _LocalStorage(root)
        return cls(impl)

    @classmethod
    def for_path(cls, path: str, spark: Optional[SparkSession] = None) -> Tuple["Filesystem", str]:
        if path.startswith("hdfs://"):
            root = posixpath.dirname(path) or path
            fs = cls.for_root(root, spark)
            return fs, path
        full = os.path.abspath(path)
        root = os.path.dirname(full) or os.getcwd()
        fs = cls.for_root(root)
        return fs, full

    def join(self, *parts: str) -> str:
        return self._impl.join(*parts)

    def relpath(self, path: str, start: Optional[str] = None) -> str:
        return self._impl.relpath(path, start or self.root)

    def exists(self, path: str) -> bool:
        return self._impl.exists(path)

    def makedirs(self, path: str) -> None:
        self._impl.makedirs(path)

    def write_text(self, path: str, data: str) -> str:
        return self._impl.write_text(path, data)

    def append_text(self, path: str, data: str) -> str:
        return self._impl.append_text(path, data)

    def read_text(self, path: str) -> str:
        return self._impl.read_text(path)

    def write_bytes(self, path: str, data: bytes) -> str:
        return self._impl.write_bytes(path, data)

    def delete(self, path: str, recursive: bool = False) -> None:
        self._impl.delete(path, recursive)

    def rename(self, src: str, dst: str) -> None:
        self._impl.rename(src, dst)

    def listdir(self, path: str) -> Iterable[str]:
        return self._impl.listdir(path)

    def replace(self, src: str, dst: str) -> None:
        self._impl.replace(src, dst)


class _LocalStorage:
    def __init__(self, root: str) -> None:
        self.root = os.path.abspath(root)

    def _full(self, path: str) -> str:
        if not path:
            return self.root
        if os.path.isabs(path):
            return path
        return os.path.join(self.root, path)

    def join(self, *parts: str) -> str:
        parts = [p for p in parts if p]
        if not parts:
            return self.root
        return os.path.join(*parts)

    def relpath(self, path: str, start: str) -> str:
        return os.path.relpath(path, start)

    def exists(self, path: str) -> bool:
        return os.path.exists(self._full(path))

    def makedirs(self, path: str) -> None:
        os.makedirs(self._full(path), exist_ok=True)

    def write_text(self, path: str, data: str) -> str:
        full = self._full(path)
        os.makedirs(os.path.dirname(full), exist_ok=True)
        with open(full, "w", encoding="utf-8") as handle:
            handle.write(data)
        return full

    def append_text(self, path: str, data: str) -> str:
        full = self._full(path)
        os.makedirs(os.path.dirname(full), exist_ok=True)
        with open(full, "a", encoding="utf-8") as handle:
            handle.write(data)
        return full

    def write_bytes(self, path: str, data: bytes) -> str:
        full = self._full(path)
        os.makedirs(os.path.dirname(full), exist_ok=True)
        with open(full, "wb") as handle:
            handle.write(data)
        return full

    def read_text(self, path: str) -> str:
        full = self._full(path)
        with open(full, "r", encoding="utf-8") as handle:
            return handle.read()

    def delete(self, path: str, recursive: bool = False) -> None:
        full = self._full(path)
        if not os.path.exists(full):
            return
        if os.path.isdir(full) and recursive:
            for root, dirs, files in os.walk(full, topdown=False):
                for name in files:
                    os.remove(os.path.join(root, name))
                for name in dirs:
                    os.rmdir(os.path.join(root, name))
            os.rmdir(full)
        elif os.path.isdir(full):
            os.rmdir(full)
        else:
            os.remove(full)

    def rename(self, src: str, dst: str) -> None:
        os.makedirs(os.path.dirname(self._full(dst)), exist_ok=True)
        os.replace(self._full(src), self._full(dst))

    def replace(self, src: str, dst: str) -> None:
        os.makedirs(os.path.dirname(self._full(dst)), exist_ok=True)
        os.replace(self._full(src), self._full(dst))

    def listdir(self, path: str) -> Iterable[str]:
        full = self._full(path)
        if not os.path.exists(full):
            return []
        return os.listdir(full)


class _HdfsStorage:
    def __init__(self, root: str, spark: SparkSession) -> None:
        self.root = root.rstrip("/")
        self.spark = spark
        self._conf = spark._jsc.hadoopConfiguration()
        self._jvm = spark.sparkContext._jvm
        self.Path = self._jvm.org.apache.hadoop.fs.Path
        self.fs = self._jvm.org.apache.hadoop.fs.FileSystem.get(self._conf)
        self.IOUtils = self._jvm.org.apache.hadoop.io.IOUtils

    def _full(self, path: str) -> str:
        if not path:
            return self.root
        if path.startswith("hdfs://"):
            return path.rstrip("/")
        clean = path.lstrip("/")
        base = self.root if self.root else ""
        if base.endswith("/"):
            base = base[:-1]
        return f"{base}/{clean}"

    def _path(self, path: str):
        return self.Path(self._full(path))

    def join(self, *parts: str) -> str:
        clean = []
        for part in parts:
            if not part:
                continue
            if part.startswith("hdfs://"):
                clean = [part.rstrip("/")]
            else:
                clean.append(part.strip("/"))
        if not clean:
            return self.root
        base = clean[0]
        for piece in clean[1:]:
            base = posixpath.join(base, piece)
        return base

    def relpath(self, path: str, start: str) -> str:
        if path.startswith(start):
            rel = path[len(start) :].lstrip("/")
            return rel or "."
        return path

    def exists(self, path: str) -> bool:
        return self.fs.exists(self._path(path))

    def makedirs(self, path: str) -> None:
        p = self._path(path)
        if not self.fs.exists(p):
            self.fs.mkdirs(p)

    def write_text(self, path: str, data: str) -> str:
        return self.write_bytes(path, data.encode("utf-8"))

    def append_text(self, path: str, data: str) -> str:
        data_bytes = data.encode("utf-8")
        full = self._full(path)
        p = self.Path(full)
        parent = p.getParent()
        if parent is not None and not self.fs.exists(parent):
            self.fs.mkdirs(parent)
        if self.fs.exists(p):
            stream = self.fs.append(p)
        else:
            stream = self.fs.create(p, True)
        try:
            stream.write(bytearray(data_bytes))
        finally:
            stream.close()
        return full

    def write_bytes(self, path: str, data: bytes) -> str:
        full = self._full(path)
        p = self.Path(full)
        parent = p.getParent()
        if parent is not None and not self.fs.exists(parent):
            self.fs.mkdirs(parent)
        stream = self.fs.create(p, True)
        try:
            stream.write(bytearray(data))
        finally:
            stream.close()
        return full

    def read_text(self, path: str) -> str:
        p = self._path(path)
        stream = self.fs.open(p)
        baos = self._jvm.java.io.ByteArrayOutputStream()
        try:
            self.IOUtils.copyBytes(stream, baos, self._conf, False)
        finally:
            stream.close()
        return bytes(baos.toByteArray()).decode("utf-8")

    def delete(self, path: str, recursive: bool = False) -> None:
        self.fs.delete(self._path(path), bool(recursive))

    def rename(self, src: str, dst: str) -> None:
        src_path = self._path(src)
        dst_path = self._path(dst)
        parent = dst_path.getParent()
        if parent is not None and not self.fs.exists(parent):
            self.fs.mkdirs(parent)
        if not self.fs.rename(src_path, dst_path):
            raise IOError(f"Unable to rename {src} -> {dst}")

    def replace(self, src: str, dst: str) -> None:
        dst_path = self._path(dst)
        if self.fs.exists(dst_path):
            self.fs.delete(dst_path, True)
        self.rename(src, dst)

    def listdir(self, path: str) -> Iterable[str]:
        p = self._path(path)
        if not self.fs.exists(p):
            return []
        statuses = self.fs.listStatus(p)
        names = []
        for status in statuses:
            names.append(status.getPath().getName())
        return names
