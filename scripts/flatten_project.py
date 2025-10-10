#!/usr/bin/env python3
"""Emit a single self-contained Python module for interactive use."""

from __future__ import annotations

import argparse
import json
import textwrap
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
PACKAGE_ROOT = PROJECT_ROOT / "salam_ingest"
DEFAULT_OUTPUT = PROJECT_ROOT / "dist" / "salam_ingest_flattened.py"


def _module_name(path: Path) -> str:
    rel = path.relative_to(PROJECT_ROOT)
    parts = list(rel.with_suffix("").parts)
    if parts and parts[-1] == "__init__":
        parts = parts[:-1]
    return ".".join(parts)


def _iter_python_files() -> list[Path]:
    pkg_files: list[Path] = []
    for path in PACKAGE_ROOT.rglob("*.py"):
        if "__pycache__" in path.parts:
            continue
        pkg_files.append(path)
    # Load packages before modules to satisfy dependencies
    pkg_files.sort(key=lambda p: (_module_name(p).count("."), _module_name(p)))
    pkg_files.append(PROJECT_ROOT / "ingestion.py")
    return pkg_files


TEMPLATE_HEADER = textwrap.dedent(
    """
    # Auto-generated single-file build.
    import types as _types
    import sys as _sys

    def _ensure_package(name: str) -> None:
        if not name or name in _sys.modules:
            return
        if "." in name:
            parent, child = name.rsplit(".", 1)
            _ensure_package(parent)
        module = _types.ModuleType(name)
        module.__path__ = []  # namespace package marker
        _sys.modules[name] = module
        if "." in name:
            setattr(_sys.modules[parent], child, module)

    def _load_module(name: str, code: str) -> None:
        package = name.rsplit(".", 1)[0] if "." in name else ""
        if package:
            _ensure_package(package)
        module = _types.ModuleType(name)
        module.__package__ = package
        _sys.modules[name] = module
        if package:
            parent, child = name.rsplit(".", 1)
            setattr(_sys.modules[parent], child, module)
        exec(code, module.__dict__)

    """
)


def build_flat_module(output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    files = _iter_python_files()
    with output.open("w", encoding="utf-8") as fh:
        fh.write(TEMPLATE_HEADER)
        for file in files:
            module_name = _module_name(file) or file.stem
            source = file.read_text(encoding="utf-8")
            payload = json.dumps(source)
            fh.write(f"\n_load_module(\"{module_name}\", {payload})\n")
    print(f"Wrote flattened module to {output}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "output",
        type=Path,
        nargs="?",
        default=DEFAULT_OUTPUT,
        help=f"Destination path (default: {DEFAULT_OUTPUT})",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    build_flat_module(args.output)


if __name__ == "__main__":
    main()
