#!/usr/bin/env python3
"""Build a zip archive that can be shipped with spark-submit."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import zipfile

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_OUTPUT = PROJECT_ROOT / "dist" / "salam_ingest_bundle.zip"
DEFAULT_INCLUDES = ["ingestion.py", "salam_ingest"]
EXCLUDE_PATTERNS = {"__pycache__", ".DS_Store", ".git", "dist"}


def should_skip(path: Path) -> bool:
    parts = set(path.parts)
    return bool(parts & EXCLUDE_PATTERNS)


def iter_includes(include_paths: list[str]) -> list[tuple[Path, Path]]:
    files: list[tuple[Path, Path]] = []
    for rel in include_paths:
        src = PROJECT_ROOT / rel
        if not src.exists():
            raise FileNotFoundError(f"Included path not found: {src}")
        if src.is_file():
            files.append((src, src.relative_to(PROJECT_ROOT)))
        else:
            for file in src.rglob("*"):
                if file.is_file() and not should_skip(file):
                    files.append((file, file.relative_to(PROJECT_ROOT)))
    return files


def build_zip(output: Path, includes: list[str]) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    files = iter_includes(includes)
    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for src, arcname in files:
            zf.write(src, arcname)
    print(f"Created {output} ({len(files)} files)")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help="Target zip path (default: dist/salam_ingest_bundle.zip)",
    )
    parser.add_argument(
        "--include",
        action="append",
        default=[],
        metavar="PATH",
        help="Additional top-level paths to include (repeatable)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    includes = DEFAULT_INCLUDES + args.include
    build_zip(args.output, includes)


if __name__ == "__main__":
    main()
