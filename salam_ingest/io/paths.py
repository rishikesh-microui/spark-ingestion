from __future__ import annotations

from typing import Any, Dict, Tuple


class Paths:
    """Helpers for constructing raw and final storage paths."""

    @staticmethod
    def build(rt: Dict[str, Any], schema: str, table: str, load_date: str) -> Tuple[str, str, str]:
        append_schema = rt.get("append_table_schema", False)
        raw_dir = (
            f"{rt['raw_root']}/{schema}/{table}/load_date={load_date}"
            if append_schema
            else f"{rt['raw_root']}/{table}/load_date={load_date}"
        )
        final_dir = (
            f"{rt['final_root']}/{schema}/{table}"
            if append_schema
            else f"{rt['final_root']}/{table}"
        )
        base_raw = (
            f"{rt['raw_root']}/{schema}/{table}"
            if append_schema
            else f"{rt['raw_root']}/{table}"
        )
        return raw_dir, final_dir, base_raw
