from __future__ import annotations

from dataclasses import asdict, is_dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional


def safe_upper(value: Any) -> Any:
    if isinstance(value, str):
        return value.upper()
    return value


def escape_literal(value: str) -> str:
    if not isinstance(value, str):
        return value
    return value.replace("'", "''")


def row_to_dict(row: Any) -> Dict[str, Any]:
    if row is None:
        return {}
    if isinstance(row, dict):
        return row
    if hasattr(row, "asDict"):
        try:
            return row.asDict(recursive=True)
        except TypeError:
            return row.asDict()
    if hasattr(row, "_asdict"):
        return dict(row._asdict())
    if isinstance(row, (list, tuple)):
        return {str(idx): to_serializable(val) for idx, val in enumerate(row)}
    return {"value": to_serializable(row)}


def collect_rows(result: Any) -> List[Dict[str, Any]]:
    if result is None:
        return []
    rows: Optional[List[Any]] = None
    collector = getattr(result, "collect", None)
    if callable(collector):
        try:
            rows = list(collector())
        except TypeError:
            rows = list(collector)
    if rows is None:
        if isinstance(result, list):
            rows = result
        elif isinstance(result, tuple):
            rows = list(result)
        elif hasattr(result, "__iter__"):
            rows = list(result)
        else:
            rows = [result]
    return [row_to_dict(row) for row in rows or []]


def to_serializable(value: Any) -> Any:
    if is_dataclass(value):
        return to_serializable(asdict(value))
    if isinstance(value, Decimal):
        try:
            if value == value.to_integral():
                return int(value)
        except Exception:
            pass
        return float(value)
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc).isoformat()
    if isinstance(value, dict):
        return {key: to_serializable(val) for key, val in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [to_serializable(item) for item in value]
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except Exception:
            return str(value)
    if isinstance(value, bytes):
        return value.hex()
    return value


__all__ = ["collect_rows", "escape_literal", "row_to_dict", "safe_upper", "to_serializable"]
