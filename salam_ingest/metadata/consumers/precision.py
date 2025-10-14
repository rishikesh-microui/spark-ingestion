from __future__ import annotations

from dataclasses import asdict, dataclass, is_dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional, Tuple

from ..core import MetadataTarget
from ..core.interfaces import MetadataRepository


@dataclass
class PrecisionSpec:
    column: str
    precision: Optional[int]
    scale: Optional[int]
    target_type: str = "numeric"


@dataclass
class PrecisionIssue:
    column: str
    precision: Optional[int]
    scale: Optional[int]
    reason: str
    handled: bool = False
    action: Optional[str] = None


@dataclass
class PrecisionGuardrailResult:
    target: MetadataTarget
    status: str
    issues: List[PrecisionIssue]
    cast_specs: Dict[str, PrecisionSpec]
    snapshot: Optional[Dict[str, Any]] = None

    def to_payload(self) -> Dict[str, Any]:
        return {
            "target": {"namespace": self.target.namespace, "entity": self.target.entity},
            "status": self.status,
            "issues": [asdict(issue) for issue in self.issues],
            "cast_specs": {name: asdict(spec) for name, spec in self.cast_specs.items()},
            "snapshot": self.snapshot,
        }


class PrecisionGuardrailEvaluator:
    """Evaluate numeric precision risks based on catalog metadata."""

    DEFAULT_MAX_PRECISION = 38
    NUMERIC_TYPES = {"NUMBER", "DECIMAL", "NUMERIC", "FLOAT"}

    def __init__(self, repository: MetadataRepository, *, max_precision: Optional[int] = None) -> None:
        self.repository = repository
        self.max_precision = max_precision or self.DEFAULT_MAX_PRECISION

    def evaluate(
        self,
        target: MetadataTarget,
        *,
        max_precision: Optional[int] = None,
        violation_action: str = "downcast",
        open_precision_action: Optional[str] = None,
        fallback_precision: Optional[int] = None,
        fallback_scale: Optional[int] = None,
    ) -> PrecisionGuardrailResult:
        record = self.repository.latest(target)
        if not record:
            return PrecisionGuardrailResult(
                target=target,
                status="metadata_missing",
                issues=[],
                cast_specs={},
                snapshot=None,
            )
        snapshot = self._snapshot_dict(record.payload)
        if snapshot is None:
            return PrecisionGuardrailResult(
                target=target,
                status="metadata_unusable",
                issues=[],
                cast_specs={},
                snapshot=None,
            )
        fields = self._schema_fields(snapshot)
        configured_max = max_precision if max_precision is not None else self.max_precision
        precision_limit = configured_max if configured_max is not None else self.DEFAULT_MAX_PRECISION
        length_limit = precision_limit
        violation_action = (violation_action or "downcast").lower()
        open_precision_action = (open_precision_action or violation_action).lower()
        issues: List[PrecisionIssue] = []
        cast_specs: Dict[str, PrecisionSpec] = {}
        fatal = False
        for field in fields:
            name = field.get("name")
            if not name:
                continue
            dtype = str(field.get("data_type") or "").upper()
            if dtype not in self.NUMERIC_TYPES:
                continue
            precision = _to_int(field.get("precision") or field.get("data_precision"))
            scale = _to_int(field.get("scale") or field.get("data_scale"))
            key = name.upper()
            stats_obj = field.get("statistics")
            if stats_obj and not isinstance(stats_obj, dict):
                if is_dataclass(stats_obj):
                    stats_obj = asdict(stats_obj)
                else:
                    stats_obj = {
                        attr: getattr(stats_obj, attr)
                        for attr in dir(stats_obj)
                        if not attr.startswith("_") and not callable(getattr(stats_obj, attr))
                    }
            observed_length, observed_sample = _max_observed_numeric_length(stats_obj)
            limit_for_precision = precision_limit
            if limit_for_precision is None:
                limit_for_precision = self.DEFAULT_MAX_PRECISION
            precision_limit_exceeded = precision is not None and precision > limit_for_precision
            observed_limit_exceeded = (
                observed_length is not None
                and length_limit is not None
                and observed_length > length_limit
            )
            if precision_limit_exceeded:
                limit_display = limit_for_precision
                if violation_action == "fail":
                    issues.append(
                        PrecisionIssue(
                            column=name,
                            precision=precision,
                            scale=scale,
                            reason=f"precision {precision} exceeds maximum {limit_display}",
                            handled=False,
                            action="fail",
                        )
                    )
                    fatal = True
                    continue
            if observed_limit_exceeded:
                limit_display = length_limit
                sample_suffix = ""
                if observed_sample:
                    sample_display = observed_sample if len(observed_sample) <= 64 else f"{observed_sample[:61]}..."
                    sample_suffix = f"; sample {sample_display}"
                if violation_action == "fail":
                    issues.append(
                        PrecisionIssue(
                            column=name,
                            precision=precision,
                            scale=scale,
                            reason=(
                                f"observed numeric magnitude requires {observed_length} digits, "
                                f"exceeds maximum {limit_display}{sample_suffix}"
                            ),
                            handled=False,
                            action="fail",
                        )
                    )
                    fatal = True
                    continue
            if precision_limit_exceeded or observed_limit_exceeded:
                reason_parts: List[str] = []
                if precision_limit_exceeded:
                    limit_display = limit_for_precision
                    reason_parts.append(f"precision {precision} exceeds maximum {limit_display}")
                if observed_limit_exceeded:
                    limit_display = length_limit
                    sample_suffix = ""
                    if observed_sample:
                        sample_display = observed_sample if len(observed_sample) <= 64 else f"{observed_sample[:61]}..."
                        sample_suffix = f"; sample {sample_display}"
                    reason_parts.append(
                        f"observed numeric magnitude requires {observed_length} digits, exceeds maximum {limit_display}{sample_suffix}"
                    )
                reason = "; ".join(reason_parts)
                cast_specs[key] = PrecisionSpec(column=name, precision=None, scale=None, target_type="string")
                issues.append(
                    PrecisionIssue(
                        column=name,
                        precision=None,
                        scale=None,
                        reason=reason,
                        handled=True,
                        action="cast_to_string",
                    )
                )
                continue
            if precision is None:
                if open_precision_action == "fail":
                    issues.append(
                        PrecisionIssue(
                            column=name,
                            precision=None,
                            scale=scale,
                            reason="precision undefined; source allows arbitrary precision",
                            handled=False,
                            action="fail",
                        )
                    )
                    fatal = True
                continue
        if fatal:
            status = "fatal"
        elif issues:
            status = "adjusted"
        else:
            status = "ok"
        return PrecisionGuardrailResult(
            target=target,
            status=status,
            issues=issues,
            cast_specs=cast_specs,
            snapshot=snapshot,
        )

    def _snapshot_dict(self, payload: Any) -> Optional[Dict[str, Any]]:
        if isinstance(payload, dict):
            return payload
        if is_dataclass(payload):
            return asdict(payload)
        return None

    def _schema_fields(self, snapshot: Dict[str, Any]) -> List[Dict[str, Any]]:
        fields = snapshot.get("schema_fields") or []
        normalized: List[Dict[str, Any]] = []
        for field in fields:
            if isinstance(field, dict):
                normalized.append(field)
            elif is_dataclass(field):
                normalized.append(asdict(field))
            else:
                attrs = {k: getattr(field, k) for k in dir(field) if not k.startswith("_")}
                normalized.append(attrs)
        return normalized


def _to_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        try:
            return int(float(value))
        except (TypeError, ValueError):
            return None


def _normalize_numeric_string(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, Decimal):
        text = format(value, "f")
    elif isinstance(value, int):
        text = str(value)
    elif isinstance(value, float):
        try:
            text = format(Decimal(str(value)), "f")
        except (InvalidOperation, ValueError):
            text = str(value)
    else:
        text = str(value).strip()
        if not text:
            return None
        normalized_candidate = text.replace("d", "e").replace("D", "E")
        if "E" in normalized_candidate:
            try:
                text = format(Decimal(normalized_candidate), "f")
            except (InvalidOperation, ValueError):
                text = normalized_candidate
    text = text.strip()
    return text or None


def _max_observed_numeric_length(stats: Optional[Dict[str, Any]]) -> Tuple[Optional[int], Optional[str]]:
    if not isinstance(stats, dict):
        return None, None
    max_length: Optional[int] = None
    sample: Optional[str] = None

    def update(length: Optional[int], candidate: Optional[str] = None) -> None:
        nonlocal max_length, sample
        if length is None:
            return
        if max_length is None or length > max_length:
            max_length = length
            sample = candidate
        elif candidate is not None and length == max_length and sample is None:
            sample = candidate

    def consider(value: Any) -> None:
        normalized = _normalize_numeric_string(value)
        if not normalized:
            return
        digits = sum(1 for ch in normalized if ch.isdigit())
        update(digits, normalized)

    for key in ("max_value", "high_value", "maximum", "max"):
        consider(stats.get(key))
    for key in ("min_value", "low_value", "minimum", "min"):
        consider(stats.get(key))
    for key in ("max_value_length", "high_value_length", "min_value_length", "low_value_length"):
        update(_to_int(stats.get(key)))

    extras = stats.get("extras")
    if isinstance(extras, dict):
        for key in ("max_value", "high_value", "maximum", "max", "min_value", "low_value", "minimum", "min"):
            if key in extras:
                consider(extras.get(key))
        for key in ("max_value_length", "high_value_length", "min_value_length", "low_value_length"):
            update(_to_int(extras.get(key)))

    return max_length, sample
