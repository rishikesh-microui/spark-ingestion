from __future__ import annotations

from dataclasses import asdict, dataclass, is_dataclass
from typing import Any, Dict, Iterable, List, Optional

from ..core import MetadataTarget
from ..core.interfaces import MetadataRepository


@dataclass
class PrecisionSpec:
    column: str
    precision: Optional[int]
    scale: Optional[int]


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
    NUMERIC_TYPES = {"NUMBER", "DECIMAL", "NUMERIC"}

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
        effective_max = max_precision or self.max_precision
        fallback_precision = fallback_precision or effective_max
        if fallback_precision is None:
            fallback_precision = self.DEFAULT_MAX_PRECISION
        if effective_max is not None:
            fallback_precision = min(fallback_precision, effective_max)
        effective_scale_default = fallback_scale if fallback_scale is not None else min(6, fallback_precision)
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
                adj_precision = fallback_precision
                adj_scale = _choose_scale(scale, adj_precision, effective_scale_default)
                cast_specs[key] = PrecisionSpec(column=name, precision=adj_precision, scale=adj_scale)
                issues.append(
                    PrecisionIssue(
                        column=name,
                        precision=adj_precision,
                        scale=adj_scale,
                        reason=f"precision undefined; downcast to {adj_precision},{adj_scale}",
                        handled=True,
                        action="downcast",
                    )
                )
                continue
            if precision > effective_max:
                if violation_action == "fail":
                    issues.append(
                        PrecisionIssue(
                            column=name,
                            precision=precision,
                            scale=scale,
                            reason=f"precision {precision} exceeds maximum {effective_max}",
                            handled=False,
                            action="fail",
                        )
                    )
                    fatal = True
                    continue
                adj_precision = effective_max
                adj_scale = _choose_scale(scale, adj_precision, effective_scale_default)
                cast_specs[key] = PrecisionSpec(column=name, precision=adj_precision, scale=adj_scale)
                issues.append(
                    PrecisionIssue(
                        column=name,
                        precision=adj_precision,
                        scale=adj_scale,
                        reason=f"precision {precision} exceeds maximum {effective_max}; downcast to {adj_precision},{adj_scale}",
                        handled=True,
                        action="downcast",
                    )
                )
                continue
            if scale is not None and scale > precision:
                adj_scale = _choose_scale(scale, precision, effective_scale_default)
                cast_specs[key] = PrecisionSpec(column=name, precision=precision, scale=adj_scale)
                issues.append(
                    PrecisionIssue(
                        column=name,
                        precision=precision,
                        scale=adj_scale,
                        reason=f"scale {scale} clipped to {adj_scale} to fit precision {precision}",
                        handled=True,
                        action="clip_scale",
                    )
                )
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


def _choose_scale(scale: Optional[int], precision: Optional[int], default_scale: int) -> int:
    if precision is None or precision <= 0:
        precision = PrecisionGuardrailEvaluator.DEFAULT_MAX_PRECISION
    if scale is None:
        scale = min(default_scale, precision)
    else:
        scale = max(0, min(scale, precision))
    return scale
