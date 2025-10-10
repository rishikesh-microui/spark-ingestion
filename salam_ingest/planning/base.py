from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Protocol

from ..endpoints.base import SourceEndpoint


@dataclass
class PlannerRequest:
    schema: str
    table: str
    load_date: str
    mode: str  # "full" | "scd1" | ...
    last_watermark: Optional[str] = None
    lag_seconds: int = 0
    table_cfg: Dict[str, Any] = field(default_factory=dict)


@dataclass
class IngestionPlan:
    """Result of planning step."""

    schema: str
    table: str
    mode: str
    probes: List["Probe"] = field(default_factory=list)
    slices: List[Dict[str, str]] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def with_slices(self, slices: List[Dict[str, str]]) -> "IngestionPlan":
        self.slices = slices
        return self

    def add_probe(self, probe: "Probe") -> None:
        self.probes.append(probe)


class Probe(Protocol):
    def run(self, endpoint: SourceEndpoint) -> Dict[str, Any]: ...


class Planner(Protocol):
    def build_plan(
        self,
        endpoint: SourceEndpoint,
        request: PlannerRequest,
    ) -> IngestionPlan: ...


class PlannerRegistry:
    def __init__(self) -> None:
        self._registry: Dict[str, Planner] = {}

    def register(self, key: str, planner: Planner) -> None:
        self._registry[key.lower()] = planner

    def get(self, key: str) -> Planner:
        return self._registry[key.lower()]


REGISTRY = PlannerRegistry()
