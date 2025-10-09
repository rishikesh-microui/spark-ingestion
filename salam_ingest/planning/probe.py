from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional

from ..endpoints.base import SourceEndpoint
from .base import Probe


@dataclass
class RowCountProbe(Probe):
    lower: str
    upper: Optional[str] = None

    def run(self, endpoint: SourceEndpoint) -> Dict[str, int]:
        count = endpoint.count_between(lower=self.lower, upper=self.upper)
        return {"rows": count, "lower": self.lower, "upper": self.upper}
