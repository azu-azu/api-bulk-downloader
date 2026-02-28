from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class DiscoveryResult:
    columns: list[str] = field(default_factory=list)
