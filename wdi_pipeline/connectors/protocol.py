from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    import duckdb


@dataclass
class DiscoveryResult:
    columns: list[str] = field(default_factory=list)


class ConnectorProtocol(Protocol):
    def discover(self, job: object) -> DiscoveryResult:
        return DiscoveryResult()

    def materialize(self, job: object, conn: "duckdb.DuckDBPyConnection") -> None:
        ...
