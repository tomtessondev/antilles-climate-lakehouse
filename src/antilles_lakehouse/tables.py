"""Where the tables and the landed files live: Unity Catalog on Databricks, a local folder
for `make run-local`. The layers only ever see table names."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Tables:
    bronze: str
    silver_daily: str
    silver_stations: str
    gold_prefix: str
    landing: Path
    # Unity Catalog objects to create on Databricks; None locally.
    catalog: str | None = None
    schema: str | None = None

    @classmethod
    def local(cls, root: Path) -> Tables:
        return cls("bronze_daily", "silver_daily", "silver_stations", "gold", root / "landing")

    @classmethod
    def unity(cls, catalog: str = "antilles", schema: str = "climate") -> Tables:
        name = f"{catalog}.{schema}"
        return cls(
            f"{name}.bronze_daily",
            f"{name}.silver_daily",
            f"{name}.silver_stations",
            f"{name}.gold",
            Path(f"/Volumes/{catalog}/{schema}/landing"),
            catalog,
            schema,
        )
