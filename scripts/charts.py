"""Charts for the README, drawn from the gold tables of a local run (`make run-local`).

Usage: python scripts/charts.py [DATA_DIR]   (default: data)
Writes docs/images/very_hot_days.png and docs/images/temperature_anomaly.png.
"""

from __future__ import annotations

import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402 - the backend must be chosen first
from pyspark.sql import SparkSession  # noqa: E402

from antilles_lakehouse.spark import local_session  # noqa: E402

TERRITORIES = {"Guadeloupe": "#1b7f5f", "Martinique": "#c2571a"}
IMAGES = Path(__file__).resolve().parents[1] / "docs" / "images"


def very_hot_days(spark: SparkSession, out: Path) -> None:
    rows = spark.sql(
        """select territory, year, mean_per_station, panel_size from gold_extremes
        where indicator = 'very_hot_days' order by year"""
    ).collect()
    fig, ax = plt.subplots(figsize=(9, 4.5))
    for territory, colour in TERRITORIES.items():
        mine = [r for r in rows if r.territory == territory]
        panel = max(r.panel_size for r in mine)
        ax.plot(
            [r.year for r in mine],
            [r.mean_per_station for r in mine],
            color=colour,
            marker="o",
            markersize=3,
            label=f"{territory} ({panel} long-series stations)",
        )
    ax.set_title("Days at or above 32 °C per year, mean per long-series station")
    ax.set_ylabel("days")
    ax.grid(alpha=0.3)
    ax.legend(frameon=False)
    fig.tight_layout()
    fig.savefig(out, dpi=150)


def temperature_anomaly(spark: SparkSession, out: Path) -> None:
    rows = spark.sql(
        """select territory, year, avg(tx_anomaly) as anomaly from gold_anomalies
        where tx_anomaly is not null
          and year <= (select max(year) from gold_extremes)  -- the last complete year
        group by territory, year order by year"""
    ).collect()
    fig, ax = plt.subplots(figsize=(9, 4.5))
    ax.axhline(0, color="#888888", linewidth=0.8)
    for territory, colour in TERRITORIES.items():
        mine = [r for r in rows if r.territory == territory]
        ax.plot([r.year for r in mine], [r.anomaly for r in mine], color=colour, label=territory)
    ax.set_title("Daily maximum temperature: departure from the 1991-2020 normal, yearly mean")
    ax.set_ylabel("°C")
    ax.grid(alpha=0.3)
    ax.legend(frameon=False)
    fig.tight_layout()
    fig.savefig(out, dpi=150)


def main() -> None:
    root = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("data")
    spark = local_session(root / "warehouse", metastore=root / "metastore")
    spark.sparkContext.setLogLevel("ERROR")
    IMAGES.mkdir(parents=True, exist_ok=True)
    very_hot_days(spark, IMAGES / "very_hot_days.png")
    temperature_anomaly(spark, IMAGES / "temperature_anomaly.png")


if __name__ == "__main__":
    main()
