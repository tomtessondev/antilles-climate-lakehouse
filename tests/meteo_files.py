"""Small files in Météo-France's exact daily format (header, `;`, gzip), for the tests."""

from __future__ import annotations

import gzip
import hashlib
from datetime import UTC, datetime
from pathlib import Path

from antilles_lakehouse.sources import LandedFile, Resource

# The real header of the files, all 60 columns (rain, temperature, wind and their codes).
HEADER = (  # noqa: SIM905 - 60 names read better laid out like the file header
    "NUM_POSTE NOM_USUEL LAT LON ALTI AAAAMMJJ RR QRR TN QTN HTN QHTN TX QTX HTX QHTX TM QTM "
    "TNTXM QTNTXM TAMPLI QTAMPLI TNSOL QTNSOL TN50 QTN50 DG QDG FFM QFFM FF2M QFF2M FXY QFXY "
    "DXY QDXY HXY QHXY FXI QFXI DXI QDXI HXI QHXI FXI2 QFXI2 DXI2 QDXI2 HXI2 QHXI2 FXI3S "
    "QFXI3S DXI3S QDXI3S HXI3S QHXI3S DRR QDRR STATUS_FXI3S STATUS_DXI3S"
).split()

STATIONS = {
    "97101014": ("LES ABYMES CHAZEAU_SAPC", "16.287500", "-61.453167", "95"),
    "97123001": ("ST-BARTHELEMY", "17.901500", "-62.850667", "48"),
    "97127002": ("ST-MARTIN", "18.043333", "-63.111667", "3"),
    "97209001": ("LE LAMENTIN-AERO", "14.595333", "-60.995667", "3"),
}


def row(station: str, day: str, **values: str) -> dict[str, str]:
    """One daily row; measures given as e.g. TX="29.3", QTX="1"."""
    name, lat, lon, alti = STATIONS[station]
    base = {"NUM_POSTE": station, "NOM_USUEL": name, "LAT": lat, "LON": lon, "ALTI": alti}
    return base | {"AAAAMMJJ": day} | values


def write(path: Path, rows: list[dict[str, str]]) -> Path:
    lines = [";".join(HEADER)] + [";".join(r.get(c, "") for c in HEADER) for r in rows]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(gzip.compress(("\n".join(lines) + "\n").encode("utf-8"), mtime=0))
    return path


def landed(
    path: Path,
    resource_id: str,
    period: str = "2025-2026",
    department: str = "971",
    modified: datetime = datetime(2026, 11, 1, tzinfo=UTC),
) -> LandedFile:
    resource = Resource(
        id=resource_id,
        title=f"QUOT_departement_{department}_periode_{period}_RR-T-Vent",
        url=f"https://example.org/{resource_id}.csv.gz",
        last_modified=modified,
        department=department,
        period=period,
    )
    return LandedFile(resource, path, hashlib.sha1(path.read_bytes()).hexdigest())
