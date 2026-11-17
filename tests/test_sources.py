"""Finding the right Météo-France files through the data.gouv.fr API, and landing them."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path

import httpx
import respx

from antilles_lakehouse.sources import Resource, climate_resources, fetch_dataset, land

FIXTURES = Path(__file__).parent / "fixtures"


def dataset() -> dict[str, object]:
    loaded: dict[str, object] = json.loads((FIXTURES / "dataset.json").read_text())
    return loaded


def test_only_daily_rain_temperature_wind_files_of_971_and_972() -> None:
    resources = climate_resources(dataset())
    assert sorted(r.title for r in resources) == [
        "QUOT_departement_971_periode_1950-2024_RR-T-Vent",
        "QUOT_departement_971_periode_2025-2026_RR-T-Vent",
        "QUOT_departement_972_periode_1950-2024_RR-T-Vent",
        "QUOT_departement_972_periode_2025-2026_RR-T-Vent",
    ]
    assert {r.department for r in resources} == {"971", "972"}
    assert {r.period for r in resources} == {"1950-2024", "2025-2026"}
    assert all(r.last_modified.tzinfo is not None for r in resources)


@respx.mock
def test_fetch_dataset_reads_the_climate_dataset() -> None:
    route = respx.get("https://www.data.gouv.fr/api/1/datasets/6569b51ae64326786e4e8e1a/").mock(
        return_value=httpx.Response(200, json=dataset())
    )
    with httpx.Client() as client:
        assert fetch_dataset(client)["id"] == dataset()["id"]
    assert route.called


@respx.mock
def test_land_streams_and_fingerprints_each_file(tmp_path: Path) -> None:
    resource = Resource(
        id="r-971",
        title="QUOT_departement_971_periode_2025-2026_RR-T-Vent",
        url="https://example.org/Q_971_latest-2025-2026_RR-T-Vent.csv.gz",
        last_modified=datetime(2026, 11, 13, tzinfo=UTC),
        department="971",
        period="2025-2026",
    )
    body = b"\x1f\x8b fake gzip bytes"
    respx.get(resource.url).mock(return_value=httpx.Response(200, content=body))
    with httpx.Client() as client:
        (landed,) = land([resource], tmp_path, client)
    assert landed.resource == resource
    assert landed.path == tmp_path / "r-971.csv.gz"
    assert landed.path.read_bytes() == body
    assert landed.sha1 == hashlib.sha1(body).hexdigest()
