"""Météo-France's daily climatological series, as published on data.gouv.fr.

The dataset "Données climatologiques de base - quotidiennes" has one gzip CSV per
department and period, rewritten in place when Météo-France republishes it (the URL stays,
`last_modified` moves) and renamed when a period rolls over each year. The files are found
through the data.gouv.fr API, never by hard-coded URL. Only rain, temperature and wind
("RR-T-Vent") for Guadeloupe (971) and Martinique (972) from 1950 on are used; the sparse
"avant-1949" files are left out.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import httpx

DATASET_URL = "https://www.data.gouv.fr/api/1/datasets/6569b51ae64326786e4e8e1a/"
_TITLE = re.compile(r"^QUOT_departement_(971|972)_periode_(\d{4}-\d{4})_RR-T-Vent$")


@dataclass(frozen=True)
class Resource:
    id: str
    title: str
    url: str
    last_modified: datetime
    department: str
    period: str


@dataclass(frozen=True)
class LandedFile:
    """A downloaded file and the fingerprint of its bytes: the data.gouv.fr API publishes
    no checksum for these files, so the identity of a version is computed here."""

    resource: Resource
    path: Path
    sha1: str


def fetch_dataset(client: httpx.Client) -> dict[str, Any]:
    response = client.get(DATASET_URL, timeout=60)
    response.raise_for_status()
    dataset: dict[str, Any] = response.json()
    return dataset


def climate_resources(dataset: dict[str, Any]) -> list[Resource]:
    resources = []
    for item in dataset["resources"]:
        match = _TITLE.match(item["title"])
        if match:
            resources.append(
                Resource(
                    id=item["id"],
                    title=item["title"],
                    url=item["url"],
                    last_modified=datetime.fromisoformat(item["last_modified"]),
                    department=match.group(1),
                    period=match.group(2),
                )
            )
    return resources


def land(resources: list[Resource], landing: Path, client: httpx.Client) -> list[LandedFile]:
    """Downloads each file to `landing/<resource id>.csv.gz`, fingerprinting it while it
    streams (the history file is tens of megabytes)."""
    landing.mkdir(parents=True, exist_ok=True)
    landed = []
    for resource in resources:
        path = landing / f"{resource.id}.csv.gz"
        digest = hashlib.sha1()
        with client.stream("GET", resource.url, timeout=300, follow_redirects=True) as response:
            response.raise_for_status()
            with path.open("wb") as out:
                for chunk in response.iter_bytes():
                    digest.update(chunk)
                    out.write(chunk)
        landed.append(LandedFile(resource=resource, path=path, sha1=digest.hexdigest()))
    return landed
