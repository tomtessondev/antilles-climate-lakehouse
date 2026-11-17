from __future__ import annotations

from collections.abc import Iterator

import pytest
from pyspark.sql import SparkSession

from antilles_lakehouse.spark import local_session


@pytest.fixture(scope="session")
def spark(tmp_path_factory: pytest.TempPathFactory) -> Iterator[SparkSession]:
    session = local_session(tmp_path_factory.mktemp("warehouse"))
    yield session
    session.stop()
