"""Fixtures shared by the job's tests: the one Spark session of the test run."""

from __future__ import annotations

from collections.abc import Iterator
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark() -> Iterator[SparkSession]:
    """The job's own Spark session, with Delta Lake in it, shared by the whole test run.

    It is built by `spark_session.build_session()` with no connector JAR, which exists only in
    the image, so the tests run on the session the job runs with. A JVM loads its packages once,
    when it starts, so this fixture must build the first session of the run: no test builds
    another one. The job's modules are imported here, not at the top of the file, so that a test
    that needs no Spark never imports them through this fixture.
    """
    import spark_session

    session = spark_session.build_session()
    yield session
    session.stop()
