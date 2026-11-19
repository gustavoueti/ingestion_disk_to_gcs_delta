"""The job's Spark session: Delta Lake, and the Cloud Storage connector set to ADC.

The session is the job's own code, built before the process runs: no ingestio connector creates
one. Authentication is set here only to Application Default Credentials, which the container's
environment provides; no key, file or token is named in this module.
"""

from __future__ import annotations

from importlib.metadata import version
from pathlib import Path

from pyspark.sql import SparkSession

# Where the image puts the shaded JAR of the Cloud Storage connector. The connector's version is
# pinned in the Dockerfile alone, so the path carries none.
GCS_CONNECTOR_JAR = Path("/opt/jars/gcs-connector-shaded.jar")


def build_session(*, gcs_connector_jar: Path | None = None) -> SparkSession:
    """Build, or return, the session the job runs with.

    Args:
        gcs_connector_jar: The shaded JAR of the Cloud Storage connector. When given it must be an
            existing file, and the session loads it through `spark.jars`. When it is `None`, as in
            the tests, `spark.jars` is not set by the job.

    Raises:
        ValueError: `gcs_connector_jar` is given and is not an existing file.
    """
    if gcs_connector_jar is not None and not gcs_connector_jar.is_file():
        raise ValueError(f"gcs connector jar not found: {gcs_connector_jar}")

    # The coordinate follows the delta-spark package of the lock, the one place of its version.
    delta_package = f"io.delta:delta-spark_2.13:{version('delta-spark')}"
    builder = (
        SparkSession.builder.master("local[2]")
        .appName("ingestion_disk_to_gcs_delta")
        .config("spark.driver.memory", "2g")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.jars.packages", delta_package)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        .config(
            "spark.hadoop.fs.AbstractFileSystem.gs.impl",
            "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS",
        )
        # The connector's default asks the metadata server of a Google Cloud machine.
        .config("spark.hadoop.fs.gs.auth.type", "APPLICATION_DEFAULT")
    )
    if gcs_connector_jar is not None:
        builder = builder.config("spark.jars", str(gcs_connector_jar))
    return builder.getOrCreate()
