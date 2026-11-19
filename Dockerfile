# The container where Spark runs the job. Java 17: Spark 4 needs Java 17 or 21.
FROM eclipse-temurin:17-jre-noble

# uv, pinned. It also provides the Python of .python-version.
COPY --from=ghcr.io/astral-sh/uv:0.13.5 /uv /uvx /bin/

# The Cloud Storage connector for Hadoop: the shaded JAR of release 4.0.6, from Maven Central.
# Its version and digest are pinned here and nowhere else. The job's session loads it from the
# path below.
ADD --checksum=sha256:553c4d8d0d22b41f0bfc4b090ca4c5f0d4b741e9d1c177a5a941dc03aa175a8d \
    https://repo1.maven.org/maven2/com/google/cloud/bigdataoss/gcs-connector/4.0.6/gcs-connector-4.0.6-shaded.jar \
    /opt/jars/gcs-connector-shaded.jar

WORKDIR /app

# Every COPY names its files: none copies the whole build context, so that no credential file
# of the working tree can reach the image. pyspark and delta-spark come from the dev group.
COPY pyproject.toml uv.lock .python-version /app/
RUN uv sync --locked

COPY src/ /app/src/

# Keeps the container running, so that `docker compose exec` runs the job.
CMD ["sleep", "infinity"]
