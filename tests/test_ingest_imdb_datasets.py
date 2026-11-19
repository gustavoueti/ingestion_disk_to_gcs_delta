"""Tests of the spec `specs/ingest_imdb_datasets.md`: the IMDb files to Delta Lake tables.

Each test is named after the acceptance criterion it covers. No test reaches GCS or needs a
credential: the process tests give the job's process configurations of their own, under
`tmp_path`, whose targets are local Delta tables (`{"system": "disk", "format": "delta"}`), and
IMDb-shaped files that they write themselves, with rows of their own. The download script runs
against a local HTTP server.

The job's modules, `main` and `spark_session`, are imported inside the tests that use them, and
pyspark only for the type checker, so that a test that needs neither never fails because one of
them is missing.
"""

from __future__ import annotations

import copy
import gzip
import json
import logging
import os
import re
import subprocess
import sys
import threading
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from importlib.metadata import version
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest
from ingestio import FullLoad, Process, ReadSource

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, Row, SparkSession

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
SCRIPT = ROOT / "scripts" / "download_imdb.py"

# The tables in load order, and the IMDb file each one is loaded from.
TABLES = ("title_basics", "title_ratings")
IMDB_NAMES = {"title_basics": "title.basics", "title_ratings": "title.ratings"}

# --------------------------------------------------------------------------------------------
# The shipped configuration, as section Configuration of the spec gives it
# --------------------------------------------------------------------------------------------

BASICS_SCHEMA = (
    "tconst STRING, titleType STRING, primaryTitle STRING, originalTitle STRING, isAdult INT, "
    "startYear INT, endYear INT, runtimeMinutes INT, genres STRING"
)
RATINGS_SCHEMA = "tconst STRING, averageRating DOUBLE, numVotes INT"
TSV_OPTIONS: dict[str, Any] = {
    "sep": "\t",
    "header": True,
    "nullValue": "\\N",
    "quote": "",
    "mode": "FAILFAST",
    "enforceSchema": False,
}
SHIPPED_CONFIGS: dict[str, dict[str, Any]] = {
    "title_basics": {
        "source": {
            "system": "disk",
            "path": "/data/title.basics",
            "format": "csv",
            "schema": BASICS_SCHEMA,
            "options": TSV_OPTIONS,
        },
        "target": {
            "system": "gcs",
            "path": "gs://ingestio-imdb-delta/imdb/title_basics",
            "format": "delta",
        },
        "convert": {"booleans": ["isAdult"], "lists": ["genres"]},
    },
    "title_ratings": {
        "source": {
            "system": "disk",
            "path": "/data/title.ratings",
            "format": "csv",
            "schema": RATINGS_SCHEMA,
            "options": TSV_OPTIONS,
        },
        "target": {
            "system": "gcs",
            "path": "gs://ingestio-imdb-delta/imdb/title_ratings",
            "format": "delta",
        },
    },
}

# --------------------------------------------------------------------------------------------
# IMDb-shaped files of the tests' own: IMDb's header and `\N`, rows invented by the tests
# --------------------------------------------------------------------------------------------

N = r"\N"
HEADERS = {
    "title_basics": (
        "tconst",
        "titleType",
        "primaryTitle",
        "originalTitle",
        "isAdult",
        "startYear",
        "endYear",
        "runtimeMinutes",
        "genres",
    ),
    "title_ratings": ("tconst", "averageRating", "numVotes"),
}
# Whole seconds, so that the instant survives every precision on the way.
MODIFIED = {
    "title_basics": datetime(2026, 11, 17, 6, 12, tzinfo=UTC),
    "title_ratings": datetime(2026, 11, 17, 6, 13, tzinfo=UTC),
}

# The first snapshot, and the rows each table holds after loading it, sorted by tconst.
BASICS_LINES = (
    ("tt9900001", "movie", '"Quoted" Title', "Title", "0", "2001", N, "95", "Comedy,Drama"),
    ("tt9900002", "tvSeries", "Second", "Second", "1", "2010", "2012", N, "Documentary"),
    ("tt9900003", "short", 'Third "Title" Here', "Third", "0", "1999", N, N, N),
)
BASICS_ROWS = [
    ("tt9900001", "movie", '"Quoted" Title', "Title", False, 2001, None, 95, ["Comedy", "Drama"]),
    ("tt9900002", "tvSeries", "Second", "Second", True, 2010, 2012, None, ["Documentary"]),
    ("tt9900003", "short", 'Third "Title" Here', "Third", False, 1999, None, None, None),
]
RATINGS_LINES = (
    ("tt9900001", "7.5", "1234"),
    ("tt9900002", "6.1", "56"),
    ("tt9900003", "8.0", "7"),
)
RATINGS_ROWS = [("tt9900001", 7.5, 1234), ("tt9900002", 6.1, 56), ("tt9900003", 8.0, 7)]
LINES = {"title_basics": BASICS_LINES, "title_ratings": RATINGS_LINES}

# A later snapshot: one title changed, two gone, one new.
SECOND_BASICS_LINES = (
    ("tt9900001", "movie", '"Quoted" Title', "Title", "0", "2001", N, "96", "Comedy,Drama"),
    ("tt9900004", "tvMiniSeries", "Fourth", "Quarto", "0", "2020", "2021", "50", "Crime"),
)
SECOND_BASICS_ROWS = [
    ("tt9900001", "movie", '"Quoted" Title', "Title", False, 2001, None, 96, ["Comedy", "Drama"]),
    ("tt9900004", "tvMiniSeries", "Fourth", "Quarto", False, 2020, 2021, 50, ["Crime"]),
]
SECOND_RATINGS_LINES = (("tt9900001", "7.6", "1301"), ("tt9900004", "8.2", "42"))
SECOND_RATINGS_ROWS = [("tt9900001", 7.6, 1301), ("tt9900004", 8.2, 42)]
SECOND_LINES = {"title_basics": SECOND_BASICS_LINES, "title_ratings": SECOND_RATINGS_LINES}

# The columns and types of each table after a run, in order (section What a table holds).
TABLE_COLUMNS = {
    "title_basics": [
        ("tconst", "string"),
        ("titleType", "string"),
        ("primaryTitle", "string"),
        ("originalTitle", "string"),
        ("isAdult", "boolean"),
        ("startYear", "int"),
        ("endYear", "int"),
        ("runtimeMinutes", "int"),
        ("genres", "array<string>"),
        ("_run_id", "string"),
        ("_source_modified_at", "timestamp"),
    ],
    "title_ratings": [
        ("tconst", "string"),
        ("averageRating", "double"),
        ("numVotes", "int"),
        ("_run_id", "string"),
        ("_source_modified_at", "timestamp"),
    ],
}

# The tree of a successful run (section The log), without the run id; `<d>` is a duration.
TREE = (
    "┌ process started · 4 steps",
    "│ ┌ extract_title_basics (1/4) · 1 callback",
    "│ │ ✔ ReadSource.run <d>",
    "│ └ extract_title_basics done in <d>",
    "│ ┌ load_title_basics (2/4) · 4 callbacks",
    "│ │ ✔ RequireRows.run <d>",
    "│ │ ✔ AddLineage.run <d>",
    "│ │ ✔ ConvertColumns.run <d>",
    "│ │ ✔ FullLoad.run <d>",
    "│ └ load_title_basics done in <d>",
    "│ ┌ extract_title_ratings (3/4) · 1 callback",
    "│ │ ✔ ReadSource.run <d>",
    "│ └ extract_title_ratings done in <d>",
    "│ ┌ load_title_ratings (4/4) · 3 callbacks",
    "│ │ ✔ RequireRows.run <d>",
    "│ │ ✔ AddLineage.run <d>",
    "│ │ ✔ FullLoad.run <d>",
    "│ └ load_title_ratings done in <d>",
    "└ process finished in <d>",
)

# --------------------------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------------------------


def _write_json(path: Path, value: object) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value), encoding="utf-8")
    return path


def _reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    """A JSON object hook: a key written twice would hold one value and hide the other."""
    keys = [key for key, _ in pairs]
    duplicates = sorted({key for key in keys if keys.count(key) > 1})
    assert not duplicates, f"keys written twice: {duplicates}"
    return dict(pairs)


def _local_config(table: str, source: Path, target: Path) -> dict[str, Any]:
    """The shipped members of a table, with a source under tmp_path and a local Delta target."""
    config = copy.deepcopy(SHIPPED_CONFIGS[table])
    config["source"]["path"] = str(source)
    config["target"] = {"system": "disk", "path": str(target), "format": "delta"}
    return config


def _imdb_bytes(header: Sequence[str], lines: Sequence[Sequence[str]]) -> bytes:
    """A file in IMDb's layout: gzip, tab-separated, a header line, one line per row."""
    text = "".join("\t".join(fields) + "\n" for fields in (header, *lines))
    return gzip.compress(text.encode("utf-8"), mtime=0)


def _set_modified(file: Path, instant: datetime) -> None:
    ns = int(instant.timestamp()) * 1_000_000_000
    os.utime(file, ns=(ns, ns))


@dataclass(frozen=True)
class _Job:
    """The job's files for one test: configurations, source directories and local targets."""

    configs: dict[str, Path]
    sources: dict[str, Path]
    targets: dict[str, Path]


@pytest.fixture
def job(tmp_path: Path) -> _Job:
    """The configurations of both tables, in load order, written under tmp_path.

    Each source directory mirrors the container's, `<data>/<IMDb name>`, and is not created:
    each test writes the files it needs.
    """
    sources = {table: tmp_path / "data" / IMDB_NAMES[table] for table in TABLES}
    targets = {table: tmp_path / "tables" / table for table in TABLES}
    configs = {
        table: _write_json(
            tmp_path / "configs" / f"config_{table}.json",
            _local_config(table, sources[table], targets[table]),
        )
        for table in TABLES
    }
    return _Job(configs, sources, targets)


def _write_source(
    job: _Job,
    table: str,
    lines: Sequence[Sequence[str]],
    *,
    header: Sequence[str] | None = None,
) -> Path:
    """Write the table's IMDb file alone in its directory, as the download leaves it."""
    directory = job.sources[table]
    directory.mkdir(parents=True, exist_ok=True)
    file = directory / f"{IMDB_NAMES[table]}.tsv.gz"
    file.write_bytes(_imdb_bytes(HEADERS[table] if header is None else header, lines))
    _set_modified(file, MODIFIED[table])
    return file


def _build(configs: Mapping[str, Path]) -> Process:
    import main

    process: Process = main.build_process(configs)
    return process


def _run(configs: Mapping[str, Path]) -> Process:
    process = _build(configs)
    process.run()
    return process


def _first_run(job: _Job) -> Process:
    """Load the first snapshot of both tables."""
    for table in TABLES:
        _write_source(job, table, LINES[table])
    return _run(job.configs)


def _run_expecting_failure(configs: Mapping[str, Path], *, match: str | None = None) -> None:
    """Run the process and expect Spark's error, whatever its class.

    Spark's errors reach Python as py4j's or pyspark's exceptions, which share no base class
    narrower than `Exception`. The process is built first, so that an error of the build is
    not taken for the error of the run.
    """
    process = _build(configs)
    with pytest.raises(Exception, match=match):
        process.run()


def _read_table(spark: SparkSession, path: Path) -> DataFrame:
    frame: DataFrame = spark.read.format("delta").load(str(path))
    return frame


def _data_rows(spark: SparkSession, path: Path, table: str) -> list[tuple[Any, ...]]:
    """The rows of the table without its lineage columns, as tuples sorted by tconst."""
    rows: list[Row] = _read_table(spark, path).select(*HEADERS[table]).collect()
    return sorted((tuple(row) for row in rows), key=lambda row: str(row[0]))


def _history(spark: SparkSession, path: Path) -> list[Row]:
    history: list[Row] = spark.sql(f"DESCRIBE HISTORY delta.`{path}`").collect()
    return history


def _snapshot(spark: SparkSession, path: Path) -> tuple[list[tuple[Any, ...]], int]:
    """What "unchanged" compares: every row with every column, and the number of versions."""
    rows: list[Row] = _read_table(spark, path).collect()
    ordered = sorted((tuple(row) for row in rows), key=lambda row: str(row[0]))
    return ordered, len(_history(spark, path))


def _is_delta_table(path: Path) -> bool:
    return (path / "_delta_log").is_dir()


def _ingestio_messages(caplog: pytest.LogCaptureFixture) -> list[str]:
    return [
        record.getMessage()
        for record in caplog.records
        if record.name == "ingestio" or record.name.startswith("ingestio.")
    ]


# --------------------------------------------------------------------------------------------
# The shipped configuration (no Spark)
# --------------------------------------------------------------------------------------------


def test_at001_tables_maps_each_table_to_its_configuration_file_in_load_order(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import main

    # The paths resolve wherever the job runs from, not only from the repository.
    monkeypatch.chdir(tmp_path)
    assert list(main.TABLES) == ["title_basics", "title_ratings"]
    for table in TABLES:
        path = Path(main.TABLES[table])
        assert path.is_file(), path
        assert path.samefile(SRC / f"config_{table}.json")


@pytest.mark.parametrize("table", TABLES)
def test_at002_shipped_configuration_holds_exactly_what_the_spec_gives(table: str) -> None:
    path = SRC / f"config_{table}.json"
    loaded = json.loads(path.read_text(encoding="utf-8"), object_pairs_hook=_reject_duplicate_keys)
    expected = SHIPPED_CONFIGS[table]
    assert loaded == expected
    # JSON's types too: `true` is not `1`, nor `false` `0`.
    assert json.dumps(loaded, sort_keys=True) == json.dumps(expected, sort_keys=True)


@pytest.mark.parametrize("table", TABLES)
def test_at003_shipped_configuration_builds_read_source_and_full_load(table: str) -> None:
    config = SRC / f"config_{table}.json"
    assert isinstance(ReadSource(config, into=table), ReadSource)
    assert isinstance(FullLoad(config, key=table), FullLoad)


def test_at003_build_process_builds_from_the_shipped_tables() -> None:
    import main

    process: Process = main.build_process(main.TABLES)
    assert isinstance(process, Process)


def test_at004_build_process_accepts_the_convert_member_of_the_spec(tmp_path: Path) -> None:
    # The control of the cases below: the same configuration, with the member the spec gives.
    config = _write_json(
        tmp_path / "config_title_basics.json",
        _local_config("title_basics", tmp_path / "title.basics", tmp_path / "title_basics"),
    )
    assert isinstance(_build({"title_basics": config}), Process)


# Every case of AT-004, in its order. Where a case allows it, the member also names a valid
# column, so that the rule "no column in total" cannot reject it in place of the rule it breaks.
BAD_CONVERT_MEMBERS: dict[str, object] = {
    # A convert that is not an object.
    "a list instead of an object": ["isAdult"],
    "a string instead of an object": "isAdult",
    # A key other than booleans and lists.
    "another key": {"bools": ["isAdult"]},
    "another key beside the two": {"booleans": ["isAdult"], "lists": ["genres"], "dates": []},
    # A value that is not a list of column names.
    "a string instead of a list": {"booleans": "isAdult"},
    "an object instead of a list": {"lists": {"genres": True}},
    "a null instead of a list": {"booleans": ["isAdult"], "lists": None},
    "a number among the names": {"lists": ["genres", 1]},
    # An empty column name.
    "an empty name": {"booleans": [""]},
    "an empty name beside a valid one": {"booleans": ["isAdult"], "lists": ["genres", ""]},
    # No column in total.
    "no key": {},
    "no column in either list": {"booleans": [], "lists": []},
}


@pytest.mark.parametrize(
    "convert", list(BAD_CONVERT_MEMBERS.values()), ids=list(BAD_CONVERT_MEMBERS)
)
def test_at004_build_process_rejects_a_convert_member_of_another_shape(
    tmp_path: Path, convert: object
) -> None:
    content = _local_config("title_basics", tmp_path / "title.basics", tmp_path / "title_basics")
    content["convert"] = convert
    config = _write_json(tmp_path / "config_title_basics.json", content)
    with pytest.raises(ValueError, match="^" + re.escape(str(config))):
        _build({"title_basics": config})


# --------------------------------------------------------------------------------------------
# The download script (a subprocess against a local HTTP server, no Spark)
# --------------------------------------------------------------------------------------------

# A proxy of the environment would receive the requests meant for the local server.
_PROXY_VARIABLES = frozenset({"http_proxy", "https_proxy", "all_proxy", "no_proxy"})


@dataclass(frozen=True)
class _Server:
    """A local HTTP server of the directory `root`, whose base URL is `url`."""

    root: Path
    url: str

    def serve(self, name: str, content: bytes, modified: datetime) -> None:
        """Serve `content` as `name`, with `modified` as its `Last-Modified`."""
        file = self.root / name
        file.write_bytes(content)
        _set_modified(file, modified)


@pytest.fixture
def imdb_server(tmp_path: Path) -> Iterator[_Server]:
    """`http.server` in a thread on 127.0.0.1: `Last-Modified` from each file, `404` otherwise."""
    root = tmp_path / "served"
    root.mkdir()
    server = ThreadingHTTPServer(
        ("127.0.0.1", 0), partial(SimpleHTTPRequestHandler, directory=str(root))
    )
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield _Server(root, f"http://127.0.0.1:{server.server_port}/")
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


def _download(directory: Path, base_url: str) -> subprocess.CompletedProcess[str]:
    """Run the script as the maintainer does, with the interpreter of the test run."""
    env = {key: value for key, value in os.environ.items() if key.lower() not in _PROXY_VARIABLES}
    return subprocess.run(
        [sys.executable, str(SCRIPT), str(directory), "--base-url", base_url],
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
        env=env,
        cwd=directory.parent,
    )


def _file_names(directory: Path) -> list[str]:
    return sorted(entry.name for entry in directory.iterdir())


def test_at005_download_writes_each_file_alone_in_its_directory_with_the_served_time(
    imdb_server: _Server, tmp_path: Path
) -> None:
    contents = {table: _imdb_bytes(HEADERS[table], LINES[table]) for table in TABLES}
    for table in TABLES:
        imdb_server.serve(f"{IMDB_NAMES[table]}.tsv.gz", contents[table], MODIFIED[table])
    directory = tmp_path / "data"
    assert not directory.exists()

    result = _download(directory, imdb_server.url)

    assert result.returncode == 0, result.stderr
    stderr_lines = result.stderr.splitlines()
    for table in TABLES:
        name = f"{IMDB_NAMES[table]}.tsv.gz"
        file = directory / IMDB_NAMES[table] / name
        assert file.read_bytes() == contents[table]
        assert file.stat().st_mtime_ns == int(MODIFIED[table].timestamp()) * 1_000_000_000
        assert _file_names(file.parent) == [name]
        url = f"{imdb_server.url}{name}"
        assert any(url in line for line in stderr_lines), result.stderr


def test_at006_a_second_download_replaces_the_files(imdb_server: _Server, tmp_path: Path) -> None:
    directory = tmp_path / "data"
    for table in TABLES:
        content = _imdb_bytes(HEADERS[table], LINES[table])
        imdb_server.serve(f"{IMDB_NAMES[table]}.tsv.gz", content, MODIFIED[table])
    first = _download(directory, imdb_server.url)
    assert first.returncode == 0, first.stderr

    later = {table: _imdb_bytes(HEADERS[table], SECOND_LINES[table]) for table in TABLES}
    for table in TABLES:
        imdb_server.serve(
            f"{IMDB_NAMES[table]}.tsv.gz", later[table], datetime(2026, 11, 18, 6, 0, tzinfo=UTC)
        )
    second = _download(directory, imdb_server.url)

    assert second.returncode == 0, second.stderr
    for table in TABLES:
        name = f"{IMDB_NAMES[table]}.tsv.gz"
        assert (directory / IMDB_NAMES[table] / name).read_bytes() == later[table]
        assert _file_names(directory / IMDB_NAMES[table]) == [name]


def test_at007_a_failed_download_keeps_the_existing_file(
    imdb_server: _Server, tmp_path: Path
) -> None:
    # title.basics.tsv.gz is not served, so the server answers 404 for it.
    imdb_server.serve(
        "title.ratings.tsv.gz",
        _imdb_bytes(HEADERS["title_ratings"], RATINGS_LINES),
        MODIFIED["title_ratings"],
    )
    directory = tmp_path / "data"
    existing = directory / "title.basics" / "title.basics.tsv.gz"
    existing.parent.mkdir(parents=True)
    content = _imdb_bytes(HEADERS["title_basics"], BASICS_LINES)
    existing.write_bytes(content)
    _set_modified(existing, MODIFIED["title_basics"])
    modified_ns = existing.stat().st_mtime_ns

    result = _download(directory, imdb_server.url)

    assert result.returncode == 1, result.stderr
    assert f"{imdb_server.url}title.basics.tsv.gz" in result.stderr
    assert existing.read_bytes() == content
    assert existing.stat().st_mtime_ns == modified_ns
    assert _file_names(existing.parent) == ["title.basics.tsv.gz"]


# --------------------------------------------------------------------------------------------
# The Spark session
# --------------------------------------------------------------------------------------------


@pytest.mark.integration
def test_at008_build_session_holds_the_settings_of_the_spec(spark: SparkSession) -> None:
    conf = dict(spark.sparkContext.getConf().getAll())
    # Every setting of the table in section The Spark session, except spark.jars.
    expected = {
        "spark.master": "local[2]",
        "spark.app.name": "ingestion_disk_to_gcs_delta",
        "spark.driver.memory": "2g",
        "spark.ui.enabled": "false",
        "spark.sql.session.timeZone": "UTC",
        "spark.jars.packages": f"io.delta:delta-spark_2.13:{version('delta-spark')}",
        "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        "spark.hadoop.fs.gs.impl": "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
        "spark.hadoop.fs.AbstractFileSystem.gs.impl": (
            "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS"
        ),
        "spark.hadoop.fs.gs.auth.type": "APPLICATION_DEFAULT",
    }
    assert {key: conf.get(key) for key in expected} == expected
    # The job sets spark.jars only when it is given gcs_connector_jar, and the fixture gives
    # none. spark-submit may still add to spark.jars the JARs it resolves for
    # spark.jars.packages, Delta Lake's among them: whatever it adds, spark.jars holds no JAR of
    # the Cloud Storage connector.
    assert "gcs-connector" not in conf.get("spark.jars", ""), conf.get("spark.jars")
    words = ("auth", "credential", "keyfile", "service.account")
    sensitive = sorted(key for key in conf if any(word in key.lower() for word in words))
    assert sensitive == ["spark.hadoop.fs.gs.auth.type"]
    assert version("pyspark") == "4.2.0"
    assert version("delta-spark") == "4.4.1"


def test_at009_build_session_refuses_a_connector_jar_that_does_not_exist(tmp_path: Path) -> None:
    import spark_session

    assert Path("/opt/jars/gcs-connector-shaded.jar") == spark_session.GCS_CONNECTOR_JAR
    missing = tmp_path / "jars" / "gcs-connector-shaded.jar"
    message = f"gcs connector jar not found: {missing}"
    with pytest.raises(ValueError, match=f"^{re.escape(message)}$"):
        spark_session.build_session(gcs_connector_jar=missing)


# --------------------------------------------------------------------------------------------
# The process (Spark and Delta Lake, local targets, IMDb-shaped files of the tests' own)
# --------------------------------------------------------------------------------------------


@pytest.mark.integration
def test_at010_a_run_writes_a_delta_table_per_file_with_the_columns_of_the_spec(
    spark: SparkSession, job: _Job
) -> None:
    _first_run(job)

    for table in TABLES:
        target = job.targets[table]
        assert _is_delta_table(target), target
        frame = _read_table(spark, target)
        columns = [(field.name, field.dataType.simpleString()) for field in frame.schema.fields]
        assert columns == TABLE_COLUMNS[table]
        tconsts: list[str] = [row["tconst"] for row in frame.select("tconst").collect()]
        assert sorted(tconsts) == sorted(line[0] for line in LINES[table])


@pytest.mark.integration
def test_at011_title_basics_holds_nulls_titles_booleans_and_genres_as_the_spec_types_them(
    spark: SparkSession, job: _Job
) -> None:
    # BASICS_LINES hold `\N` in endYear, runtimeMinutes and genres, a primaryTitle that starts
    # with `"` and holds another one later, isAdult 0 and 1, and the genres Comedy,Drama.
    _first_run(job)

    assert _data_rows(spark, job.targets["title_basics"], "title_basics") == BASICS_ROWS


@pytest.mark.integration
def test_at012_every_row_holds_the_run_id_and_the_modification_time_of_its_file(
    spark: SparkSession, job: _Job
) -> None:
    process = _first_run(job)

    for table in TABLES:
        rows: list[Row] = (
            _read_table(spark, job.targets[table])
            .selectExpr("_run_id", "unix_micros(_source_modified_at) AS modified_micros")
            .collect()
        )
        expected = (process.run_id, int(MODIFIED[table].timestamp()) * 1_000_000)
        assert len(rows) == len(LINES[table])
        assert {(row["_run_id"], row["modified_micros"]) for row in rows} == {expected}


@pytest.mark.integration
def test_at013_a_second_run_replaces_each_table_with_the_rows_of_the_new_files(
    spark: SparkSession, job: _Job
) -> None:
    _first_run(job)
    for table in TABLES:
        _write_source(job, table, SECOND_LINES[table])

    _run(job.configs)

    assert _data_rows(spark, job.targets["title_basics"], "title_basics") == SECOND_BASICS_ROWS
    assert _data_rows(spark, job.targets["title_ratings"], "title_ratings") == SECOND_RATINGS_ROWS
    for table in TABLES:
        history = sorted(_history(spark, job.targets[table]), key=lambda row: int(row["version"]))
        versions = [
            (row["version"], row["operation"], row["operationParameters"].get("mode"))
            for row in history
        ]
        assert versions == [(0, "WRITE", "Overwrite"), (1, "WRITE", "Overwrite")]


@pytest.mark.integration
def test_at014_running_the_same_files_twice_keeps_each_line_once_with_the_second_run_id(
    spark: SparkSession, job: _Job
) -> None:
    _first_run(job)

    second = _run(job.configs)

    assert _data_rows(spark, job.targets["title_basics"], "title_basics") == BASICS_ROWS
    assert _data_rows(spark, job.targets["title_ratings"], "title_ratings") == RATINGS_ROWS
    for table in TABLES:
        rows: list[Row] = _read_table(spark, job.targets[table]).select("_run_id").collect()
        assert {row["_run_id"] for row in rows} == {second.run_id}
        assert len(_history(spark, job.targets[table])) == 2


@pytest.mark.integration
def test_at015_a_directory_with_no_file_fails_its_table_with_empty_batch_error(
    spark: SparkSession, job: _Job, caplog: pytest.LogCaptureFixture
) -> None:
    import main

    caplog.set_level(logging.INFO, logger="ingestio")
    _write_source(job, "title_basics", BASICS_LINES)
    job.sources["title_ratings"].mkdir(parents=True)
    process = _build(job.configs)

    with pytest.raises(main.EmptyBatchError) as caught:
        process.run()

    assert str(caught.value) == "title_ratings: no row to load"
    assert issubclass(main.EmptyBatchError, RuntimeError)
    failure = re.compile(
        r"✖ RequireRows\.run failed after .+: EmptyBatchError: title_ratings: no row to load"
    )
    assert any(failure.search(message) for message in caplog.messages), caplog.messages
    assert _data_rows(spark, job.targets["title_basics"], "title_basics") == BASICS_ROWS
    assert not _is_delta_table(job.targets["title_ratings"])


@pytest.mark.integration
def test_at016_a_header_only_file_fails_the_run_and_leaves_the_table_unchanged(
    spark: SparkSession, job: _Job
) -> None:
    import main

    _first_run(job)
    before = _snapshot(spark, job.targets["title_basics"])
    _write_source(job, "title_basics", ())
    process = _build(job.configs)

    with pytest.raises(main.EmptyBatchError):
        process.run()

    assert _snapshot(spark, job.targets["title_basics"]) == before


@pytest.mark.integration
@pytest.mark.usefixtures("spark")
def test_at017_a_missing_source_directory_fails_the_run_before_any_table_is_written(
    job: _Job, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO, logger="ingestio")
    _write_source(job, "title_ratings", RATINGS_LINES)
    assert not job.sources["title_basics"].exists()
    process = _build(job.configs)

    with pytest.raises(FileNotFoundError):
        process.run()

    assert any("✖ ReadSource.run failed" in message for message in caplog.messages), caplog.messages
    assert not _is_delta_table(job.targets["title_basics"])
    assert not _is_delta_table(job.targets["title_ratings"])


@pytest.mark.integration
def test_at018_a_value_that_does_not_parse_fails_the_run_and_leaves_the_table_unchanged(
    spark: SparkSession, job: _Job
) -> None:
    _first_run(job)
    before = _snapshot(spark, job.targets["title_basics"])
    # The last row, so that the read fails at the write rather than at the first row.
    bad_row = ("tt9900003", "short", 'Third "Title" Here', "Third", "0", "19x5", N, N, N)
    _write_source(job, "title_basics", (*BASICS_LINES[:2], bad_row))

    _run_expecting_failure(job.configs)

    assert _snapshot(spark, job.targets["title_basics"]) == before


@pytest.mark.integration
def test_at019_a_row_with_a_field_missing_fails_the_run_and_leaves_the_table_unchanged(
    spark: SparkSession, job: _Job
) -> None:
    _first_run(job)
    before = _snapshot(spark, job.targets["title_ratings"])
    _write_source(job, "title_ratings", (*RATINGS_LINES[:2], ("tt9900003", "8.0")))

    _run_expecting_failure(job.configs)

    assert _snapshot(spark, job.targets["title_ratings"]) == before


@pytest.mark.integration
def test_at020_a_header_in_another_order_fails_the_run_and_leaves_the_table_unchanged(
    spark: SparkSession, job: _Job
) -> None:
    _first_run(job)
    before = _snapshot(spark, job.targets["title_basics"])
    # primaryTitle and originalTitle swap places, in the header and in every row. Both are
    # strings, so every row still parses by position: only the header differs from the schema.
    order = (0, 1, 3, 2, 4, 5, 6, 7, 8)
    header = [HEADERS["title_basics"][index] for index in order]
    lines = [[line[index] for index in order] for line in BASICS_LINES]
    _write_source(job, "title_basics", lines, header=header)

    _run_expecting_failure(job.configs)

    assert _snapshot(spark, job.targets["title_basics"]) == before


@pytest.mark.integration
def test_at021_an_is_adult_other_than_0_or_1_fails_the_run_and_leaves_the_table_unchanged(
    spark: SparkSession, job: _Job
) -> None:
    _first_run(job)
    before = _snapshot(spark, job.targets["title_basics"])
    bad_row = ("tt9900003", "short", 'Third "Title" Here', "Third", "2", "1999", N, N, N)
    _write_source(job, "title_basics", (*BASICS_LINES[:2], bad_row))

    _run_expecting_failure(job.configs, match="isAdult")

    assert _snapshot(spark, job.targets["title_basics"]) == before


@pytest.mark.integration
@pytest.mark.usefixtures("spark")
def test_at022_a_successful_run_logs_the_tree_of_the_spec_in_order(
    job: _Job, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO, logger="ingestio")

    process = _first_run(job)

    messages = _ingestio_messages(caplog)
    prefix = re.escape(f"[{process.run_id}] ")
    remaining = iter(messages)
    for line in TREE:
        pattern = prefix + re.escape(line).replace("<d>", r"\S.*")
        # `any` consumes the records up to the line it finds, so the lines keep their order.
        if not any(re.fullmatch(pattern, message) for message in remaining):
            pytest.fail(
                f"no record holds {line!r} after the lines before it:\n" + "\n".join(messages)
            )
