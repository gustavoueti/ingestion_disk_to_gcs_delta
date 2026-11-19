# Spec: Ingest the IMDb datasets into Delta Lake tables on GCS

- **Status:** validated
- **Slug:** `ingest_imdb_datasets`. The spec is `specs/ingest_imdb_datasets.md` and its tests are `tests/test_ingest_imdb_datasets.py`.
- **Version:** `0.2.0`
- **Related:** no other spec; no entry of `LIB_GAPS.md`.

<!--
Status values, in order: draft → approved → tested → documented → implemented → validated.
Only the maintainer's explicit approval moves a spec from draft to approved. The main session
updates the status and the History; the stages report to it. See docs/workflow.md.
-->

> **How to read this spec.** This first spec settles what `AGENTS.md` and
> `.claude/kb/ingestio.md` leave to the job. Every choice that belonged to the maintainer is
> recorded under **Decisions**, as M1 to M19, dated 2026-11-18. No question is open.

## Summary

A script of the job, run on the host, downloads two files of the IMDb non-commercial datasets,
`title.basics.tsv.gz` and `title.ratings.tsv.gz`, each into a directory of its own. A
container started by `docker compose` mounts that directory at `/data` and runs the job. The job
reads each file with `ReadSource` (`DiskReader`, Spark's `csv` source) and replaces a Delta Lake
table under the prefix `imdb` of the bucket `ingestio-imdb-delta` with it, through `FullLoad`
(`GcsWriter`, `delta`). Three callbacks of the job's own sit between the read and the write:
one fails the run rather than let a table be emptied, one adds lineage columns, and one types
IMDb's `0`/`1` and list columns. Spark authenticates to Cloud Storage only with the Application
Default Credentials file that compose mounts.

## Motivation

- **The job's first process.** It is the goal of `AGENTS.md`: files on disk, inside a Spark
  container, to Delta Lake tables in a real GCS bucket. It uses only the public API of
  ingestio: the `disk` and `gcs` systems named in configuration files, the shipped `ReadSource`
  and `FullLoad`, and callbacks of the job's own.
- **The first real use of `GcsWriter` with Delta Lake.** The library's CI runs neither its GCS
  nor its Delta integration tests, and nothing in the library shows `GcsWriter` writing Delta to
  a real bucket (`.claude/kb/ingestio.md`, pitfall 13). The first run of this job against
  the bucket is that evidence. A failure that the library's documentation does not explain is
  reported with the versions involved.
- **What the library looks like in practice.** The process uses one configuration file per
  table, because a file holds one source and one target. It runs one extract step and one load
  step per table. Its own callbacks guard, enrich and convert the batch in `process.state`
  between the shipped read and write. It shows what the job must build around the library: the
  Spark session with Delta Lake and the Cloud Storage connector, the download and the
  container.

## Source and target

- **Source:** the files `title.basics.tsv.gz` and `title.ratings.tsv.gz` of the IMDb
  non-commercial datasets, downloaded from `https://datasets.imdbws.com/`. Each is a
  gzip-compressed, tab-separated file in UTF-8, with a header line and `\N` for a missing value,
  refreshed daily under the same name (*IMDb*). The source is a public download. It is real
  data of a third party, which `AGENTS.md`, section **Goal**, admits for this job within limits
  this spec keeps:
  - the files are licensed for personal and non-commercial use only
    (https://data.imdb.com/non-commercial-datasets/; the older address,
    `developer.imdb.com/non-commercial-datasets/`, redirects there);
  - the job's script downloads them on the host where the job runs, into a directory that git
    ignores. They are never committed, never copied into the image, and mounted read-only into
    the container;
  - the tests write files with the same layout and rows of their own, never real IMDb data.
- **Target:** two Delta Lake tables in the GCS bucket `ingestio-imdb-delta`, one per file:
  `gs://ingestio-imdb-delta/imdb/title_basics` and `gs://ingestio-imdb-delta/imdb/title_ratings`
  (M5, M6). The bucket is the one exception to
  "public or local" and "never credentials" that `AGENTS.md`, section **Goal**, grants. Spark
  reaches it only through Application Default Credentials that the container's environment
  provides. The bucket and the prefix are in the `target` section of each configuration file.
  No credential is committed, copied into the image or written into the job's code or
  configuration. The bucket must not be publicly readable, because the
  license allows personal use only.
- **Data:** one row of `title_basics` after a run. The values are invented, not IMDb's.

  | Column | Value |
  |---|---|
  | `tconst` | `tt9900001` |
  | `titleType` | `movie` |
  | `primaryTitle` | `"Quoted" Test Title` |
  | `originalTitle` | `Test Title` |
  | `isAdult` | `false` (the file holds `0`) |
  | `startYear` | `2001` |
  | `endYear` | null (the file holds `\N`) |
  | `runtimeMinutes` | `95` |
  | `genres` | `["Comedy", "Drama"]` (the file holds `Comedy,Drama`) |
  | `_run_id` | `a3f7c9e1`, the run id of the ingestio process |
  | `_source_modified_at` | `2026-11-17 06:12:00` (UTC), the last-modified time of the file |

  One row of `title_ratings` has `tconst` `tt9900001`, `averageRating` `7.5`, `numVotes` `1234`,
  `_run_id` and `_source_modified_at`.

## Fit with ingestio

- **Shipped callbacks:**
  - `ReadSource(config, into=<table>)`, one per table, in the step `extract_<table>`. It reads
    the `source` section and builds `DiskReader`, which loads the one file of the table's
    directory through Spark's `csv` source. It is given no `watermark_value`, because the
    strategy has no watermark (M7), and no `spark`, because the reader uses the session that is
    active when `read()` runs.
  - `FullLoad(config, key=<table>)`, one per table, last in the step `load_<table>`. It reads the
    `target` section and builds `GcsWriter`. When it runs it calls
    `write(state[<table>], replace=True)`: one Delta commit per table per run, which replaces the
    table with exactly the batch. It fits because each IMDb file is a full snapshot (M7).
  - Not used: `AppendOnly`, because no table keeps snapshots (M7), and `SCD2`, which rejects a
    `gcs` target.
- **Own callbacks** in `src/main.py`, each a subclass of `ingestio.Callback`. Every one is
  essential, as every callback is at 0.7.0, so its failure fails the run.
  - `RequireRows(key)` runs first in each load step. When `state[key]` has no row
    (`DataFrame.isEmpty()`), it raises `EmptyBatchError(f"{key}: no row to load")`. This stops a
    missing or header-only file from emptying the table through `FullLoad` (pitfall 1). An
    empty source is an error, not a normal run (M12). It changes nothing in the state.
  - `AddLineage(key)` runs second. It replaces `state[key]` with the same DataFrame plus two
    columns at its end: `_run_id`, a `STRING` holding `self.process.run_id`, and
    `_source_modified_at`, a `TIMESTAMP` holding `_metadata.file_modification_time` of the row's
    file. It runs after `RequireRows`, because an empty selection has no `_metadata` column
    (pitfall 2). It runs on the reader's own DataFrame, before any conversion (M14).
  - `ConvertColumns(key, *, booleans=(), lists=())` runs third, and only in the load steps of
    tables whose configuration has a `convert` member, which is `title_basics` alone. It
    replaces `state[key]` with a DataFrame in which each column of `booleans` changes from `INT`
    to `BOOLEAN`: `0` becomes `false`, `1` becomes `true` and null stays null. Any other value
    makes the action that evaluates it fail, with a message that holds the column's name and the
    value, such as `isAdult: unexpected value 2`. Each column of `lists` changes from a
    comma-separated `STRING` to `ARRAY<STRING>`, and null stays null. Columns keep their
    position, and the other columns are untouched. This is the conversion that Spark's CSV
    parser cannot make directly (pitfall 5, M13).
  - `EmptyBatchError`, a subclass of `RuntimeError` defined in `src/main.py`, is the error
    `RequireRows` raises.

  None of them reads or writes a file, the bucket or a table. Each transforms or checks the
  DataFrame that `ReadSource` put in the state, and `FullLoad` writes it.
- **Connectors:** the configuration files name `"system": "disk"` with the `csv` format as the
  source and `"system": "gcs"` with the `delta` format as the target. No code of the job builds
  a connector itself. The tests give the same process a target that stays local,
  `{"system": "disk", "path": "<tmp_path>/<table>", "format": "delta"}`, which the library moves
  to by an edit of the JSON alone (ADR 0015).
- **Outside the process,** as section 3 of the knowledge base puts it: the Spark session, built
  by `src/spark_session.py` in `main.py` before the process runs; the download, a script of the
  job run on the host before the container runs the job (pitfall 6, rule 5); and the
  container.
- **Rules of a valid job:**
  1. **Only the public API.** The job imports `Callback`, `FullLoad`, `Process`, `ReadSource`,
     `Step` and `log_to_console` from `ingestio`, and nothing else of the library. It needs no
     connector of `ingestio.connector.all`. Its code uses no internal module and no name that
     starts with `_`. pyspark and Delta Lake are not ingestio.
  2. **Extension only through the documented way.** The three callbacks are subclasses of
     `Callback` in `src/main.py`, with nothing registered. The job adds no connector: `disk` and
     `gcs` are shipped systems.
  3. **Criticality.** No stage of the process should let the run go on after it fails, so every
     callback is essential, which is all 0.7.0 offers. No callback catches its own failure.
     `RequireRows` raises an error that fails the run (M12).
  4. **No connector decides the write strategy.** `FullLoad` chooses replace. The `target`
     sections hold no `mode`, `append` or `replace`, and no `options` at all.
  5. **No workaround.** No monkeypatching and no subclass of a shipped callback or connector.
     The configuration files are static files in `src/`, never generated at run time. The job
     never reads the files with `spark.read` or writes a table with `df.write`. It never reads,
     merges or vacuums a Delta table. It puts no file-system or authentication setting in a
     section's `options`. The session, the download before the process and the tests' own
     configurations under `tmp_path` are the legitimate pieces that rule 5 names.
  6. **The shipped callbacks wherever they fit.** `ReadSource` reads, and `FullLoad` writes.
     The job never reimplements either. `SCD2` does not fit a `gcs` target.

  The process meets every rule without a workaround.

## Scope

In scope:

- The tables `title_basics` and `title_ratings`, loaded from `title.basics.tsv.gz` and
  `title.ratings.tsv.gz` (M1).
- The download script, run on the host (M2).
- The configuration file of each table, the job's own callbacks, the process, and the Spark
  session with Delta Lake and the Cloud Storage connector set to Application Default
  Credentials.
- The image, its build context and the compose file that starts it, with the data directory and
  the credential file mounted read-only (M3, M4).
- Tests with local targets and IMDb-shaped files of their own. The job's version is `0.2.0`.

The files the change creates or changes, with the stage that writes each:

| File | Created or changed | Stage | Note |
|---|---|---|---|
| `specs/ingest_imdb_datasets.md` | created | planner | this spec |
| `tests/test_ingest_imdb_datasets.py`, `tests/conftest.py` | created | tester | |
| `docs/` (a page for the process), `docs/index.md`, `README.md`, `CHANGELOG.md` | created or changed | documenter | |
| `src/main.py` | created | implementer | the entry point and the job's callbacks |
| `src/spark_session.py` | created | implementer | the session and the path of the connector JAR |
| `src/config_title_basics.json`, `src/config_title_ratings.json` | created | implementer | one configuration file per table |
| `scripts/download_imdb.py` | created, at the root | implementer | M2, authorized by M18 |
| `Dockerfile`, `.dockerignore`, `compose.yaml` | created, at the root | implementer | M3, authorized by M18 |
| `pyproject.toml` | changed | implementer | `project.version` to `0.2.0`; `pyspark==4.2.0` and `delta-spark==4.4.1` added to the `dev` group (M3, M10, authorized by M18) |
| `uv.lock` | changed | implementer | |
| `.gitignore` | changed | implementer | adds `data/`, the default host directory of the files (M17), and keeps the maintainer's own uncommitted line for `.dotenv` |
| `.github/workflows/ci.yml` | unchanged, as planned | implementer | Java 17 is already there; the Spark tests reach Maven Central for Delta Lake |
| `CONTRIBUTING.md` | changed, by the main session, at the maintainer's request | main session | section **Layout** now names `src/config_<table>.json`, the files at the root and the command that runs the job (M19). This edit of a file the hub generates is authorized for this job |

The implementer's row in `docs/workflow.md` does not list `scripts/`, the `Dockerfile`,
`.dockerignore`, `compose.yaml` or the two `dev` packages. The maintainer authorized them for
this job (M18), and `CONTRIBUTING.md`, section **Layout**, now names the files at the root
(M19). The validator's check 9 reads all of them as authorized, the main session's edit of
`CONTRIBUTING.md` included.

Out of scope:

- The other five IMDb files.
- Incremental loads, upserts, merges, SCD2 and append-only history (M7).
- Partitioning, `VACUUM`, `OPTIMIZE` and any other maintenance of the tables (M16).
- Registering the tables in a catalog, a metastore or BigQuery.
- Reading the tables back as part of the job, and checking the bucket's contents from the job.
- Scheduling, and two runs at once against the same tables.
- Any test that reaches GCS, needs credentials, builds the image or starts compose.
- Running the job outside the container, and publishing the image.

## Configuration

### The configuration files

A configuration file holds one source and one target (`.claude/kb/ingestio.md`, section 3),
so the job has one file per table, in `src/`: `src/config_title_basics.json` and
`src/config_title_ratings.json`. `CONTRIBUTING.md`, section **Layout**, names them that way
(M19). `main.TABLES` lists the files in load order.

`src/config_title_basics.json`:

```json
{
  "source": {
    "system": "disk",
    "path": "/data/title.basics",
    "format": "csv",
    "schema": "tconst STRING, titleType STRING, primaryTitle STRING, originalTitle STRING, isAdult INT, startYear INT, endYear INT, runtimeMinutes INT, genres STRING",
    "options": {
      "sep": "\t",
      "header": true,
      "nullValue": "\\N",
      "quote": "",
      "mode": "FAILFAST",
      "enforceSchema": false
    }
  },
  "target": {
    "system": "gcs",
    "path": "gs://ingestio-imdb-delta/imdb/title_basics",
    "format": "delta"
  },
  "convert": {"booleans": ["isAdult"], "lists": ["genres"]}
}
```

`src/config_title_ratings.json`:

```json
{
  "source": {
    "system": "disk",
    "path": "/data/title.ratings",
    "format": "csv",
    "schema": "tconst STRING, averageRating DOUBLE, numVotes INT",
    "options": {
      "sep": "\t",
      "header": true,
      "nullValue": "\\N",
      "quote": "",
      "mode": "FAILFAST",
      "enforceSchema": false
    }
  },
  "target": {
    "system": "gcs",
    "path": "gs://ingestio-imdb-delta/imdb/title_ratings",
    "format": "delta"
  }
}
```

What each key does:

| Key | Value | Why |
|---|---|---|
| `source.path` | `/data/<IMDb name>`, an absolute directory in the container | compose mounts the host directory at `/data` (M4), and each file is alone in its own directory (pitfall 3) |
| `source.format` | `csv` | Spark's CSV source reads the TSV files and decompresses `.gz` by extension |
| `source.schema` | the file's columns, in the file's order, as a DDL string | the types of the table (M13). The names and order match IMDb's page. Without it every column would be a string, and an empty selection would have no column (pitfall 1) |
| `options.sep` | a tab | |
| `options.header` | `true` | the first line holds the column names |
| `options.nullValue` | `\N` | IMDb's marker of a missing value |
| `options.quote` | the empty string, which turns quoting off for reading (*Spark*) | IMDb quotes no value. With Spark's default `"`, a title that starts with `"` swallows the fields after it (pitfall 5) |
| `options.mode` | `FAILFAST` | a row that does not parse fails the read instead of turning into nulls or being dropped silently (M15) |
| `options.enforceSchema` | `false` | Spark checks the header's names against the schema, by position, and fails on a difference, instead of applying the schema to whatever columns the file holds (M15) |
| `target.path` | `gs://ingestio-imdb-delta/imdb/<table>` | the bucket (M5), the prefix `imdb` and the table directory (M6) |
| `target.format` | `delta` | |
| no `target.options` | | no `overwriteSchema` or `mergeSchema`: a change to a table's schema is a change of the job, decided by its own spec (pitfall 14, Decision D7) |
| `convert` | a member of the job's own, for `title_basics` only | the columns `ConvertColumns` converts (M13). Every library callback ignores a top-level member that is not `source`, `target` or `scd2` |

`watermark_pattern` is not set. Each file's watermark would be its last-modified time, but the
strategy uses none (M7).

**The job's own member `convert`.** It is an object with at most two keys, `booleans` and
`lists`. Each value is a list of column names as non-empty strings, and the two lists together
name at least one column. `build_process` reads the member, and a member with another key, or a
value of another shape, raises `ValueError` whose message starts with the file's path, such as
`<path>: convert.bools is not a key`. A table with no `convert` member gets no
`ConvertColumns`.

**What never goes in a configuration file:**

- Credentials, a key file's path or any authentication setting, in a section or in its
  `options` (pitfall 11).
- The Spark session and its settings, Delta Lake's and the Cloud Storage connector's among them.
  They are in `src/spark_session.py`.
- The write strategy. `FullLoad` is placed in `src/main.py`.
- A watermark, because the strategy has none.
- The list and the order of the tables, which are in `main.TABLES`.

### What `src/main.py` holds

- `TABLES`, a mapping in load order: `"title_basics"` maps to `src/config_title_basics.json`,
  then `"title_ratings"` maps to `src/config_title_ratings.json`. Each path is built from
  `Path(__file__)`, so it resolves wherever the job runs from. The key is the table's name. It
  is the key of the state, the suffix of the steps' names and the last segment of the target
  path.
- `EmptyBatchError`, `RequireRows`, `AddLineage` and `ConvertColumns`, as **Fit with ingestio**
  describes them. Their required argument `key` is positional, and the options are keyword-only
  (`CONTRIBUTING.md`).
- `build_process(configs: Mapping[str, Path]) -> Process`. For each table, in the mapping's
  order, it builds two steps:
  - `Step("extract_<table>", [ReadSource(config, into=<table>)])`;
  - `Step("load_<table>", [RequireRows(<table>), AddLineage(<table>),
    ConvertColumns(<table>, booleans=..., lists=...), FullLoad(config, key=<table>)])`, with
    `ConvertColumns` only when the file has a `convert` member.

  It returns `Process(steps)` with the default run id. Building the process reads each
  configuration file and runs every check, the library's and the job's. It touches no Spark
  session, no network and no other file.
- `main() -> None`, called only under `if __name__ == "__main__":`, does the following:
  1. It configures logging: `ingestio.log_to_console()` for the run's tree, and no handler on
     the root logger (pitfall 19). Spark's own output goes to standard error, as Spark
     configures it.
  2. It builds the process with `build_process(TABLES)`, so that a configuration mistake fails
     before a JVM starts.
  3. It builds the session with
     `spark_session.build_session(gcs_connector_jar=spark_session.GCS_CONNECTOR_JAR)`.
  4. It runs the process.
  5. It stops the session, whether the run succeeded or failed.

  An exception propagates, so the job exits with a non-zero status. `main()` downloads nothing,
  reads no credential and touches the bucket only through `FullLoad`.

### The Spark session: `src/spark_session.py`

- `GCS_CONNECTOR_JAR = Path("/opt/jars/gcs-connector-shaded.jar")` is where the image puts the
  shaded JAR of the Cloud Storage connector (M3). The path carries no version. The connector's
  version is pinned in one place, the `Dockerfile`.
- `build_session(*, gcs_connector_jar: Path | None = None) -> SparkSession` returns the
  session through `SparkSession.builder...getOrCreate()`, with the settings below. When
  `gcs_connector_jar` is given and is not an existing file, it raises
  `ValueError(f"gcs connector jar not found: {gcs_connector_jar}")` before Spark starts. When it
  is `None`, which is how the tests call it, the job does not set `spark.jars`, and every other
  setting is the same. pyspark starts the JVM through spark-submit, which adds to `spark.jars`
  the JARs it resolves for `spark.jars.packages`, Delta Lake's among them. So the running session
  may still have `spark.jars` set. What the job guarantees is that it then holds no JAR of the
  Cloud Storage connector.

| Setting | Value | Why |
|---|---|---|
| master | `local[2]` | Spark runs in local mode in the container. One gzip file is read by one task (pitfall 4), and the tables load one after another (M11) |
| `spark.app.name` | `ingestion_disk_to_gcs_delta` | |
| `spark.driver.memory` | `2g` | M11 |
| `spark.ui.enabled` | `false` | nothing exposes the UI's port |
| `spark.sql.session.timeZone` | `UTC` | pitfall 18 |
| `spark.jars.packages` | `io.delta:delta-spark_2.13:<version of the installed delta-spark>` | the coordinate follows the `delta-spark` package of the lock, so the lock is the one place of Delta Lake's version. delta-spark's own `configure_spark_with_delta_pip` builds the same coordinate, when the installed release provides it. The JARs come from Maven Central when the session starts (M10) |
| `spark.jars` | set by the job to `gcs_connector_jar` only when it is given. Without it, the session's `spark.jars` holds no JAR of the Cloud Storage connector, whatever spark-submit adds to it for `spark.jars.packages` | the Cloud Storage connector, baked into the image (M3) |
| `spark.sql.extensions` | `io.delta.sql.DeltaSparkSessionExtension` | |
| `spark.sql.catalog.spark_catalog` | `org.apache.spark.sql.delta.catalog.DeltaCatalog` | |
| `spark.hadoop.fs.gs.impl` | `com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem` | |
| `spark.hadoop.fs.AbstractFileSystem.gs.impl` | `com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS` | |
| `spark.hadoop.fs.gs.auth.type` | `APPLICATION_DEFAULT` | the connector's default asks a Google Cloud machine's metadata server (pitfall 12) |

No other authentication setting is made: no key file, no service account, no token, no
`google.cloud.auth.*`. The connector finds the file that `GOOGLE_APPLICATION_CREDENTIALS`
names. No log store of Delta Lake is set, so Delta Lake uses its own for the `gs` scheme, and the
first real run confirms the commits (pitfall 13).

### The download script: `scripts/download_imdb.py`

It is run on the host, with the standard library only:
`uv run python scripts/download_imdb.py <directory> [--base-url <url>]`.

- It downloads, in order, `title.basics.tsv.gz` and `title.ratings.tsv.gz` from `<base-url>`,
  which is `https://datasets.imdbws.com/` by default. Each file goes to
  `<directory>/<IMDb name>/<IMDb name>.tsv.gz`, such as `data/title.basics/title.basics.tsv.gz`,
  one directory per file (pitfall 3). It creates the directories that are missing.
- It writes each file under a temporary name in the same directory,
  `.<IMDb name>.tsv.gz`, and renames it to its final name with `os.replace` once the download is
  complete (pitfall 6). The leading `.` keeps `DiskReader` from selecting a partial file. The
  `.tsv.gz` ending keeps it under `.gitignore`, even when the directory sits inside the
  repository.
- It sets the file's modification time to the server's `Last-Modified` header when the response
  has one, as `curl --remote-time` does. `_source_modified_at` then names IMDb's snapshot rather
  than the time of the download.
- It logs one `INFO` line per file to standard error, naming the URL and the destination, and
  exits with status 0.
- On a failure, such as an HTTP status other than 200, a network error or a timeout, it removes
  its temporary file and leaves an existing file at the destination as it was. It logs a line to
  standard error that names the URL and the error, and exits with status 1 without trying the
  files after it. Files that this run already downloaded stay.
- Every run downloads both files again. It reads no configuration file of `src/`. Its list of
  files and the `source.path` of the configuration files agree by this spec, and the tests check
  both sides.

### The container

**`Dockerfile`**, at the root of the repository (M3, M18):

- Java 17 (M3): the base image is `eclipse-temurin:17-jre-noble`, the same JDK vendor as CI
  (M10).
- uv comes from a pinned tag of `ghcr.io/astral-sh/uv`. The image copies `pyproject.toml`,
  `uv.lock` and `.python-version` to `/app` and runs `uv sync --locked`. That installs the `dev`
  group, with `pyspark==4.2.0` and `delta-spark==4.4.1` (M3, M10), on the Python 3.12 of
  `.python-version`, which uv provides. It then copies `src/`. The first build downloads and
  builds pyspark's source archive.
- Every `COPY` names its files: those three and `src/`. No `COPY .`, `ADD .` or other instruction
  copies the whole build context. Together with `.dockerignore`, this keeps the repository's
  credential files out of the image. Those include `.dotenv`, which has no role in the job but
  may stay at the root of the working tree with a service account's key in it (M8).
- The shaded JAR of `com.google.cloud.bigdataoss:gcs-connector` 4.0.6 (M10) comes from Maven
  Central and is put at `/opt/jars/gcs-connector-shaded.jar`, with `ADD --checksum=sha256:...`.
  Its version and digest are pinned in the `Dockerfile` alone.
- Its default command keeps the container running and does nothing else, so that
  `docker compose exec` runs the job (M3).
- No credential, no IMDb file and no variable that names a credential is in the image.

**`.dockerignore`**, at the root (M18). The build context is the repository directory, and
that directory may hold a credential file: `.dotenv`, with a service account's key in it, which
the working tree's `.gitignore` ignores. The job never uses it (M8). `.dockerignore` therefore:

- starts by excluding everything (`*`), and then admits only `pyproject.toml`, `uv.lock`,
  `.python-version` and `src/`;
- after those lines, excludes again, wherever they appear, including inside `src/`:
  - `.dotenv`, `.env` and `.env*`;
  - every credential pattern of `.gitignore`, from `application_default_credentials.json` and
    `*credentials*.json` to `*.pem`, `.gcloud/`, `gcloud/` and `secrets/`;
  - the IMDb data: `*.tsv`, `*.tsv.gz` and `data/`.

No credential and no IMDb file can then reach the build context, and so neither can reach the
image. No stage of the workflow reads `.dotenv`.

**`compose.yaml`**, at the root (M3, M4, M18), with one service, `job`, built from the
`Dockerfile`:

- Two read-only bind mounts, in the long syntax with `create_host_path: false`. A host path that
  does not exist then fails `docker compose up` instead of being created as an empty directory:
  - the data directory: `${IMDB_DATA_DIR}`, by default `./data`, mounted at `/data` (M17);
  - the credential file: `${GOOGLE_ADC_FILE}`, the absolute path of the service account's key
    file on the host, outside the repository, mounted at `/run/secrets/adc.json` (M8). The
    variable has no default. Compose's `${GOOGLE_ADC_FILE:?...}` form makes
    `docker compose up` fail, with a message that names the variable, when it is unset. That way
    the job never runs with another identity by accident.
- `GOOGLE_APPLICATION_CREDENTIALS=/run/secrets/adc.json` is set in the service's environment
  (M4). The variable names a path, as Application Default Credentials require, and never holds
  the key itself.
- No port, no other variable, and no value of a credential.
- `.env`, which `.gitignore` already ignores and which compose reads by default, holds only
  paths: `GOOGLE_ADC_FILE`, and `IMDB_DATA_DIR` when it differs from the default. It never holds
  a key. It is never committed. `.env.*` is ignored too, so the variables are documented in
  `README.md`, not in an example file. The job does not read `.dotenv`, which has no role in it
  (M8).
- The container runs as the image's default user, root, so that it can read a mounted credential
  file whatever the file's owner and mode (Decision D13).

**How the job is run**, which the documentation shows, and which `CONTRIBUTING.md`, section
**Layout**, names (M19):

```bash
uv run python scripts/download_imdb.py data      # on the host: the two IMDb files
docker compose up -d --build                     # build the image, start the container
docker compose exec job uv run --locked python src/main.py
docker compose down                              # when done
```

The container needs the network when it runs: Maven Central for Delta Lake when the session
starts, and Google's APIs for the token and the bucket. Its Ivy cache lives as long as the
container does, so Delta Lake is downloaded on the first run in a new container.

## Behaviour

### A run

1. `main()` builds the process from `main.TABLES`. It reads and checks both configuration files.
   A mistake raises `ValueError` before Spark starts and writes no line of the run's log.
2. It builds the session. A missing connector JAR raises
   `ValueError("gcs connector jar not found: /opt/jars/gcs-connector-shaded.jar")`.
3. It runs the process. For each table, in the order `title_basics`, then `title_ratings`:
   - `extract_<table>`: `ReadSource` lists the table's directory, selects the one file whose
     name starts with neither `.` nor `_`, and stores a lazy DataFrame in `state[<table>]`. No
     row is read yet.
   - `load_<table>`: `RequireRows` runs `isEmpty()`, which reads only the start of the file.
     `AddLineage` and `ConvertColumns` add lazy transformations. `FullLoad` writes the table in
     one Delta commit, reading the whole file once more. No count or other action is run
     (pitfall 4).
4. It stops the session. The job exits with status 0 after a successful run, and with a
   non-zero status after any error. `docker compose exec` returns that status.

### What a table holds after a run

- After a successful run, each table holds exactly the data rows of the file the run read, each
  row once. The columns and types, in order, are:
  - `title_basics`: `tconst STRING`, `titleType STRING`, `primaryTitle STRING`,
    `originalTitle STRING`, `isAdult BOOLEAN`, `startYear INT`, `endYear INT`,
    `runtimeMinutes INT`, `genres ARRAY<STRING>`, `_run_id STRING`,
    `_source_modified_at TIMESTAMP`;
  - `title_ratings`: `tconst STRING`, `averageRating DOUBLE`, `numVotes INT`, `_run_id STRING`,
    `_source_modified_at TIMESTAMP`.
- Every run adds one version to each table it reaches. Delta Lake's history shows each version
  as a `WRITE` in `Overwrite` mode. The first run creates the table at its path, because
  `GcsWriter` creates a Delta table where the path holds none. Earlier versions stay readable
  through Delta Lake's time travel, and their files stay under the prefix, until a `VACUUM` that
  the job never runs. The prefix therefore grows by a full copy of each table's data files on
  every run. The maintainer accepts this growth (M16), and the documentation states it, adding
  that `VACUUM` may be run outside the job from any Spark with Delta Lake.
- The strategy is idempotent. Running the same files again leaves the same data rows, with the
  new run's `_run_id`, in one more version.

### The log

A successful run logs this tree, where `<id>` is the run id and `<d>` a duration:

```text
[<id>] ┌ process started · 4 steps
[<id>] │ ┌ extract_title_basics (1/4) · 1 callback
[<id>] │ │ ✔ ReadSource.run <d>
[<id>] │ └ extract_title_basics done in <d>
[<id>] │ ┌ load_title_basics (2/4) · 4 callbacks
[<id>] │ │ ✔ RequireRows.run <d>
[<id>] │ │ ✔ AddLineage.run <d>
[<id>] │ │ ✔ ConvertColumns.run <d>
[<id>] │ │ ✔ FullLoad.run <d>
[<id>] │ └ load_title_basics done in <d>
[<id>] │ ┌ extract_title_ratings (3/4) · 1 callback
[<id>] │ │ ✔ ReadSource.run <d>
[<id>] │ └ extract_title_ratings done in <d>
[<id>] │ ┌ load_title_ratings (4/4) · 3 callbacks
[<id>] │ │ ✔ RequireRows.run <d>
[<id>] │ │ ✔ AddLineage.run <d>
[<id>] │ │ ✔ FullLoad.run <d>
[<id>] │ └ load_title_ratings done in <d>
[<id>] └ process finished in <d>
```

A failing callback logs `✖ <Callback>.run failed after <d>: <Type>: <message>` at `ERROR`,
followed by the failed closing lines of the library. The download script logs its own lines on
the host, and those lines are not part of this tree.

### Errors

| When | What fails | What the table keeps |
|---|---|---|
| a configuration file has a mistake in a library section | the library's `ValueError`, when the process is built | every table is unchanged; no line of the run's log |
| a `convert` member has a mistake | `ValueError` whose message starts with the file's path, when the process is built | every table is unchanged |
| the connector JAR is missing from the image | `ValueError("gcs connector jar not found: ...")`, before Spark starts | every table is unchanged |
| a table's directory does not exist | `FileNotFoundError` from `ReadSource.run` (`✖ ReadSource.run failed`) | that table and the ones after it are unchanged |
| a table's batch has no row (no file in the directory, or a header-only file) | `EmptyBatchError("<table>: no row to load")` from `RequireRows.run` (M12) | that table and the ones after it are unchanged |
| a file's header does not name the schema's columns in order | Spark's error, at the first action, usually `RequireRows.run` | that table and the ones after it are unchanged |
| a row does not parse, by its number of fields or a value's type | Spark's `FAILFAST` error, at `RequireRows.run` or `FullLoad.run` | that table and the ones after it are unchanged |
| `isAdult` holds a value other than `0`, `1` or `\N` | Spark's error from the write, whose message holds `isAdult` and the value | that table and the ones after it are unchanged |
| no credential file, credentials refused, missing privileges, a missing bucket | the error of Spark or of the connector, from `FullLoad.run` | the Delta commit is not made |
| the path holds a Delta table with another schema | Delta Lake's schema mismatch error, from `FullLoad.run` | that table and the ones after it are unchanged |

In every case the exception leaves `Process.run()` unchanged, `main()` stops the session and the
job exits with a non-zero status. A failed Delta write makes no commit, so a reader through Delta
sees the table as it was. Tables that loaded before the failing one keep their new version
(pitfall 16). Because the strategy is idempotent, running again after the cause is fixed brings
every table to the same snapshot.

### Edge cases

- **A leftover temporary file**, such as `.title.basics.tsv.gz` after an interrupted download,
  is never selected by the reader and is overwritten by the next download.
- **Any other file in a table's directory** whose name starts with neither `.` nor `_` is
  loaded together with the IMDb file, because the reader loads every candidate. The directories
  under the data directory belong to the job.
- **Downloading during a run** is not supported. A file renamed between the extract step and the
  write would be read as it is at the write.
- **Two runs at once** against the same tables are not supported. This spec makes no claim
  about concurrent Delta commits on `gs://` from two drivers.
- **An empty source** is an error (M12). Every run follows a complete download, so a table with
  no row points to a broken environment.

### The first run against the bucket

The documentation tells the maintainer to do these steps and checks on the first real run:

1. The bucket `ingestio-imdb-delta` exists and is not publicly readable. The maintainer creates
   it (M5). Nothing exists under its `imdb/` prefix yet: a Delta table there with another schema
   would make the first overwrite fail.
2. The service account holds `roles/storage.objectUser` and `roles/storage.legacyBucketReader`,
   bound on the bucket only and not on the project (M9). Together the two roles hold the
   minimum a Delta write on this prefix needs:

   | Permission | Why |
   |---|---|
   | `storage.objects.create` | to write data files and commits |
   | `storage.objects.delete` | for overwrites and for the copies of commits |
   | `storage.objects.get` | to read the table's log before a commit |
   | `storage.objects.list` | to list the prefix |
   | `storage.buckets.get` | the connector may need it |

   A role that grants create alone, such as `roles/storage.objectCreator`, fails the first
   overwrite with `403`.
3. The service account's key is a file outside the repository, and `.env` names its path in
   `GOOGLE_ADC_FILE` (M8).
4. Download the files, then start the container and run the job with the commands of
   **The container**.
5. The run's log ends with `└ process finished`, after a `✔ FullLoad.run` line for each table.
6. `gcloud storage ls gs://ingestio-imdb-delta/imdb/title_basics/_delta_log/` lists the table's
   first commit, and likewise for `title_ratings`.
7. Some failures have a likely cause:

   | Error | Likely cause |
   |---|---|
   | names the metadata server or Compute Engine | the authentication setting did not apply |
   | `403` | missing privileges |
   | `404` on the bucket | the bucket does not exist |
   | `ClassNotFoundException` for `GoogleHadoopFileSystem` | the connector JAR is not on the classpath |

   A failure that the library's documentation does not explain goes to the maintainer, with the
   versions of pyspark, Delta Lake and the connector, as pitfall 13 asks.

## Acceptance criteria

Each criterion is testable on its own, from outside the job's code. Its tests are named after
it: `AT-001` is covered by `test_at001_<description>` in `tests/test_ingest_imdb_datasets.py`.

"A local target" means the test's own configuration, whose target is
`{"system": "disk", "path": "<tmp_path>/<table>", "format": "delta"}`. "Unchanged" means that the
table holds the same rows and the same number of versions in its history as before the run, or
that no Delta table exists at the path when none existed before.

**The shipped configuration** (no Spark):

- **AT-001**: `main.TABLES` maps, in this order, `title_basics` to `src/config_title_basics.json`
  and `title_ratings` to `src/config_title_ratings.json`, and both files exist.
- **AT-002**: Each shipped configuration file holds exactly the members, keys and values that
  section **Configuration** gives for it, and nothing else. That includes the source directory
  `/data/<IMDb name>`, the schema, the six options, the target `gs://ingestio-imdb-delta/imdb/<table>`
  in the `delta` format with no `options`, and the `convert` member, for `title_basics` only.
- **AT-003**: Building `ReadSource` and `FullLoad` from each shipped configuration file raises
  nothing, and neither does `main.build_process(main.TABLES)`.
- **AT-004**: Given a configuration file whose `convert` member breaks a rule of
  **The job's own member `convert`**, `main.build_process` raises `ValueError` whose message
  starts with the file's path. The cases are:
  - a `convert` that is not an object, such as a list;
  - a key other than `booleans` and `lists`;
  - a value that is not a list of column names;
  - an empty column name, `""`;
  - no column in total, as in `{}` or `{"booleans": [], "lists": []}`.

**The download script** (run as a subprocess against a local HTTP server, no Spark):

- **AT-005**: Given a local HTTP server that serves `title.basics.tsv.gz` and
  `title.ratings.tsv.gz` with a `Last-Modified` header, when
  `scripts/download_imdb.py <directory> --base-url <server>` runs and `<directory>` does not
  exist yet, then the script:
  - exits with status 0;
  - writes `<directory>/title.basics/title.basics.tsv.gz` and
    `<directory>/title.ratings/title.ratings.tsv.gz`, with the bytes served and the
    `Last-Modified` instant as their modification times;
  - leaves no other file in either directory;
  - writes a line to standard error for each file, naming its URL.
- **AT-006**: Given the two files already downloaded, a second run against a server that serves
  other bytes replaces them, and each directory still holds its one file.
- **AT-007**: Given a server that answers `404` for `title.basics.tsv.gz` and a destination that
  already holds that file, the script exits with status 1 and writes the file's URL to standard
  error. The existing file keeps its bytes and its modification time, and no temporary file is
  left in its directory.

**The Spark session** (Spark):

- **AT-008**: `spark_session.build_session()` returns a session whose configuration holds every
  setting of the table in **The Spark session** except `spark.jars`. Its `spark.jars` holds no
  JAR of the Cloud Storage connector, whatever spark-submit adds to it for
  `spark.jars.packages`. Its `spark.jars.packages` holds
  `io.delta:delta-spark_2.13:<version of the installed delta-spark>`.
  The only key of its configuration that contains `auth`, `credential`, `keyfile` or
  `service.account`, in any case, is `spark.hadoop.fs.gs.auth.type`. The installed pyspark is
  `4.2.0` and the installed delta-spark is `4.4.1`.
- **AT-009**: `spark_session.GCS_CONNECTOR_JAR` is `Path("/opt/jars/gcs-connector-shaded.jar")`,
  and `spark_session.build_session(gcs_connector_jar=<a path under tmp_path that does not exist>)`
  raises `ValueError` whose message is `gcs connector jar not found: <that path>`.

**The process** (Spark and Delta Lake, local targets, IMDb-shaped files the test writes):

- **AT-010**: Given a gzip-compressed file for each table, with IMDb's header and three data rows
  of the test's own, in its own directory, when `main.build_process(configs).run()` runs on local
  targets, then a Delta table exists at each target. Its columns and types, in order, are those
  of **What a table holds after a run**, and it holds one row per data line of its file.
- **AT-011**: Given a `title_basics` file whose rows hold:
  - `\N` in `endYear`, `runtimeMinutes` and `genres`;
  - a `primaryTitle` that starts with `"` and holds another `"` later;
  - `isAdult` values `0` and `1`;
  - `genres` `Comedy,Drama`,

  then the table holds null for each `\N`, the titles exactly as the file writes them, `false`
  and `true` for `isAdult`, and `["Comedy", "Drama"]` for `genres`.
- **AT-012**: Given files whose modification times the test set to whole seconds, every row of
  each table holds the run's `process.run_id` in `_run_id`, and its file's modification time, in
  UTC, in `_source_modified_at`.
- **AT-013**: Given tables loaded by a first run, a second run on files with other rows leaves
  each table holding exactly the second file's rows. Its history holds two versions, each a
  `WRITE` in `Overwrite` mode.
- **AT-014**: Given the same files run twice, each table holds each data line once, with the
  second run's `_run_id`, and its history holds two versions.
- **AT-015**: Given a `title_ratings` directory that holds no file, the run raises
  `main.EmptyBatchError` with the message `title_ratings: no row to load`. The log holds
  `✖ RequireRows.run failed after <d>: EmptyBatchError: title_ratings: no row to load`. The
  `title_basics` table holds its rows, and no Delta table exists at `title_ratings`'s target.
- **AT-016**: Given a table loaded by a first run, a second run on a file with a header line and
  no data row raises `main.EmptyBatchError`, and the table is unchanged.
- **AT-017**: Given a `title_basics` source directory that does not exist, the run raises
  `FileNotFoundError`, the log holds `✖ ReadSource.run failed`, and no Delta table exists at
  either target.
- **AT-018**: Given a table loaded by a first run, a second run on a file in which one row holds
  `19x5` in `startYear` raises, and the table is unchanged.
- **AT-019**: Given a table loaded by a first run, a second run on a file in which one row has
  one field fewer than the header raises, and the table is unchanged.
- **AT-020**: Given a table loaded by a first run, a second run on a file whose header names the
  schema's columns in another order raises, and the table is unchanged.
- **AT-021**: Given a `title_basics` table loaded by a first run, a second run on a file in which
  one row holds `2` in `isAdult` raises an error whose message holds `isAdult`, and the table is
  unchanged.
- **AT-022**: Given a successful run of both tables, the records of the `ingestio` logger hold,
  in this order, every line of the tree in **The log**, from `process started · 4 steps` to
  `process finished`, each beginning with `[<process.run_id>]`.

### Notes for testing

- **What the tests leave out.** No test reaches GCS or needs a credential. No test runs
  `main.main()`, builds the image or starts compose. The transport to `gs://`, the credentials,
  the connector JAR on the classpath and the container are exercised by the first real run.
  AT-002 and AT-003 cover the `gcs` targets the job ships, without Spark.
- **Configurations.** Process tests write their configurations with `json.dumps` under
  `tmp_path`. The members are those of the shipped files, the `convert` member of `title_basics`
  included, except that `source.path` is a directory under `tmp_path` and the target is local.
- **Source files.** Each file is gzip-compressed with Python's `gzip` module, tab-separated,
  with IMDb's header and `\N`, and its rows are the test's own. Each file sits in its own
  directory. Modification times are set with `os.utime(file, ns=(n, n))`.
- **Spark.** One session serves the whole test run: `spark_session.build_session()`, in a
  session-scoped fixture of `tests/conftest.py`, built before any other session (pitfall 17).
  Tests that need it are marked `@pytest.mark.integration`. CI needs Maven Central for Delta
  Lake, and not for the connector, which the tests do not load.
- **Reading back.** The tests read the tables with `spark.read.format("delta").load(<path>)`,
  and their history with ``DESCRIBE HISTORY delta.`<path>` ``. That is a check of the output, not
  part of the job.
- **The log.** Tests call `caplog.set_level(logging.INFO, logger="ingestio")` and use
  `process.run_id` for the prefix of each line.
- **The script.** Tests run it with `sys.executable`, because pytest puts only `src/` on the
  path. The server is `http.server` in a thread on `127.0.0.1`. Its `SimpleHTTPRequestHandler`
  sends `Last-Modified` from the served file's modification time, and a file it does not have
  answers `404`.

## Library gaps

None. The process meets every rule of a valid job with the public API.

Two other choices would each have run into a gap: an incremental upsert by key, because no
shipped callback merges into a Delta table at a `gs://` path, and a prefix kept to a bounded
size, because ingestio offers no maintenance of a Delta target. The maintainer chose `FullLoad`
(M7) and accepted the growth of the prefix (M16), so neither gap arises, and none is recorded.

## Decisions

**Decided by the maintainer**, relayed by the main session, all dated 2026-11-18:

- **M1.** The job loads `title.basics.tsv.gz` and `title.ratings.tsv.gz` only, as two Delta
  tables under `gs://<bucket>/<prefix>`.
- **M2.** A script of the job downloads the IMDb files. It lives in the repository, outside
  `src/`, as `scripts/download_imdb.py`, and keeps to pitfalls 6 and 3: a temporary name and
  then a rename, and one directory per file. The files are never committed.
- **M3.** A `Dockerfile` with Java 17 installs pyspark and delta-spark from the `dev` dependency
  group, where both are added and pinned so that they match, and puts the shaded JAR of the
  Cloud Storage connector in the image. A `compose.yaml` starts the container, and the job runs
  with `docker compose exec`.
- **M4.** Compose mounts the host directory of the IMDb files at `/data`, and mounts the ADC file
  read-only, with `GOOGLE_APPLICATION_CREDENTIALS` set to its path inside the container.
- **M5.** The bucket is `ingestio-imdb-delta`. The maintainer creates it before the first real
  run.
- **M6. The prefix and the table paths.** The prefix is `imdb`, and the table directories are
  `title_basics` and `title_ratings`: `gs://ingestio-imdb-delta/imdb/title_basics` and
  `gs://ingestio-imdb-delta/imdb/title_ratings`. Each path reads as dataset and table, matches
  the names that the log and the state use, and needs no quoting in a SQL path.
- **M7. The load strategy is `FullLoad` with no watermark,** for both tables. Every run reloads
  each file and replaces its table in one Delta commit. Each IMDb file is a full snapshot, so
  the table holds exactly the latest snapshot. The strategy is idempotent, keeps no state outside
  the table, and repairs a failed run by running again.
- **M8. The credential is a key file outside the repository.**
  - `.env`, which `.gitignore` ignores and compose reads by default, holds only the key file's
    path, in `GOOGLE_ADC_FILE`.
  - Compose mounts the file read-only at `/run/secrets/adc.json` and sets
    `GOOGLE_APPLICATION_CREDENTIALS` to that path. That is Application Default Credentials
    provided by the environment, as `AGENTS.md` describes them, and the key never passes through
    a variable.
  - `.dotenv` has no role in the job. `.dockerignore` keeps excluding it, because it may stay in
    the working tree.
- **M9. The identity's privileges.** `roles/storage.objectUser` and
  `roles/storage.legacyBucketReader`, bound on the bucket only. Together they hold the minimum
  that **The first run against the bucket** lists, and the documentation asks for them.
- **M10. The versions.**
  - `pyspark==4.2.0` and `delta-spark==4.4.1` in the `dev` group. This is the newest pair the
    knowledge base found compatible: delta-spark 4.4.1 declares `pyspark>=4.0.1,<=4.2.0`.
  - The Cloud Storage connector `gcs-connector` 4.0.6, as a shaded JAR. It is the release the
    library's own documentation and tests name.
  - The base image `eclipse-temurin:17-jre-noble`, with Python 3.12 from uv.
  - Delta Lake's JARs come from Maven Central when the session starts.
- **M11. The size of Spark** is `local[2]` with `spark.driver.memory` set to `2g`.
- **M12. An empty source is an error.** `RequireRows` raises `EmptyBatchError`, and the run fails
  with a non-zero status. The table keeps its version, and the tables after it are not run.
- **M13. The tables are typed.** Years, minutes and votes are `INT`, and `averageRating` is
  `DOUBLE`. `isAdult` becomes `BOOLEAN` and `genres` becomes `ARRAY<STRING>`, through
  `ConvertColumns`. The columns are confirmed: the main session checked them against IMDb's
  page, https://data.imdb.com/non-commercial-datasets/. The names, the order, the header line
  and `\N` match **Configuration**.
- **M14. The lineage columns** are `_run_id` and `_source_modified_at`.
- **M15. Strict parsing**: `mode` set to `FAILFAST` and `enforceSchema` set to `false`.
- **M16. The growth of the prefix is accepted and documented.** No gap is recorded for it.
- **M17. The files live in `./data` on the host.** `data/` is added to `.gitignore`, and
  `IMDB_DATA_DIR` in `.env` overrides the location.
- **M18. The layout exception is accepted.** The implementer writes the following, although the
  implementer's row in `docs/workflow.md` does not list them:
  - the `Dockerfile`, `.dockerignore`, `compose.yaml` and `scripts/download_imdb.py`, at the root
    of the repository;
  - `pyspark` and `delta-spark` in the `dev` group of `pyproject.toml`.

  The validator's check 9 reads all of them as authorized for this change.
- **M19. One configuration file per table is kept, and `CONTRIBUTING.md` is changed for this
  job.** At the maintainer's request, the main session edited `CONTRIBUTING.md`, section
  **Layout**. It now says that:
  - each table has its own `src/config_<table>.json`, listed in load order by `main.py`;
  - the `Dockerfile`, `.dockerignore`, `compose.yaml` and `scripts/` are the only code of the job
    at the root;
  - the job runs with `docker compose exec job uv run --locked python src/main.py`.

  `CONTRIBUTING.md` is a file the hub generates. This edit is authorized for this job, and the
  validator's check 9 does not fail on it.

**Taken by this spec:**

- **D1. One configuration file per table**, `src/config_<table>.json`, listed in load order by
  `main.TABLES`. A file holds one source and one target (`.claude/kb/ingestio.md`, section 3).
  `CONTRIBUTING.md`, section **Layout**, now names the files this way (M19).
- **D2. Table names** `title_basics` and `title_ratings`. Each is the key of the state, the suffix
  of its steps' names and the last segment of its target path.
- **D3. Two steps per table**, `extract_<table>` and `load_<table>`. `title_basics` runs first,
  then the `title_ratings` that refer to it. A failure in `title_ratings` then leaves the
  titles current.
- **D4. The order of the load step** is `RequireRows`, `AddLineage`, `ConvertColumns` and then
  `FullLoad`. The guard comes before anything reads `_metadata` (pitfall 2). Lineage reads
  `_metadata` from the reader's own DataFrame, before any conversion. The guard's `isEmpty()` is
  the only action before the write (pitfall 4).
- **D5. The conversions are a member of the table's configuration file**, next to the schema
  they complete, so that a table's shape lives in one file.
- **D6. Strict reading**: `quote` off, `FAILFAST` and `enforceSchema` set to `false` (M15). A
  conversion that meets an unexpected value fails as well, through Spark's error at the write,
  with no extra pass over the file.
- **D7. No target `options`.** An overwrite with another schema fails, so a change to a table's
  schema needs a spec of its own (pitfall 14).
- **D8. `main()` builds the process before the session**, so that every configuration mistake
  fails before a JVM starts. It stops the session in every case.
- **D9. The session lives in `src/spark_session.py`**, shared by `main.py` and the tests' fixture,
  so that the tests run on the job's own session. The only difference is the connector JAR,
  which exists only in the image. A missing JAR fails before Spark starts, instead of at the
  first `gs://` write after the files are read.
- **D10. The download uses the standard library alone**, so it adds no dependency. The temporary
  name `.<IMDb name>.tsv.gz` is hidden from `DiskReader` and still covered by `.gitignore`.
  Modification times come from `Last-Modified`.
- **D11. Read-only mounts, in the long syntax with `create_host_path: false`.** The job never
  writes to `/data`. A missing host path fails `docker compose up` instead of turning into an
  empty directory.
- **D12. A build context closed to credentials.** `.dockerignore` admits only the files the
  image needs and excludes `.dotenv`, `.env*` and every credential pattern of `.gitignore`.
  Every `COPY` names its files. The repository directory may hold a service account's key in
  `.dotenv`, which has no role in the job (M8), and a `COPY .` would put it in a layer of the
  image.
- **D13. The container runs as root,** the image's default user. A credential file is usually
  readable by its owner alone, and root reads it whatever its owner. A non-root user would need
  the file's mode or owner changed on the host.
- **D14. One run at a time.** The job makes no claim about concurrent commits.
- **D15. Version `0.2.0`.** The change is the job's first feature, and `pyproject.toml` holds
  `0.1.0`, which carries only the architecture. A feature bumps the minor version
  (`CONTRIBUTING.md`, **Versions and the changelog**).

## Open questions

None. The maintainer answered every question, and the answers are M6 to M19 under
**Decisions**.

## History

One entry per stage, recorded by the main session from the stage's report.

| Date | Stage | Status | Summary |
|---|---|---|---|
| 2026-11-18 | planning | draft | Wrote the first spec (version 0.2.0): `title.basics` and `title.ratings`, downloaded on the host by `scripts/download_imdb.py`, read with `ReadSource` and replaced as Delta tables in `gs://ingestio-imdb-delta/<prefix>/` with `FullLoad`, with three callbacks of the job's own in between, the session in `src/spark_session.py`, and a `Dockerfile` and `compose.yaml` that mount the data and the credential file read-only. Records the maintainer's decisions M1 to M5 and 22 acceptance criteria, and leaves Q1 to Q12 open. No library gap. The main session checked the IMDb columns against https://data.imdb.com/non-commercial-datasets/ (the old developer.imdb.com URL redirects there): they match. |
| 2026-11-18 | planning | draft | Revised with the maintainer's answers to Q1 to Q12, recorded as M6 to M17: prefix `imdb`, `FullLoad` with no watermark, the key file outside the repository mounted read-only, `roles/storage.objectUser` + `roles/storage.legacyBucketReader` on the bucket, the recommended versions and sizing, and the rest as recommended. M18 authorizes the root files and the two `dev` packages for the implementer. M19 records the maintainer-requested edit of `CONTRIBUTING.md` (one configuration file per table, the container files at the root), made by the main session. No open question and no library gap; process, configuration and acceptance criteria are unchanged. |
| 2026-11-18 | approval | approved | Approved by the maintainer. |
| 2026-11-18 | tests | approved | Wrote `tests/test_ingest_imdb_datasets.py` (35 items for AT-001 to AT-022: 21 without Spark, 14 integration tests on the job's own session) and `tests/conftest.py` (the session-wide `spark` fixture, built with `spark_session.build_session()`). All fail for the missing implementation (`main`, `spark_session`, the configuration files, the download script). The Spark tests were checked by reading only, because the host's only JDK is Java 27. |
| 2026-11-18 | tests → planning | draft | Returned to planning: AT-008 asks for `spark.jars` to be unset in the tests' session, which no correct implementation can meet, because spark-submit adds the JARs it resolves from `spark.jars.packages` (Delta Lake's) to `spark.jars`. The test checks instead that `spark.jars` holds no JAR of the Cloud Storage connector, pending the planner's wording. |
| 2026-11-18 | planning | draft | Revised after the return from tests: AT-008 and **The Spark session** now say that the job sets `spark.jars` only when `gcs_connector_jar` is given, and that without it the session's `spark.jars` holds no JAR of the Cloud Storage connector, whatever spark-submit adds for `spark.jars.packages`. AT-004 now lists every case that the rules of the `convert` member already reject. Nothing else changed. |
| 2026-11-19 | approval | approved | Approved again by the maintainer, after the revision of AT-008 and AT-004. On the host, the stages run the Spark tests with `JAVA_HOME=/usr/lib/jvm/java-17-openjdk`, by the maintainer's decision: the host's default JDK stays Java 27. |
| 2026-11-19 | tests | tested | Aligned AT-008's comments with the revised wording: the job sets `spark.jars` only when `gcs_connector_jar` is given, and without it `spark.jars` holds no JAR of the Cloud Storage connector, whatever spark-submit adds. Organized AT-004 by the spec's five cases and added two isolated ones (a string `convert`, an empty name beside a valid column), for 37 items in all (23 without Spark, 14 integration tests). Every item fails for the missing implementation (`main`, `spark_session`, the configuration files, the download script); against throwaway stand-ins written from the spec, every test passed on Java 17, the Spark tests included. No library gap. |
| 2026-11-19 | documentation | documented | Documented the process in a new page, `docs/ingest_imdb_datasets.md`: the two files, the host-side download script, the container with read-only mounts, authentication through Application Default Credentials only, the two configuration files and the `convert` member, the Spark session, the output and lineage columns, the log, the errors, the first run against the bucket, and how to run the tests (Java 17 or 21, `JAVA_HOME` on the command). Updated `docs/index.md`, `README.md` (the whole run sequence) and `CHANGELOG.md` under 0.2.0. Spec and tests were consistent. |
| 2026-11-19 | implementation | implemented | Implemented version 0.2.0: `src/main.py` (`TABLES`, `EmptyBatchError`, the callbacks `RequireRows`, `AddLineage` and `ConvertColumns`, and `build_process` with the validation of `convert`), `src/spark_session.py` (the Delta and ADC session, `spark.jars` only with a connector JAR), `src/config_title_basics.json` and `src/config_title_ratings.json`, `scripts/download_imdb.py` (standard library only), a `Dockerfile` with the gcs-connector 4.0.6 shaded JAR pinned by checksum, `.dockerignore` and `compose.yaml` (read-only mounts, `GOOGLE_ADC_FILE` required). `pyproject.toml` at 0.2.0 with `pyspark==4.2.0` and `delta-spark==4.4.1` in `dev`, `uv.lock` regenerated, `data/` added to `.gitignore`. 37 tests pass on Java 17, and ruff and mypy are clean. The image was not built and no bucket was reached. No library gap. |
| 2026-11-19 | validation | validated | Passed. All 22 acceptance criteria have passing tests (37 passed on Java 17; the 23 tests without Spark also pass on the default JDK), and ruff, the format check, strict mypy and `uv lock --check` are clean. The code matches the documentation and every rule of a valid job. The credentials exception holds: no secret in the tree, the bucket and the prefix only in the `target` sections, no test reaches GCS, no IMDb file tracked. `docker compose config` and the Maven Central checksum of the gcs-connector JAR were checked. No library gap. |
| 2026-11-19 | validation → documentation | tested | Returned by the maintainer's decision to fix the validator's non-blocking notes before the commit. Documentation: `README.md` still names `src/config.json`, the v0.2.0 entry of `CHANGELOG.md` has no date, and the status of the spec in `docs/index.md` is stale. Implementation: `main(argv=None)` of `scripts/download_imdb.py` takes an option that is not keyword-only. The maintainer leaves as it is that `ConvertColumns` skips a `convert` column absent from the batch, a case the spec does not cover. |
| 2026-11-19 | documentation | documented | Fixed the three documentation notes: `README.md` now refers to `src/config_<table>.json`, the v0.2.0 heading of `CHANGELOG.md` carries the date 2026-11-19, and the status of the spec in `docs/index.md` is `validated`, the status the spec has at the end of this return. |
| 2026-11-19 | implementation | implemented | `argv` of `main` in `scripts/download_imdb.py` is now keyword-only, as `CONTRIBUTING.md` requires; its only caller, `sys.exit(main())`, needed no change. Behaviour unchanged: 37 tests pass on Java 17, and ruff and mypy are clean. |
| 2026-11-19 | validation | validated | Passed again after the return. The four notes are fixed correctly and introduce nothing new: `README.md` names `src/config_<table>.json`, the v0.2.0 heading carries 2026-11-19, `docs/index.md` gives `validated`, and `argv` of `main` in `scripts/download_imdb.py` is keyword-only. 37 tests pass on Java 17, and ruff, the format check, strict mypy and `uv lock --check` are clean. The code matches the documentation and every rule of a valid job. The credentials exception holds. No library gap. |
