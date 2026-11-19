# Ingest the IMDb datasets into Delta Lake tables on GCS

Spec: [ingest_imdb_datasets](../specs/ingest_imdb_datasets.md). Job version: `0.2.1`.

The job loads two files of the IMDb non-commercial datasets, `title.basics.tsv.gz` and
`title.ratings.tsv.gz`, into two Delta Lake tables of a Google Cloud Storage bucket. A script
downloads the files on the host, a container started by `docker compose` runs the job, and the
job replaces each table with the latest snapshot of its file. The job is built with
[ingestio](https://pypi.org/project/ingestio/0.7.0/) `0.7.0`.

## What it ingests, and where it writes

| | |
|---|---|
| Source | `title.basics.tsv.gz` and `title.ratings.tsv.gz`, from https://datasets.imdbws.com/. Each file is gzip-compressed, tab-separated, in UTF-8, with a header line and `\N` for a missing value. IMDb refreshes them daily under the same names. |
| Target | two Delta Lake tables in the bucket `ingestio-imdb-delta`: `gs://ingestio-imdb-delta/imdb/title_basics` and `gs://ingestio-imdb-delta/imdb/title_ratings` |
| Strategy | a full replace of each table, with no watermark: every run reloads both files, and each table then holds exactly the latest snapshot |

**License.** The IMDb datasets are licensed for personal and non-commercial use only
(https://data.imdb.com/non-commercial-datasets/). The job downloads them where it runs. They are
never committed, never copied into the image, and mounted read-only into the container. The tests
write files with the same layout and rows of their own, never IMDb's data. The bucket must not be
publicly readable.

## Prerequisites

| Prerequisite | How the job gets it |
|---|---|
| Docker with Compose | on the host; it builds the image and starts the container |
| uv and Python | on the host, to run the download script and the tests (`uv sync`) |
| Java 17 | in the image (`eclipse-temurin:17-jre-noble`). Spark 4 needs Java 17 or 21 |
| Spark and Delta Lake | `pyspark==4.2.0` and `delta-spark==4.4.1`, pinned together in the `dev` group of `pyproject.toml`, installed in the image with `uv sync --locked`. Delta Lake's JARs come from Maven Central when the Spark session starts |
| The Cloud Storage connector | the shaded JAR of `gcs-connector` 4.0.6, put in the image at `/opt/jars/gcs-connector-shaded.jar` |
| The bucket | `ingestio-imdb-delta`, which exists before the first run. The job creates no bucket |
| Credentials | Application Default Credentials that the environment provides, see [Authentication](#authentication) |
| The IMDb files | downloaded by `scripts/download_imdb.py`, see [The download](#the-download) |
| The network | the container needs Maven Central when the session starts, and Google's APIs for the token and the bucket |

The image's Ivy cache lives as long as the container does, so Delta Lake is downloaded on the
first run in a new container. The first build of the image downloads and builds the source
archive of pyspark, which is large.

## How to run it

```bash
uv run --locked python scripts/download_imdb.py data   # on the host: the two IMDb files
docker compose up -d --build                     # build the image, start the container
docker compose exec job uv run --locked python src/main.py
docker compose down                              # when done
```

Before `docker compose up`, `.env` names the credential file, see
[Authentication](#authentication). The job exits with status 0 after a successful run and with a
non-zero status after any error, and `docker compose exec` returns that status. Run one job at a
time against the same tables: two runs at once are not supported.

## The download

`scripts/download_imdb.py` runs on the host and uses the standard library only:

```bash
uv run --locked python scripts/download_imdb.py <directory> [--base-url <url>]
```

- It downloads `title.basics.tsv.gz` and then `title.ratings.tsv.gz` from `<url>`, which is
  `https://datasets.imdbws.com/` by default. Each file goes to
  `<directory>/<name>/<name>.tsv.gz`, such as `data/title.basics/title.basics.tsv.gz`: one
  directory per file, created when missing. The job's configuration reads a directory, and every
  file in it, so the two files never share one.
- It writes each file under the name `.<name>.tsv.gz` and renames it when the download is
  complete. The job's reader never selects a name that starts with `.`, so a partial file is
  never read. A leftover temporary file from an interrupted download is overwritten by the next
  one.
- It sets each file's modification time to the server's `Last-Modified` header, when there is one.
  The lineage column `_source_modified_at` then names IMDb's snapshot, not the time of the
  download.
- It logs one `INFO` line per file to standard error, naming the URL and the destination, and
  exits with status 0.
- On a failure (an HTTP status other than 200, a network error, a timeout) it removes its
  temporary file, leaves an existing file at the destination as it was, logs a line naming the URL
  and the error, and exits with status 1 without trying the files after it. Files that this run
  already downloaded stay.
- Every run downloads both files again. Do not download while the job runs.

`data/` is the default directory on the host and is ignored by git. `IMDB_DATA_DIR` in `.env`
changes where compose looks for it. The directories under it belong to the job: any other file in
a table's directory whose name starts with neither `.` nor `_` is loaded together with the IMDb
file.

## The container

The container is described by three files at the root of the repository.

- **`Dockerfile`** starts from `eclipse-temurin:17-jre-noble` (Java 17), takes uv from a pinned
  image, copies `pyproject.toml`, `uv.lock`, `.python-version` and `src/`, and runs
  `uv sync --locked` (Python 3.12, with the `dev` group). It puts the Cloud Storage connector's
  shaded JAR at `/opt/jars/gcs-connector-shaded.jar`. Every `COPY` names its files, none copies
  the whole build context. The default command only keeps the container running, so that
  `docker compose exec` runs the job. The image holds no credential and no IMDb file. The
  connector's version and checksum are pinned in the `Dockerfile` alone.
- **`.dockerignore`** excludes everything and then admits only `pyproject.toml`, `uv.lock`,
  `.python-version` and `src/`. It excludes again, wherever they appear, `.dotenv`, `.env*`, the
  credential patterns of `.gitignore` and the IMDb data (`*.tsv`, `*.tsv.gz`, `data/`). A
  credential file that sits in the working tree cannot reach the build context, nor the image.
- **`compose.yaml`** defines one service, `job`, built from the `Dockerfile`, with two read-only
  bind mounts:
  - `${IMDB_DATA_DIR}`, `./data` by default, at `/data`;
  - `${GOOGLE_ADC_FILE}`, at `/run/secrets/adc.json`.

  Both mounts use `create_host_path: false`: a host path that does not exist fails
  `docker compose up` instead of becoming an empty directory. `GOOGLE_ADC_FILE` has no default,
  and `docker compose up` fails with a message that names it when it is unset. The service sets
  `GOOGLE_APPLICATION_CREDENTIALS=/run/secrets/adc.json`, publishes no port and holds no other
  variable. It runs as the image's default user, root, so that it reads the mounted file whatever
  its owner and mode.

## Authentication

The job authenticates to Cloud Storage only through Application Default Credentials that its
environment provides. No key, credential file or token is committed, copied into the image,
or written in the code or the configuration files.

1. Keep the credential file, a service account's key, outside the repository.
2. Create a `.env` file at the root, which git ignores and compose reads by default. It holds
   paths only, never a key:

   ```text
   GOOGLE_ADC_FILE=<absolute path of the credential file on the host>
   IMDB_DATA_DIR=<only when the data is not in ./data>
   ```

3. Compose mounts the file read-only at `/run/secrets/adc.json` and sets
   `GOOGLE_APPLICATION_CREDENTIALS` to that path. The Cloud Storage connector, inside the Spark
   session, finds the file by that variable. The session sets the connector's authentication type
   to `APPLICATION_DEFAULT`, because the connector's own default asks the metadata server of a
   Google Cloud machine, which a container elsewhere does not have.

The job never reads a credential itself, and no other authentication setting is made.

## Configuration

The job has one configuration file per table, because a file holds one source and one target:
`src/config_title_basics.json` and `src/config_title_ratings.json`. `main.TABLES` lists them in
load order: `title_basics`, then `title_ratings`. Each table's name is the key of the state, the
suffix of the steps' names and the last segment of the target path.

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

`src/config_title_ratings.json` is the same, with the source `path` `/data/title.ratings`, the
schema `tconst STRING, averageRating DOUBLE, numVotes INT`, the target
`gs://ingestio-imdb-delta/imdb/title_ratings` and no `convert` member.

| Key | Value | Meaning |
|---|---|---|
| `source.system` | `disk` | reads files from a directory with Spark |
| `source.path` | `/data/<IMDb name>` | the absolute directory, inside the container, that holds the one file of the table |
| `source.format` | `csv` | Spark's CSV source reads the TSV and decompresses `.gz` from the extension |
| `source.schema` | a DDL string | the file's columns, in its order, and the types of the table |
| `options.sep` | a tab | the field separator |
| `options.header` | `true` | the first line holds the column names |
| `options.nullValue` | `\N` | IMDb's marker of a missing value |
| `options.quote` | the empty string | turns quoting off: IMDb quotes no value, and a title that starts with `"` would swallow the fields after it |
| `options.mode` | `FAILFAST` | a row that does not parse fails the read instead of becoming nulls or being dropped |
| `options.enforceSchema` | `false` | Spark checks the header's names against the schema, by position, and fails on a difference |
| `target.system` | `gcs` | writes to Cloud Storage |
| `target.path` | `gs://ingestio-imdb-delta/imdb/<table>` | the bucket, the prefix `imdb` and the table's directory |
| `target.format` | `delta` | a Delta Lake table |
| `convert` | an object, `title_basics` only | the job's own member, below |

The bucket and the prefix are configuration, in the `target` section, never in the code. The
target has no `options`: a change to a table's schema is a change of the job, decided by a spec.
No watermark is set, because the strategy has none.

**The `convert` member.** It belongs to the job: the library's callbacks ignore it. It is an
object with at most two keys, `booleans` and `lists`, each a list of column names as non-empty
strings, and the two lists together name at least one column. A member with another key, or of
another shape, raises `ValueError` when the process is built, with a message that starts with the
file's path, such as `<path>: convert.bools is not a key`. A table with no `convert` member gets
no conversion step.

**What never goes in a configuration file:** credentials, a key file's path or any authentication
setting; the Spark session and its settings; the write strategy; a watermark; the list and order
of the tables.

## The Spark session

`src/spark_session.py` builds the session before the process runs. Spark runs in local mode in
the container.

| Setting | Value |
|---|---|
| master | `local[2]` |
| `spark.app.name` | `ingestion_disk_to_gcs_delta` |
| `spark.driver.memory` | `2g` |
| `spark.ui.enabled` | `false` |
| `spark.sql.session.timeZone` | `UTC` |
| `spark.jars.packages` | `io.delta:delta-spark_2.13:<version of the installed delta-spark>`, downloaded from Maven Central when the session starts |
| `spark.jars` | the Cloud Storage connector's JAR, `/opt/jars/gcs-connector-shaded.jar` |
| `spark.sql.extensions` | `io.delta.sql.DeltaSparkSessionExtension` |
| `spark.sql.catalog.spark_catalog` | `org.apache.spark.sql.delta.catalog.DeltaCatalog` |
| `spark.hadoop.fs.gs.impl` | `com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem` |
| `spark.hadoop.fs.AbstractFileSystem.gs.impl` | `com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS` |
| `spark.hadoop.fs.gs.auth.type` | `APPLICATION_DEFAULT` |

If the connector JAR is not a file, the job raises
`ValueError("gcs connector jar not found: /opt/jars/gcs-connector-shaded.jar")` before Spark
starts, instead of failing at the first write to `gs://`.

## What a run does

1. The job builds the process from `main.TABLES`. It reads and checks both configuration files.
   A mistake raises `ValueError` before Spark starts, and no line of the run's log is written.
2. It builds the Spark session.
3. It runs the process, one table after the other. For each table there are two steps:
   - `extract_<table>`: `ReadSource` selects the one file of the table's directory and holds it
     as a lazy DataFrame. No row is read yet.
   - `load_<table>`: three callbacks of the job's own and then `FullLoad`:
     - `RequireRows` fails the run when the batch has no row, so that a missing or header-only file
       never empties a table;
     - `AddLineage` adds the lineage columns;
     - `ConvertColumns` (only for tables with a `convert` member) types the `0`/`1` and list
       columns, which Spark's CSV parser cannot type directly: each column of `booleans` goes
       from `INT` to `BOOLEAN` (`0` is `false`, `1` is `true`, null stays null; any other value
       fails the write with a message that holds the column's name and the value, such as
       `isAdult: unexpected value 2`), and each column of `lists` goes from a comma-separated
       string to `ARRAY<STRING>` (null stays null);
     - `FullLoad` replaces the table with the batch in one Delta commit.
4. It stops the session, whether the run succeeded or failed.

## What a run writes

After a successful run, each table holds exactly the data rows of the file the run read, each row
once. The columns, in order:

| `title_basics` | Type |
|---|---|
| `tconst`, `titleType`, `primaryTitle`, `originalTitle` | `STRING` |
| `isAdult` | `BOOLEAN` |
| `startYear`, `endYear`, `runtimeMinutes` | `INT` |
| `genres` | `ARRAY<STRING>` |
| `_run_id` | `STRING` |
| `_source_modified_at` | `TIMESTAMP` |

| `title_ratings` | Type |
|---|---|
| `tconst` | `STRING` |
| `averageRating` | `DOUBLE` |
| `numVotes` | `INT` |
| `_run_id` | `STRING` |
| `_source_modified_at` | `TIMESTAMP` |

A missing value (`\N`) is null. Titles are stored exactly as the file writes them.

**Lineage columns.** `_run_id` holds the run id of the ingestio process, the same value in every
row of the run and in the first field of every line of the log. `_source_modified_at` holds the
last-modified time of the row's file, in UTC.

**Versions.** Every run adds one version to each table it reaches: in Delta Lake's history, a
`WRITE` in `Overwrite` mode. The first run creates the table at its path. Running the same files
again leaves the same data rows, with the new run's `_run_id`, in one more version, so the
strategy is idempotent. Earlier versions stay readable through Delta Lake's time travel, and their
files stay under the prefix until a `VACUUM`, which the job never runs. **The prefix therefore
grows by a full copy of each table's data files on every run.** The maintainer accepts this
growth. A `VACUUM` may be run outside the job, from any Spark with Delta Lake.

## The log

The job logs the run as a tree on standard error, one line each, starting with the run id.
Spark's own output also goes to standard error. A successful run:

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

`<id>` is the run id and `<d>` a duration. A failing callback logs
`✖ <Callback>.run failed after <d>: <Type>: <message>` at `ERROR`, followed by the failed closing
lines. The download script logs its own lines, on the host, outside this tree.

## Errors and edge cases

Every error stops the run, leaves `Process.run()` unchanged, and makes the job exit with a
non-zero status. Every callback is essential: none catches its own failure. A failed Delta write
makes no commit, so a reader through Delta sees the table as it was. Tables that loaded before
the failing one keep their new version, and the tables after it are not run. Because the strategy
is idempotent, running again after the cause is fixed brings every table to the same snapshot.

| When | What fails | What the table keeps |
|---|---|---|
| a configuration file has a mistake in a library section | the library's `ValueError`, when the process is built | every table unchanged; no line of the run's log |
| a `convert` member has a mistake | `ValueError` starting with the file's path, when the process is built | every table unchanged |
| the connector JAR is missing from the image | `ValueError("gcs connector jar not found: ...")`, before Spark starts | every table unchanged |
| a table's directory does not exist | `FileNotFoundError` from `ReadSource.run` | that table and the ones after it unchanged |
| a table's batch has no row (no file in the directory, or a header-only file) | `EmptyBatchError("<table>: no row to load")` from `RequireRows.run` | that table and the ones after it unchanged |
| a file's header does not name the schema's columns in order | Spark's error, at the first action, usually `RequireRows.run` | that table and the ones after it unchanged |
| a row does not parse, by its number of fields or a value's type | Spark's `FAILFAST` error, at `RequireRows.run` or `FullLoad.run` | that table and the ones after it unchanged |
| `isAdult` holds a value other than `0`, `1` or `\N` | Spark's error from the write, whose message holds `isAdult` and the value | that table and the ones after it unchanged |
| no credential file, credentials refused, missing privileges, a missing bucket | the error of Spark or of the connector, from `FullLoad.run` | no Delta commit is made |
| the path holds a Delta table with another schema | Delta Lake's schema mismatch error, from `FullLoad.run` | that table and the ones after it unchanged |

An empty source is an error, not a normal run. Every run follows a complete download, so a table
with no row points to a broken environment.

Not supported: downloading during a run (a file renamed between the extract step and the write
would be read as it is at the write), and two runs at once against the same tables.

## The first run against the bucket

The job's tests never reach GCS. The first real run is where the transport to `gs://`, the
credentials, the connector JAR on the classpath and the container are exercised, and it is the
first use of ingestio's `GcsWriter` with Delta Lake against a real bucket. Do these steps and
checks:

1. The bucket `ingestio-imdb-delta` exists and is not publicly readable. Create it before the
   run. Nothing exists under its `imdb/` prefix yet: a Delta table there with another schema makes
   the first overwrite fail. The bucket, the service account, its two roles and its key are
   created once with `gcloud`, replacing the placeholders:

   ```bash
   gcloud config set project <PROJECT_ID>
   gcloud storage buckets create gs://ingestio-imdb-delta --location=<LOCATION> \
     --uniform-bucket-level-access --public-access-prevention
   gcloud iam service-accounts create <SA_NAME>
   SA=<SA_NAME>@<PROJECT_ID>.iam.gserviceaccount.com
   gcloud storage buckets add-iam-policy-binding gs://ingestio-imdb-delta \
     --member="serviceAccount:$SA" --role=roles/storage.objectUser
   gcloud storage buckets add-iam-policy-binding gs://ingestio-imdb-delta \
     --member="serviceAccount:$SA" --role=roles/storage.legacyBucketReader
   gcloud iam service-accounts keys create <path outside the repository>/<SA_NAME>.json \
     --iam-account="$SA"
   ```

   An organization policy may forbid creating the key (`iam.disableServiceAccountKeyCreation`).
2. The service account holds `roles/storage.objectUser` and `roles/storage.legacyBucketReader`,
   bound on the bucket only, not on the project. Together they hold the minimum a Delta write on
   this prefix needs:

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
   `GOOGLE_ADC_FILE`.
4. Download the files, start the container and run the job, as in [How to run it](#how-to-run-it).
5. The run's log ends with `└ process finished`, after a `✔ FullLoad.run` line for each table.
6. Check that the commits exist:
   `gcloud storage ls gs://ingestio-imdb-delta/imdb/title_basics/_delta_log/` lists the table's
   first commit, and likewise for `title_ratings`.
7. Some failures have a likely cause:

   | Error | Likely cause |
   |---|---|
   | names the metadata server or Compute Engine | the authentication setting did not apply |
   | `403` | missing privileges |
   | `404` on the bucket | the bucket does not exist |
   | `ClassNotFoundException` for `GoogleHadoopFileSystem` | the connector JAR is not on the classpath |

   A failure that ingestio's documentation does not explain goes to the maintainer, with the
   versions of pyspark, Delta Lake and the connector.

## Running the tests

```bash
uv sync
uv run --locked pytest -q
```

- **No test reaches a real bucket, needs a credential, builds the image or starts compose.** CI
  has no credentials. The tests give the same process a local target, a Delta table under
  `tmp_path`, by an edit of the configuration alone, and read the tables back with Spark.
- The test files are `tests/test_ingest_imdb_datasets.py` and `tests/conftest.py`. They cover the
  shipped configuration files and the `convert` member, the download script against a local HTTP
  server, the Spark session's settings, and the process run on IMDb-shaped files that the tests
  write themselves.
- The tests that need Spark are marked `integration`. They share one session, built by the job's
  own `spark_session.build_session()` with no connector JAR, which exists only in the image. They
  need Java and Maven Central, for Delta Lake's JARs. To run the tests that need neither:

  ```bash
  uv run --locked pytest -q -m "not integration"
  ```

- **Spark 4 needs Java 17 or 21.** On a host whose default JDK is another version, set
  `JAVA_HOME` to a JDK 17 or 21 on the command:

  ```bash
  JAVA_HOME=/usr/lib/jvm/java-17-openjdk uv run --locked pytest
  ```

  The path is an example: use the location of the JDK on the host.
