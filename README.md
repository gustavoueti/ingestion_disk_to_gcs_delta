# ingestion_disk_to_gcs_delta

Loads the gzip-compressed TSV files of the IMDb non-commercial datasets from a directory on disk, inside a Spark container, into Delta Lake tables in a Google Cloud Storage bucket.

A proof-of-concept job built with [ingestio](https://github.com/gustavoueti/ingestio)
[`0.7.0`](https://pypi.org/project/ingestio/0.7.0/).

## Goal

The job transfers movie data that sits on disk, inside a container where Spark runs, to a
Google Cloud Storage bucket as Delta Lake tables. Its source is the files of the
[IMDb non-commercial datasets](https://developer.imdb.com/non-commercial-datasets/), the
gzip-compressed TSV files such as `title.basics.tsv.gz` and `title.ratings.tsv.gz`, in a
directory of the container. The job reads them with `DiskReader` and writes each one as a Delta
Lake table under a prefix of the bucket with `GcsWriter`, in the `delta` format. Which IMDb
files, the load strategy (full replace or incremental), the layout of the tables in the bucket
and the container setup are decisions of the job, taken with its own agents in the planner's
first spec.

**An exception granted by the maintainer.** The target is a real GCS bucket, not an emulator,
so this job holds one exception to the rule "never credentials" of the rules of the job in
`AGENTS.md`, within these limits:

- The job authenticates only through Application Default Credentials that its environment
  provides, such as an ADC file mounted into the container or `GOOGLE_APPLICATION_CREDENTIALS`
  pointing at one. No key, credential file or token is ever committed, put in a
  `src/config_<table>.json` file or written into code, and `.gitignore` covers the usual names of
  credential files.
- The bucket and the prefix are configuration, in the `target` section of the job's
  configuration, never hard-coded.
- CI has no credentials: no test of the job reaches a real bucket. The bucket is reached only
  when the job runs in its container.
- The IMDb datasets are licensed for personal and non-commercial use only. The job downloads
  them where it runs and never commits them; its tests write files with the same layout and
  data of their own.

Everything else in those rules holds as written.

## What it loads

Two files, `title.basics.tsv.gz` and `title.ratings.tsv.gz`, into two Delta Lake tables,
`gs://ingestio-imdb-delta/imdb/title_basics` and `gs://ingestio-imdb-delta/imdb/title_ratings`.
Each run replaces each table with the latest snapshot of its file and adds the lineage columns
`_run_id` and `_source_modified_at`. The full description is in
[docs/ingest_imdb_datasets.md](docs/ingest_imdb_datasets.md).

## Prerequisites

- Docker with Compose, and uv on the host.
- **Java.** Running the job needs no Java on the host: the container has it. Running the tests
  on the host needs a JDK 17 or 21 (Spark 4) and network access to Maven Central, because the
  first Spark start downloads Delta Lake's JARs.
- The bucket `ingestio-imdb-delta`, created beforehand and not publicly readable, and a service
  account with `roles/storage.objectUser` and `roles/storage.legacyBucketReader` on it, with a
  key file. [The GCP setup](#the-gcp-setup) below gives the commands.
- A `.env` file at the root, ignored by git, that holds paths only, never a key:

  ```text
  GOOGLE_ADC_FILE=<absolute path of the credential file, outside the repository>
  IMDB_DATA_DIR=<only when the data is not in ./data>
  ```

  Compose mounts the file read-only in the container, which authenticates only through
  Application Default Credentials. No credential is committed or copied into the image.
- The IMDb datasets are licensed for personal and non-commercial use only
  (https://data.imdb.com/non-commercial-datasets/). The download script fetches them on the host;
  they are never committed.

### The GCP setup

Once, with `gcloud` signed in to a project you own. Replace the placeholders:

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

The two roles are bound on the bucket only, not on the project. An organization policy may
forbid creating the key (`iam.disableServiceAccountKeyCreation`); then the key must come from
another way that your organization allows.

The key file is the credential file. Keep it outside the repository, and create `.env` at the
root of the repository with `GOOGLE_ADC_FILE` set to the absolute path of that JSON file:

```text
GOOGLE_ADC_FILE=/absolute/path/to/<SA_NAME>.json
```

`.env` holds paths only, never a key, and the key never goes inside the repository.

## Setup

```bash
uv sync
uv run pre-commit install --hook-type commit-msg
```

## Run

```bash
uv run --locked python scripts/download_imdb.py data   # on the host: the two IMDb files
docker compose up -d --build                           # build the image, start the container
docker compose exec job uv run --locked python src/main.py
docker compose down                                    # when done
```

`docker compose up` fails with a message that names `GOOGLE_ADC_FILE` when `.env` does not set
it.

**What success looks like.** The run's log, on standard error, ends with `└ process finished`,
after a `✔ FullLoad.run` line for each table, and the command exits with status 0. To check that
the tables exist in the bucket:

```bash
gcloud storage ls gs://ingestio-imdb-delta/imdb/title_basics/_delta_log/
gcloud storage ls gs://ingestio-imdb-delta/imdb/title_ratings/_delta_log/
```

Each lists the table's commit files. The job exits with a non-zero status when a run fails. The
checks and likely causes for the first run against the bucket are in
[the first run](docs/ingest_imdb_datasets.md#the-first-run-against-the-bucket).

## Tests

```bash
JAVA_HOME=/usr/lib/jvm/java-17-openjdk uv run --locked pytest -q     # all tests
uv run --locked pytest -q -m "not integration"                       # no Java, no network
```

No test reaches a real bucket. Spark 4 needs Java 17 or 21: set `JAVA_HOME` to a JDK 17 or 21
on the command when the host's default JDK is another version, and use the location of the JDK
on your host (the path above is an example). The full run takes a couple of minutes, mostly
Spark starting. See [Running the tests](docs/ingest_imdb_datasets.md#running-the-tests).

## Documentation

[docs/index.md](docs/index.md) maps every page and spec.
