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
  pointing at one. No key, credential file or token is ever committed, put in `src/config.json`
  or written into code, and `.gitignore` covers the usual names of credential files.
- The bucket and the prefix are configuration, in the `target` section of the job's
  configuration, never hard-coded.
- CI has no credentials: no test of the job reaches a real bucket. The bucket is reached only
  when the job runs in its container.
- The IMDb datasets are licensed for personal and non-commercial use only. The job downloads
  them where it runs and never commits them; its tests write files with the same layout and
  data of their own.

Everything else in those rules holds as written.

## Setup

```bash
uv sync
uv run pre-commit install --hook-type commit-msg
```

How to run the job is documented here once it is implemented.
