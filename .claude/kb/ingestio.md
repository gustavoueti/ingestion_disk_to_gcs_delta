# ingestio for ingestion_disk_to_gcs_delta

- **ingestio version:** `0.7.0`
- **Package:** https://pypi.org/project/ingestio/0.7.0/
- **Generated:** 2026-11-18, by the ingestio-expert of the ingestio_examples hub, from
  the ingestio repository at tag `v0.7.0`
- **Regenerate it from the hub when the job moves to another version of ingestio.** It
  describes that version only, and nobody edits it by hand. If `pyproject.toml` pins another
  version, stop: this guidance is stale.

The agents of this job work from this file alone: they do not read the ingestio repository or
the hub. When this file and the library disagree, the library at the tag above wins, and the
disagreement is a defect of this file, to report to the maintainer.

How to read it: a path in parentheses, such as `(docs/concepts/callbacks.md)`, names the file of
the library, at tag `v0.7.0`, that a statement comes from. A statement marked *(Spark)*,
*(Delta Lake)*, *(Cloud Storage connector)*, *(Google Cloud)*, *(IMDb)* or *(PyPI)* describes
that tool or source, not ingestio: ingestio only passes values through to it, or leaves it to the
environment. Such statements were checked when this file was generated, against the versions
they name; the spec confirms them for the versions the job locks. Code blocks are
illustrations, never files of the job: the job's files are written by its own agents, from its
specs.

## 1. Philosophy and concepts

ingestio describes any data movement process as an ordered sequence of steps. It does not
assume that the data is a table, that there are bronze and silver layers, or that processing
is batch: those are things a job builds with it (README.md). It is an early alpha: the API is
not stable yet (README.md).

**Two concepts.** Everything in ingestio, and everything a job adds to it, is either a
*connector* or a *callback*. Something that fits neither is not a third concept: stop and
report it (docs/philosophy.md).

**Vocabulary** (docs/philosophy.md):

- **Stage**: one callback, the unit of work of a pipeline.
- **Process** (`Process`): the pipeline, an ordered list of steps run together.
- **Step** (`Step`): a named point of the process that holds the callbacks running there.

**Principles** (docs/philosophy.md):

- **Simplicity.** The smallest abstraction that solves the problem: no internal frameworks,
  no open registries, no magic. The one mapping from names to classes is the closed table of
  the connectors the library ships, which its read and write callbacks use (ADR 0015).
- **The user's autonomy.** The library composes; it does not impose. A job writes its own
  callbacks in its own code, with nothing to register. Users do not extend connectors: a
  system the library does not integrate gets a connector contributed to the library.
- **A core without dependencies.** `ingestio` itself has no dependency; an integration that
  needs a third-party package is an optional extra, such as `ingestio[postgres]` (ADR 0002).
  An engine that the runtime provides, pyspark among them, is a documented prerequisite and
  never part of an extra; so is Delta Lake (docs/concepts/connectors.md, section Extras).
- **Streaming is a future direction**, not implemented (ADR 0012).

**What exists at 0.7.0, and what is only a direction:**

| The philosophy states | At 0.7.0 |
|---|---|
| A callback is essential or non-essential, as its author declares | Not implemented: every callback is essential, and none can be declared otherwise (docs/concepts/criticality.md, ADR 0011) |
| A non-essential failure is recorded in the result of the run | There is no run result: `Process.run()` returns the process itself (docs/concepts/criticality.md) |
| Append-only and SCD2 are the library's default callbacks | Shipped as `AppendOnly` and `SCD2`, beside `ReadSource` and `FullLoad` (docs/concepts/callbacks.md) |
| The library ships every connector | Eleven systems: `json_api`, `json_lines`, `disk`, `postgres`, `mongodb`, `mysql`, `s3`, `gcs`, `volume`, `unity_catalog`, `bigquery` (docs/concepts/configuration.md) |
| Streaming | Deferred: every reader returns one batch per `read()`, every writer needs the whole batch (ADR 0012) |
| Extras "such as `ingestio[spark]`" | There is no `spark` extra. The extras are `postgres`, `mysql`, `s3`, `gcs`, `databricks` and `bigquery`, and ingestio never installs pyspark or Delta Lake (docs/concepts/connectors.md, section Extras; pyproject.toml) |
| A connector decides no format | `JsonLinesWriter` fixes its format, JSON Lines: the one known divergence (docs/concepts/connectors.md, section Known divergence) |

### Connectors

- A connector is an integration with an external system. It knows how to talk to that system
  and nothing else: it decides no write strategy, no format and no architecture
  (docs/philosophy.md, docs/concepts/connectors.md).
- Its capabilities are two protocols, `Reader[T]` with `read() -> T` and `Writer[T]` with
  `write(data) -> None`. They are structural, with no base class, and not checked at runtime:
  the type checker checks them. The payload type is open: a list, a generator, a DataFrame
  (ADR 0003, docs/concepts/connectors.md).
- **The library ships every connector.** A job names its source and its target systems in its
  JSON configuration file, and the library's read and write callbacks build the connector that
  each `system` names, from a closed table that nothing outside the library can add to or
  replace (ADR 0015, docs/concepts/configuration.md).
- A connector takes an ingestion's parameters as constructor arguments named after the
  configuration keys. It never reads a file, an environment variable or a secret store, and
  has no `from_config`. Credentials, SDK clients and per-run values, such as the watermark,
  reach it through the arguments of the callback (ADR 0013, ADR 0015). An SDK's default client,
  created when the job passes none, follows that SDK's own lookup of credentials, such as
  Application Default Credentials, which the connector neither reads nor configures (ADR 0013).
- A reader filters inside the source: `DiskReader` selects the files above the watermark
  before Spark loads anything, so Spark never opens a file that was not selected (ADR 0013).
- Every shipped writer has `write(data, *, replace=False)`: the callback that calls it chooses
  between appending and replacing (ADR 0016).
- A writer may create a missing target only in the form its system derives entirely from the
  data, with no layout, location, property or type mapping chosen by the connector, and never
  creates what holds the target (ADR 0014). For this job: `GcsWriter` with the `delta` format
  writes a Delta table at the path, creating it when the path holds none, and creates no bucket
  (docs/concepts/connectors.md).
- Every shipped connector except `JsonApiReader` and `JsonLinesWriter` is built on Spark: it
  reads into, or writes from, a `pyspark.sql.DataFrame`, expects a Spark session to be running
  already, and never starts one (README.md, docs/concepts/connectors.md).
- The modules import third-party packages only inside the functions that use them, so
  `ingestio` and `ingestio.connector.all` import without pyspark and without any extra
  (docs/concepts/connectors.md, section Extras).

### Callbacks

- A callback is a stage of the pipeline that performs an action: formatting, medallion layers,
  CDC, a write strategy (docs/philosophy.md).
- It subclasses `ingestio.Callback` and overrides any of three moments, `before()`, `run()`
  and `after()`, which do nothing by default (docs/concepts/callbacks.md, ADR 0006).
- Callbacks exchange data through `self.process.state`, a dictionary shared by the whole
  process, instead of passing a payload from one to the next. Its keys are conventions
  between callbacks: nothing checks before the run that a key another callback needs will be
  there (ADR 0004).
- The library ships four callbacks, `ReadSource`, `AppendOnly`, `FullLoad` and `SCD2`. Each
  write strategy is one callback (ADR 0016). They read the job's configuration file when they
  are built and build their connector from it; they never accept a connector instance
  (ADR 0015, docs/concepts/callbacks.md).
- A process uses a shipped callback by placing it in a step; the library never adds one on
  its own (docs/philosophy.md).
- A callback stops part of the run by raising `CancelStepException` or
  `CancelProcessException` (ADR 0007, docs/concepts/callbacks.md).

### Criticality

- **The direction** (docs/philosophy.md, ADR 0011): a stage is essential or non-essential. An
  essential failure fails the pipeline; a non-essential one lets it continue, and is recorded
  in the result of the run, never silent. The author of a callback declares its criticality,
  and without a declaration a callback is essential. Append-only and SCD2 are essential.
- **At 0.7.0** (docs/concepts/criticality.md): not implemented. Every exception other than the
  two cancel exceptions propagates out of `Process.run()`, after the `after` pass of the
  current step, and stops the run. Every callback therefore behaves as essential, there is no
  way to declare one non-essential, and there is no run result to record a tolerated failure.
- The shipped callbacks are essential: every error of `read()` and `write()` propagates out of
  `Process.run()` unchanged (docs/concepts/configuration.md).
- Rule 3 of section 5 says what this means for the job.

### Process and Step

`Process` and `Step` are the engine that composes the two concepts. They are not a third
concept, and nobody extends them (docs/philosophy.md).

- **Building** (docs/concepts/lifecycle.md): `Process(steps, *, state=None, run_id=None,
  log=None)`. `state` is copied into `process.state`; `run_id` is eight hexadecimal
  characters when omitted; `log` is the logger to write to, `ingestio.core` by default. A
  process with no step raises `ValueError("a process needs at least one step")`, and two steps
  with one name raise `ValueError("duplicate step name: '<name>'")` (src/ingestio/core.py).
  `Step(name, callbacks=())` raises `ValueError("step name must be a non-empty string")` for a
  blank name; a step may hold no callback. Building the process binds every callback to it:
  from then on `callback.process` is the process.
- **Running**: `process.run()` runs the steps in list order and returns the process, so the
  state stays reachable: `Process([...]).run().state`. Inside a step the callbacks run in
  three passes, each in list order: every `before`, then every `run`, then every `after`.
  There is no priority and no reordering (ADR 0005, ADR 0006). `process.started_at` holds the
  UTC time the run started.
- **Cancellation and failure** (docs/concepts/lifecycle.md):

  | Raised by a callback | Effect |
  |---|---|
  | `CancelStepException` | the rest of the step is skipped, `run` pass included; the process carries on with the next step |
  | `CancelProcessException` | the process stops cleanly and `process.cancelled` becomes `True` |
  | any other exception | it propagates out of `run()` and the run fails |

  The `after` pass of the current step runs in every case. A `CancelStepException` raised from
  an `after` is not handled and fails the run (ADR 0007).
- **Logging** (docs/concepts/lifecycle.md, ADR 0008): through the standard `logging` module,
  never `print`. A run logs a tree, one record per line, each line starting with the run id:

  ```text
  [a3f7c9] ┌ process started · 2 steps
  [a3f7c9] │ ┌ extract (1/2) · 1 callback
  [a3f7c9] │ │ ✔ ReadSource.run 412ms
  [a3f7c9] │ └ extract done in 413ms
  [a3f7c9] │ ┌ load (2/2) · 2 callbacks
  [a3f7c9] │ │ ⊘ SkipEmptyBatch.run cancelled the step after 1ms: no new file
  [a3f7c9] │ └ load cancelled in 2ms
  [a3f7c9] └ process finished in 432ms
  ```

  Starts, successes and cancellations are `INFO`; a failing moment (`✖ <callback>.<moment>
  failed after <d>: <Type>: <message>`) and the failed closing lines are `ERROR`. A failure
  line holds the type and message of the exception, never a traceback; the exception itself
  leaves `run()` unchanged. A callback that implements no moment gets no line. `ingestio`
  registers only a `NullHandler`, so nothing shows until the job configures logging, for
  instance with `log_to_console()`.
- **Behaviour not specified yet** (docs/concepts/lifecycle.md), which the job must not rely on:
  an exception in an `after` stops the remaining `after` calls of the step and replaces any
  exception already propagating; when a `before` fails, every `after` of the step still runs;
  calling `run()` again reuses the state of the previous run; a callback placed in two
  processes stays bound to the one built last.

## 2. Extending the library

At 0.7.0 a job extends ingestio in one way: **callbacks of its own**. It uses everything else
as the library ships it.

**A callback of the job's own** (docs/philosophy.md, docs/concepts/callbacks.md). Subclass
`Callback` in the job's code, override the moments it needs, and read and write
`self.process.state`. Its constructor arguments are the job's to choose. Nothing is
registered and nothing is configured. Its `name`, the class name by default, is what the log
shows. For instance, a stage that ends the load step when the source selected nothing:

```python
from __future__ import annotations

from ingestio import Callback, CancelStepException


class SkipEmptyBatch(Callback):
    """Ends the step when the batch has no row, so that nothing after it in the step runs."""

    def __init__(self, *, key: str = "records") -> None:
        self.key = key

    def run(self) -> None:
        if self.process.state[self.key].isEmpty():  # a Spark DataFrame: this runs a Spark job
            raise CancelStepException("no new file")
```

**A shipped connector inside a callback of the job's own** (docs/concepts/connectors.md,
section Using a connector). For a strategy that the shipped callbacks do not offer, a job's
own callback may build a connector of `ingestio.connector.all` itself and call it, annotating
what it holds with `Reader` or `Writer`. That callback then chooses the strategy itself, with
`write(data)` or `write(data, replace=True)`, and receives its parameters from the job: it
must never hand a source's `path` to a writer (ADR 0013). This job's goal does not need it:
`ReadSource`, `FullLoad` and `AppendOnly` cover reading the files and writing the tables.

```python
from typing import Any

from ingestio import Callback, Writer


class WriteWith(Callback):
    """Writes a value of the state with the writer it was given."""

    def __init__(self, writer: Writer[Any], *, key: str = "records") -> None:
        self.writer = writer
        self.key = key

    def run(self) -> None:
        self.writer.write(self.process.state[self.key])
```

**Controlling the flow** (ADR 0007). `CancelStepException` skips the rest of the step,
`CancelProcessException` stops the process cleanly; the policy is the job's.

**Logging** (docs/concepts/lifecycle.md). The job's own modules log with
`logging.getLogger(__name__)`. `Process(..., log=<logger>)` sends the tree to another logger,
and `log_to_console()` shows it on standard error.

**What a job cannot extend at 0.7.0:**

- **Connectors.** A job cannot add a connector, or name one of its own in the configuration:
  the table of systems is closed, private and not exported, with no mapping argument, no
  import path in the file and no entry point (docs/concepts/configuration.md, ADR 0015). A
  system or a capability the library lacks, such as a reader of a Delta table at a `gs://`
  path, is a library gap. Rule 2 of section 5 speaks of custom connectors "through the
  documented extension": for connectors, none exists at this version.
- **`Process` and `Step`**, which are the engine, not an extension point (docs/philosophy.md).
- **Criticality**, which cannot be declared (docs/concepts/criticality.md).
- **The shipped callbacks and connectors**, whose behaviour comes only from their arguments
  and the keys of the configuration. Their private attributes and modules are not extension
  points (section 5, rules 1 and 5).
- **The reserved names of the configuration**, `source`, `target` and `scd2`
  (docs/concepts/configuration.md).

## 3. The structure of a job

ingestio's minimum job is two files: `main.py`, the entry point, and `config.json`, the job's
configuration. In this job both live in `src/`, with every other file of the job's code, and
the job's own callbacks are written in `main.py` itself, never in a module of their own.
Neither file exists until the implementer writes it; this section says what each one is
responsible for and how they connect to the library.

### `config.json`

**Its responsibility.** It names the job's source and target systems with their parameters,
and holds the job's own parameters. It is the only way the library's callbacks reach a
connector: changing the source or the target is an edit of this file (ADR 0015,
docs/concepts/configuration.md).

**Its shape** (docs/concepts/configuration.md):

- The top level is a JSON object. The names `source`, `target` and `scd2` are the library's:
  `ReadSource` reads `source`, `AppendOnly`, `FullLoad` and `SCD2` read `target`, and only
  `SCD2` reads `scd2`. Every other top-level member belongs to the job: every library callback
  ignores it, and the job's own code may read it.
- A section names its system with the key `system`. Every other key of the section is the
  constructor argument of the same name of the connector that the system builds, passed as it
  is, with nothing renamed or converted. A key that the system does not take is an error.
- Each section names its own location: the `target` section's `path` is the target's.
- **One source and one target per file.** "A job with two sources uses two files"
  (docs/concepts/configuration.md, section Known limitations). Each IMDb file this job loads is
  one source and one Delta table one target, so **each table has a configuration file of its
  own**. `CONTRIBUTING.md` names the job's configuration `src/config.json`; with more than one
  table, the spec names the files, all in `src/` next to `main.py`. Generating them from code
  at run time is not a way around the rule (section 5, rule 5).

For this job, `source` names the `disk` system and `target` the `gcs` system. The shape of one
table's file, with the values the spec chooses:

```json
{
  "source": {
    "system": "disk",
    "path": "<absolute directory, inside the container, that holds this one IMDb file>",
    "format": "csv",
    "schema": "<the file's columns, as a DDL string>",
    "options": {"sep": "\t", "header": true, "nullValue": "\\N"}
  },
  "target": {"system": "gcs", "path": "gs://<bucket>/<prefix>/<table>", "format": "delta"}
}
```

`watermark_pattern`, `schema` and `options` are optional on the source, `options` on the
target; section 4 says what each one does, and which TSV options the IMDb files need.

**What never goes in it** (docs/concepts/configuration.md):

- Credentials of any kind. The `gcs` target takes no credential key and no callback argument
  at all: Spark writes with the credentials of its own session, which come from the
  environment (docs/concepts/connectors.md, section GCS). A key that the system does not take
  is rejected; and an authentication setting inside `options` is a credential in the file,
  which this job never writes (section 4, pitfall 11).
- Per-run values: the watermark is the `watermark_value` argument of `ReadSource`.
- SDK clients and Spark sessions, which are not JSON. Neither are Spark's or Hadoop's session
  settings, such as the Cloud Storage connector's or Delta Lake's: they belong to the session
  the job builds.
- A write strategy: `append`, `replace` and `mode` are not keys. The callback the job places
  chooses (ADR 0016). Inside `options`, `mode` is Spark's CSV parse mode on the source, not a
  write mode.

**Its JSON types** (docs/concepts/configuration.md, section JSON types). `system`, `path`,
`format`, `watermark_pattern` and `schema` are strings; `options` is an object whose values
are strings, numbers or booleans. `null` is never a value: a key that the job does not set is
left out. `NaN`, `Infinity` and `-Infinity` are rejected. A key that appears twice in an
object keeps its last value, silently. In JSON, a tab is `"\t"` and IMDb's null marker `\N` is
`"\\N"`.

**When it is read.** Each callback reads the file once, when it is built, and builds its
connector then; later changes to the file are not seen. `ReadSource` and `FullLoad` built
from the same path read it twice. Building a callback touches no Spark session, no client, no
bucket and no network (docs/concepts/configuration.md). The `config` argument is a `str` or an
`os.PathLike`; a relative path is resolved against the working directory when the callback is
built, so `Path(__file__).with_name("config.json")` names a file in `src/` wherever the job
is run from (README.md).

**Its errors** (docs/concepts/configuration.md, section Errors). A mistake anywhere in the
file raises when the callback is built, before `Process` exists and before anything runs, and
writes no line of the run's log:

- the callback's own checks raise `ValueError` with a message that starts with the file's
  path, such as `<path>: source.watermark_patern is not a key of system 'disk'`,
  `<path>: target.format is required for system 'gcs'` or
  `<path>: target system 'gcs' takes no argument client`;
- a value that the connector rejects raises the connector's own `ValueError`, unchanged, with
  one note, `while building the target of <path>, system 'gcs'`: for instance
  `path must be a gs:// URI`;
- an unknown `system` raises `<path>: target.system '<name>' is not one of: bigquery, disk,
  gcs, json_lines, mysql, postgres, s3, unity_catalog, volume`.

The checks run in a fixed order: the argument and the file, the JSON, the section, `system`,
the keys, the required keys, the types, the callback arguments, and last the connector.

### `main.py`

**Its responsibility.** The entry point: it builds what the configuration cannot hold,
defines the job's own callbacks, places the callbacks in steps, runs the process and decides
what the log shows (README.md, section Structuring a job).

**What belongs in it, for this job:**

- **The Spark session**, created before the process runs, with Delta Lake and the Cloud
  Storage connector for Hadoop in it and the connector set to Application Default Credentials.
  No connector creates one, and Spark's access to GCS "is the environment's concern"
  (README.md, docs/concepts/connectors.md); section 4 says how. Helpers it needs, such as the
  version constants of the packages, may live in a module next to `main.py`.
- **The last watermark already ingested**, when the spec's strategy has one, computed before
  `ReadSource` is built, because `watermark_value` is an argument of the callback and a value
  that a step of the same process computes cannot reach the reader
  (docs/concepts/configuration.md, section Known limitations).
- **The job's own callbacks**, defined in `main.py` itself.
- **The process**: its steps, their order, and the callbacks of each, one pair of steps per
  table when there are several.
- **Logging**: `ingestio.log_to_console()` for the tree, and the job's own handlers.
- **No credential, ever.** Nothing in `main.py` reads, builds or names a key, a token or a
  credential file: the session's connector finds Application Default Credentials in the
  environment itself (`AGENTS.md`, section Goal).

**The guard.** `main.py` runs only under `if __name__ == "__main__":`, so tests import its
pieces without running it (CONTRIBUTING.md of this job). For this job that means more than not
calling `run()` at import: building the callbacks reads the configuration files, and building
the session starts a JVM and downloads packages. The job keeps all of it in functions called
under the guard, such as one that builds the process from the paths of its configuration files,
so that a test runs the same process on configurations of its own.

```python
from pathlib import Path

from ingestio import FullLoad, Process, ReadSource, Step

# SkipEmptyBatch is the job's own callback of section 2, defined in main.py.


def build_process(configs: dict[str, Path]) -> Process:
    """The job's process: one extract step and one load step per table."""
    steps: list[Step] = []
    for table, config in configs.items():
        steps.append(Step(f"extract_{table}", [ReadSource(config, into=table)]))
        steps.append(
            Step(f"load_{table}", [SkipEmptyBatch(key=table), FullLoad(config, key=table)])
        )
    return Process(steps)
```

### How they connect to the library

1. `ReadSource(config, ...)` reads the `source` section and, through the closed table, builds
   `DiskReader(path, format, watermark_pattern=..., schema=..., options=...,
   watermark_value=..., spark=...)` when it is built (docs/concepts/configuration.md).
2. `FullLoad(config)`, or `AppendOnly(config)`, reads the `target` section and builds
   `GcsWriter(path, format, options=...)` when it is built. That validates the `gs://` path and
   nothing else: no Spark, no bucket, no credentials (docs/concepts/connectors.md).
3. When the process runs, `ReadSource.run()` calls `read()` and stores the DataFrame it
   returns in `state["records"]`, or in the key given as `into` (docs/concepts/callbacks.md).
4. The job's own callbacks between the two read that key and may replace it with a new
   DataFrame.
5. `FullLoad.run()` calls `write(state[key], replace=True)` and `AppendOnly.run()` calls
   `write(state[key])`. The writer runs
   `data.write.format("delta").options(<options>).mode("overwrite" | "append").save(<path>)`
   through the DataFrame's own session (docs/concepts/connectors.md).

**What `main.py` passes as arguments** (docs/concepts/configuration.md, section What does not
belong in the file):

| Callback | Argument | For this job |
|---|---|---|
| `ReadSource` | `watermark_value` | the last watermark already ingested: a `str` with `watermark_pattern`, a timezone-aware `datetime` without it. `None`, or nothing, reads every file |
| `ReadSource` | `spark` | optional: the session to read with. The active session, looked up when `read()` runs, otherwise |
| `ReadSource` | `into` | the key of the state that receives the DataFrame, `"records"` by default |
| `FullLoad`, `AppendOnly` | `key` | the key of the state to write, `"records"` by default |

An argument that is `None` is not given. An argument that the system does not take raises
when the callback is built: `ReadSource` takes no `connection` or `client` for `disk`, and the
write callbacks take neither for `gcs`. The write callbacks take no `spark`, `replace` or
`watermark_value`: the writer saves the DataFrame through the DataFrame's own session, so the
session that read the files is the one that writes to GCS.

**How the data flows** (docs/concepts/callbacks.md, docs/concepts/connectors.md). The state
holds the very DataFrame the reader returned: the write callbacks never copy, convert, count or
check it, and leave the state as they found it. A missing key raises `KeyError(key)` when the
process runs, and the writer is not called. The DataFrame is lazy: the gzip files are read when
an action runs on it, such as `isEmpty()` or the write, and every action reads the selected
files again.

### Testing a job

- **Through the public API.** A test imports the job's functions from `main.py` and its
  modules, builds a `Process`, runs it, and checks what is observable: the rows of the table
  written, `process.state`, `process.cancelled`, the log and the errors.
- **An initial state.** `Process(steps, state={...}, run_id="<id>")` runs a step on a value
  the test built, such as the load step alone on a DataFrame the test created; a fixed run id
  makes the log lines predictable (docs/concepts/callbacks.md, docs/concepts/lifecycle.md).
- **The configuration.** Tests write each configuration with `json.dumps` to a file under
  `tmp_path` and pass its path, as the library's own tests do
  (specs/config_connectors.md, section Notes for testing). Its source `path` is the absolute
  directory, under `tmp_path`, where the test wrote its files.
- **No test reaches GCS** (`AGENTS.md`, section Goal): CI has no credentials. The library moves
  a target by an edit of the JSON, with the same process (ADR 0015), so a test that runs the
  job's process gives it a target that stays local, such as
  `{"system": "disk", "path": "<tmp_path>/<table>", "format": "delta"}`: `DiskWriter` runs the
  same call chain as `GcsWriter`, and with `delta` writes a Delta table at the path
  (docs/concepts/connectors.md, section Disk). What such tests do not cover, the transport to
  `gs://` and the credentials, is exercised when the job runs in its container.
- **The `gcs` target without Spark.** Building `FullLoad` or `AppendOnly` from the job's real
  configuration file checks its `target` section and its `gs://` path, and touches no Spark,
  no bucket and no credentials (docs/concepts/configuration.md): a test may do that to cover the
  configuration the job ships.
- **A local source.** Files with the IMDb layout, written by the test under `tmp_path`:
  tab-separated, a header line, `\N` for null, gzip-compressed with Python's `gzip` module, each
  in its own directory. Their rows are the test's own, never real IMDb data. Modification times
  are set by the test with `os.utime(file, ns=(n, n))` when the watermark is the last-modified
  time (specs/disk_connectors.md, section Notes for testing).
- **Spark.** One local session for the whole run, built as the library's own integration tests
  build theirs: `SparkSession.builder.master("local[2]")`, `spark.sql.session.timeZone` set to
  `UTC` and `spark.ui.enabled` set to `false` (tests/test_disk_connectors.py), with Delta Lake
  loaded through `spark.jars.packages`, `spark.sql.extensions` set to
  `io.delta.sql.DeltaSparkSessionExtension` and `spark.sql.catalog.spark_catalog` set to
  `org.apache.spark.sql.delta.catalog.DeltaCatalog` (tests/test_unity_catalog_connectors.py).
  "A JVM loads its packages once, when it starts" (tests/test_unity_catalog_connectors.py): the
  session that carries Delta Lake must be the first one of the test run. A test may build it
  with the job's own session function, so that the session the job runs with is the one tested.
- **Reading the table back.** No reader of ingestio reads a Delta table at a path: `DiskReader`
  rejects `delta` (docs/concepts/connectors.md). The tests read what the job wrote with Spark
  itself, `spark.read.format("delta").load(<path>)`, and its history with
  ``DESCRIBE HISTORY delta.`<path>` `` *(Delta Lake)*; that is a check of the output, not a part
  of the job.
- **The log.** Each line of the tree is one record whose message is the plain line, such as
  `[run0001] │ │ ✔ FullLoad.run 15ms` (docs/concepts/lifecycle.md). `caplog` sees the
  `INFO` lines only after `caplog.set_level(logging.INFO, logger="ingestio")`: the library's
  logger has only a `NullHandler`, and its level otherwise comes from the root logger.
- **The errors.** `pytest.raises(ValueError, match=...)` around the building of a callback
  catches the configuration's errors; the errors of `read()` and `write()` leave
  `Process.run()` unchanged.
- **What the library offers for tests:** nothing beyond its public API. There are no fakes,
  fixtures, test sessions or test buckets, no run result to inspect, and no way to see which
  files the reader selected.

## 4. What this job uses

The goal: the gzip-compressed TSV files of the IMDb non-commercial datasets, in a directory of
a container where Spark runs, read with `DiskReader` and written as Delta Lake tables under a
prefix of a real GCS bucket with `GcsWriter`, with the credentials of the environment.

### Callbacks

**`ReadSource`** (docs/concepts/callbacks.md, docs/concepts/configuration.md)

```text
ReadSource(config, *, into="records", connection=None, client=None, watermark_value=None, spark=None)
```

- Built: reads the `source` section, checks it, and builds a `DiskReader` from it, which
  validates its arguments. No Spark, no file system access.
- Run: calls `read()` once and stores the DataFrame in `state[into]`. The listing, the
  selection of the files and the errors of the file system happen here (see `disk` below).
- For `disk` it takes `watermark_value` and `spark`, both optional, and rejects `connection`
  and `client`.
- `watermark_value` is annotated `int | Decimal | str | date | None`, wider than what `disk`
  accepts: a `str` with `watermark_pattern`, a timezone-aware `datetime` without it. mypy does
  not catch a wrong one; the reader raises its `ValueError` when the callback is built
  (src/ingestio/callback/io.py, docs/concepts/connectors.md).
- Essential: every error propagates unchanged.

**`FullLoad`** (docs/concepts/callbacks.md, docs/concepts/connectors.md)

```text
FullLoad(config, *, key="records", connection=None, client=None)
```

- Built: reads the `target` section and builds a `GcsWriter`. The `gcs` target takes no
  `connection` and no `client`; giving one raises. No extra is needed.
- Run: calls `write(state[key], replace=True)` once. With `delta`, the overwrite is one commit
  and a new version of the table: the table holds exactly the batch, and a reader through Delta
  sees the table before the commit or after it (docs/concepts/connectors.md, section GCS).
- **It writes whatever the batch holds.** An empty DataFrame with columns makes a version with
  no row: the table is emptied. A DataFrame with no column fails in Spark
  (docs/concepts/callbacks.md, pitfall 1).
- Idempotent for a given batch: running the same snapshot twice leaves the same rows, in one
  more version of the table.
- Essential: every error of `write()` propagates unchanged.

**`AppendOnly`** (docs/concepts/callbacks.md)

```text
AppendOnly(config, *, key="records", connection=None, client=None)
```

- As `FullLoad`, but calls `write(state[key])`: with `delta`, one commit that adds the rows and
  leaves the existing ones as they are.
- **An append is not idempotent:** the same files appended twice are in the table twice
  (docs/concepts/callbacks.md).

**Not available for this target.** `SCD2` supports only `unity_catalog`, `postgres`, `mysql`
and `bigquery`; building it on a `gcs` target raises `<path>: target.system 'gcs' is not one
of: bigquery, mysql, postgres, unity_catalog` (docs/concepts/callbacks.md). No merge or upsert
callback ships at 0.7.0: ADR 0016 says a merge would be one more callback of the library, and
none exists (ADR 0016, docs/backlog.md). Pitfall 8 says what that means for an incremental
strategy.

**The job's own callbacks this goal may need.** The library leaves these to the job, and the
spec decides which exist:

- a stage that ends the load step when the batch is empty, before `FullLoad` in the same step,
  so that a run with nothing new does not empty the table (pitfall 1);
- a stage that adds lineage columns, such as the snapshot the rows come from, taken from the
  file's `_metadata.file_modification_time`, or the run id (pitfalls 2 and 7);
- a stage that converts columns that Spark's CSV parser cannot type directly, such as IMDb's
  `0`/`1` booleans or its comma-separated lists (pitfall 5).

None of them reads or writes files, the bucket or the table itself: reading is `ReadSource`'s
and writing is the write callback's (section 5, rule 5). Each of them is essential, as every
callback is at 0.7.0.

### Connectors and systems

#### Source: `"system": "disk"`, built as `DiskReader`

```text
from ingestio.connector.all import DiskReader      # not exported by ingestio itself

DiskReader(
    path: str | PathLike[str], format: str,
    *, watermark_pattern: str | None = None,
    watermark_value: str | datetime | None = None,
    schema: StructType | str | None = None,
    options: Mapping[str, str | int | float | bool] | None = None,
    spark: SparkSession | None = None,
)
DiskReader.read() -> DataFrame
```

It reads the files of one directory through Spark's file data sources and returns a
`pyspark.sql.DataFrame`. It needs no extra (docs/concepts/connectors.md, section Disk).

**Its keys:**

| Key | Required | What it is |
|---|---|---|
| `path` | yes | the directory that holds the files: an absolute local path. A relative path, an empty one or a URI (`file:/`, `gs://`) raises `ValueError("path must be an absolute local path")`. A trailing slash is dropped |
| `format` | yes | the name of the Spark data source, here `csv`, passed to Spark as it is. `delta` is rejected; an unknown name fails in Spark when the reader reads |
| `watermark_pattern` | no | a regular expression that the whole name of a file matches, with exactly one group that captures the file's watermark. Without it, a file's watermark is its last-modified time |
| `schema` | no | the schema of the files, as a DDL string such as `"tconst STRING, averageRating DOUBLE, numVotes INT"`. Spark infers it when it is left out. A `StructType` cannot be given through the configuration |
| `options` | no | Spark data source options, passed through as they are, with no default of the connector |

The connector never chooses a format, never infers one from file extensions, and transforms
nothing: it adds no column, casts nothing and filters no rows. Spark's CSV data source does the
conversion (docs/concepts/connectors.md).

**How a read selects the files.** Each `read()` goes through these steps, and the selection is
made before Spark loads anything (docs/concepts/connectors.md):

1. **Session:** `spark` when given, otherwise `SparkSession.active()`, looked up at this moment.
   With no active session, PySpark's error propagates.
2. **Listing:** the entries directly inside `path`, once, on the driver, with the standard
   library. A missing `path` raises `FileNotFoundError`, a file raises `NotADirectoryError`,
   and `PermissionError` propagates.
3. **Candidates:** regular files, or links to one, whose name starts with neither `_` nor `.`
   and does not end with `._COPYING_`. Subdirectories are ignored, with everything in them.
4. **The watermark of each candidate**, as below.
5. **Selection:** the candidates whose watermark is strictly greater than `watermark_value`, or
   all of them when it is `None`.
6. **Load:** one load of the selection, with `format`, `schema` and `options` as given. Spark
   receives the list of selected paths, sorted by file name, never the directory.

**The watermark of a file** (docs/concepts/connectors.md):

- *With `watermark_pattern`:* the pattern is matched against the file name with
  `re.fullmatch` (Python's dialect), and the watermark is the text its group captured. A file
  whose name does not match is not read. Watermarks compare as text, code point by code point,
  and are never converted. `watermark_value` must then be a `str`.
- *Without it:* the watermark is the file's last-modified time, following links, truncated to
  whole milliseconds and compared with `watermark_value` as an instant. `watermark_value` must
  then be a timezone-aware `datetime`.

**The selection is frozen; the contents are not.** Every action on the DataFrame reads the
selected files and no other, so files that appear after `read()` wait for the next run. A
selected file that changes before an action is read as it is then; one that is deleted makes
the action fail, unless the job passes Spark's `ignoreMissingFiles` option
(docs/concepts/connectors.md, section Limitations of the disk connectors).

**The selection is not exposed.** The reader keeps no record of it. A job that advances its
watermark takes it from the DataFrame, through Spark's hidden `_metadata` column, whose fields
`file_name`, `file_path` and `file_modification_time` describe the file of each row
(docs/concepts/connectors.md). `file_modification_time` has millisecond precision, the same as
the reader's, so a job that takes its next watermark from it never selects its last file
again.

**The empty selection.** When nothing is selected, `read()` makes no load and returns an empty
DataFrame: with the columns of `schema` when it is given, with no column at all otherwise. It
is not backed by files, so it has no `_metadata` column (docs/concepts/connectors.md).

**Its errors when it is built** (docs/concepts/connectors.md, section Building the disk
connectors): `path must be an absolute local path`; `format must not be empty`;
`format '<format>' cannot be read file by file` for `delta`; `option '<key>' is set by the
connector` for `path`, `paths`, `pathGlobFilter`, `recursiveFileLookup`, `modifiedBefore` and
`modifiedAfter`, in any case; `invalid watermark_pattern: <error>`;
`watermark_pattern must have exactly one group` (a named group counts, `(?:...)` does not);
`watermark_value must be a str with watermark_pattern`;
`watermark_value must be a datetime without watermark_pattern`;
`watermark_value must be timezone-aware`. The connector catches nothing and writes no log line.

**The IMDb files** *(IMDb)*. Each dataset is one gzip-compressed, tab-separated file in UTF-8,
whose first line holds the column names, and `\N` marks a missing value. The files are
`title.akas`, `title.basics`, `title.crew`, `title.episode`, `title.principals`,
`title.ratings` and `name.basics`, each `<name>.tsv.gz`, downloaded from
https://datasets.imdbws.com/ and refreshed daily under the same names. Their use is licensed
for personal and non-commercial purposes only
(https://developer.imdb.com/non-commercial-datasets/).

**Reading them with the `csv` format** *(Spark)*. The `options` object reaches Spark's CSV
data source as it is. For these files:

- `sep`: a tab, `"\t"` in JSON. `header`: `true`. `nullValue`: `\N`, `"\\N"` in JSON.
  `encoding` is UTF-8 by default.
- `.gz` files are decompressed by Spark from their extension; a gzip file cannot be split, so
  each file is read whole by one task (pitfall 4).
- `quote`: IMDb quotes no value, while Spark's CSV source treats `"` as a quote by default, so
  a value that starts with `"` can run into the following fields. Spark documents an empty
  string as turning quoting off for reading (pitfall 5).
- `mode`: Spark's parse mode. `PERMISSIVE`, the default, turns what it cannot parse into nulls
  silently; `FAILFAST` fails the read; `DROPMALFORMED` drops the rows.
- A JSON boolean such as `true` reaches Spark as `"true"` (specs/disk_connectors.md, section
  Notes for testing).

#### Target: `"system": "gcs"`, built as `GcsWriter`

```text
from ingestio.connector.all import GcsWriter       # not exported by ingestio itself

GcsWriter(
    path: str, format: str,
    *, options: Mapping[str, str | int | float | bool] | None = None,
)
GcsWriter.write(data: DataFrame, *, replace: bool = False) -> None
```

It writes a Spark DataFrame under a prefix of a bucket, in the format it is given, with
"the one call chain of `DiskWriter`": `data.write.format(format).options(options).mode(mode)
.save(path)`. It takes no session and no client, imports no google-cloud-storage, and lists,
creates and deletes nothing itself (docs/concepts/connectors.md, section GCS).

**Its keys:** `path` and `format`, required, and `options`. **Its callback arguments:** none.
**Its extra:** none; the `gcs` extra serves only `GcsReader` without a `client`, which this
job does not use (docs/concepts/configuration.md, docs/concepts/connectors.md, section Extras).

**`path`** is `gs://<bucket>/<prefix>`, a `str`. Building raises `ValueError` (with the note of
the configuration) for (docs/concepts/connectors.md):

| Case | Message |
|---|---|
| not a `str` that starts with `gs://`, in lowercase: `GS://`, `gcs://`, `gs:/...`, `https://`, a local path, a `pathlib.Path` | `path must be a gs:// URI` |
| no bucket: `gs://`, `gs:///tables/x` | `path must name a bucket` |
| an empty, `.` or `..` segment: `gs://lake//x`, `gs://lake/./x` | `path must not contain empty, '.' or '..' segments` |
| the bucket root: `gs://lake`, `gs://lake/` | `path must name a prefix inside the bucket` |

Every trailing `/` is removed. The bucket name is not validated further: a bucket that does not
exist, or that the credentials cannot reach, fails in Spark when the write runs.

**`format`** is passed to Spark as given; this job gives `delta`. **`options`** are Spark's
write options, passed through with no default; `path`, `paths`, `replaceWhere` and
`partitionOverwriteMode` are reserved in any case and raise `option '<key>' is set by the
connector`, because the last two turn a replace into a partial replace, a write strategy
(docs/concepts/connectors.md, section Building the disk connectors).

**A write** (docs/concepts/connectors.md, sections Disk and GCS):

- `write(data)` appends and `write(data, replace=True)` replaces; the callback chooses. With
  Delta, an overwrite "is a new version of the table".
- With `delta` it writes a Delta table at the path, creating it when the path holds none.
  Nothing is registered in a catalog.
- It does not partition its output and calls no `saveAsTable` or `insertInto`. An empty
  DataFrame goes through the same call. Every error propagates out of `write()` unchanged,
  and the connector writes no log line.
- **Atomicity with Delta:** an append or an overwrite is one commit in the table's log, so
  readers through Delta see the table before the commit or after it, and a failed write leaves
  it as it was. Whether several Spark drivers can write one table at the same time on
  open-source Spark depends on Delta Lake's log store for `gs://`: "no claim is made here".
- **Privileges** of the identity Spark writes with: `storage.objects.create`, and
  `storage.objects.delete` for overwrites and for the copies of commits; Spark's reads need
  `storage.objects.get`; the connector may need more, such as `storage.buckets.get`. A Delta
  write reads the table's log before it commits *(Delta Lake)*, so the identity also reads and
  lists the prefix.
- **Two configurations do not arise.** Only `GcsReader` lists with a client of its own; the
  writer has only Spark's access to GCS, which "is the environment's concern. The connectors
  configure neither Spark nor Hadoop, and create no bucket."

#### How the library expects them to run here

- **Open-source Spark 3.5 or later, in local mode, in the container.** Spark's default file
  system is the local one, `fs.defaultFS` of `file:///`, the default outside Hadoop clusters,
  so the driver's Python process and Spark see the same directory, which `DiskReader` requires
  (docs/concepts/connectors.md, section Environments of the disk connectors).
- **Java 17 or 21 for Spark 4** (docs/concepts/connectors.md, section Environments of the GCS
  connectors).
- **The Cloud Storage connector for Hadoop on the classpath**,
  `com.google.cloud.bigdataoss:gcs-connector`, "the release for the session's Hadoop version,
  usually its shaded JAR", with its file system registered for `gs` and its credentials in the
  Hadoop configuration, `spark.hadoop.*`, as the connector's release documents
  (docs/concepts/connectors.md). The library's own integration tests configure it so: the
  shaded JAR of release `4.0.6` from Maven Central through `spark.jars`;
  `spark.hadoop.fs.gs.impl` set to `com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem`;
  `spark.hadoop.fs.AbstractFileSystem.gs.impl` set to
  `com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS`; and `spark.hadoop.fs.gs.auth.type` set to
  `APPLICATION_DEFAULT` (tests/test_gcs_connectors.py).
- **Delta Lake in the session:** "`delta-spark` for Delta Lake on open-source Spark", a
  prerequisite, never an extra (docs/concepts/connectors.md, section Environments of the disk
  connectors). The library's own tests load it through `spark.jars.packages`, with the Maven
  coordinate `io.delta:delta-spark_2.13:<version>` "that matches the installed pyspark", and
  set `spark.sql.extensions` and `spark.sql.catalog.spark_catalog`
  (tests/test_unity_catalog_connectors.py, tests/test_scd2_callback.py).
- **The bucket exists before the job runs**: the connectors create none.
- **The library's CI runs neither.** Its GCS integration tests run only with
  `INGESTIO_GCS_URL` set and its Delta tests only with `INGESTIO_DELTA_PACKAGE` set, its CI
  sets neither (.github/workflows/ci.yml), the GCS ones were "written blind"
  (specs/gcs_connectors.md, section History) and the Unity Catalog ones, with Delta Lake,
  "written without being run" (tests/test_unity_catalog_connectors.py). Nothing in the library
  shows `GcsWriter` writing Delta to a real bucket before this job (pitfall 13).

### Prerequisites

| Prerequisite | Where it comes from | How this job gets it |
|---|---|---|
| ingestio | PyPI, `ingestio==0.7.0`, with no extra | `dependencies` in `pyproject.toml`, the only entry there; `uv.lock` resolves 0.7.0 from PyPI |
| pyspark | never installed by ingestio: a prerequisite that the runtime provides, 3.5 or later (docs/concepts/connectors.md, section Extras) | the `dev` dependency group of `pyproject.toml`, pinned to a version that a Delta Lake release supports, below |
| Java 17 or 21 | a JDK, outside Python | in the container image; in CI, `actions/setup-java` with Temurin 17, already in `.github/workflows/ci.yml` |
| Delta Lake | the JAR `io.delta:delta-spark_2.13:<version>` on Maven Central, or the Python package `delta-spark`, which adds that JAR to a session it configures *(Delta Lake)* | the job's Spark session; a Python package goes in the `dev` group |
| the Cloud Storage connector | the shaded JAR of `com.google.cloud.bigdataoss:gcs-connector` on Maven Central | the job's Spark session, through `spark.jars` (a URL, or a path in the image) |
| credentials | Application Default Credentials from the environment, never the repository (`AGENTS.md`, section Goal) | mounted into the container when it runs, below |
| the bucket | the maintainer's, existing beforehand | its name and the prefix in the `target` section of each configuration file |
| the IMDb files | https://datasets.imdbws.com/ *(IMDb)* | downloaded where the job runs, outside the ingestio process, never committed (pitfall 6) |
| the container | the job's own | written by the implementer from the spec: its image, its Java, how the job and the files reach it, how it is started |

**pyspark and Delta Lake must match** *(PyPI, Delta Lake)*.

- ingestio sets only a floor, 3.5, and its own lock holds pyspark 4.2.0 (uv.lock). When this
  file was generated, the latest pyspark was 4.3.0, published on 2026-10-30, and the latest
  `delta-spark`, 4.4.1, declared `pyspark>=4.0.1,<=4.2.0`; 4.2.x and 4.3.x of `delta-spark`
  declared `<=4.1.1`. No Delta Lake release supported pyspark 4.3. So `pyspark>=3.5` alone
  resolves to a Spark that Delta Lake does not support.
- The job pins pyspark in the `dev` group to a version that the Delta Lake release it uses
  supports, such as `pyspark==4.2.0` with Delta Lake 4.4.1, or adds `delta-spark` to the `dev`
  group so that its declared range bounds pyspark in the lock. The Maven coordinate the session
  loads names the same Delta Lake version, with the Scala suffix of that Spark, `_2.13` for
  Spark 4.
- pyspark 4.2.0 and 4.3.0 are published only as source archives of about 450 MB *(PyPI)*: the
  first `uv sync` downloads and builds pyspark, and uv caches the result; CI's `setup-uv` cache
  keeps it between runs.
- An environment built without the dev group (`uv sync --no-dev`, `pip install .`) has no
  Spark: the container installs the dev group, or provides the same pyspark otherwise. One
  Spark only: an image that brings a Spark of its own besides the pyspark of `uv.lock` runs
  two, and the spec picks the one the job and its tests use.

**The Cloud Storage connector** *(Cloud Storage connector)*.

- Release 4.0.6 is built against Hadoop 3.4: the parent POM of that release sets
  `hadoop.version` to 3.4.2. The library's tests chose it as "built for Hadoop 3.4, the line
  closest to the Hadoop that pyspark bundles" (tests/test_gcs_connectors.py). The latest
  release on Maven Central when this file was generated is `4.0.10`. The spec chooses, and
  pins it in one place of the job's code that `main.py` and the tests share.
- The library's tests give the URL of the shaded JAR, the file with the classifier
  `-shaded`, to `spark.jars` (tests/test_gcs_connectors.py).
- `fs.gs.auth.type` defaults to `COMPUTE_ENGINE` in releases 4.0.6 and 4.0.10, which asks the
  metadata server of a Google Cloud machine; `APPLICATION_DEFAULT` uses Application Default
  Credentials. This job always sets `APPLICATION_DEFAULT` (pitfall 12).

**Maven Central at session start** *(Spark)*. `spark.jars.packages` and a URL in `spark.jars`
are downloaded when the session starts and cached in the user's home directory, so the first
run in a fresh container needs the network, and so does every CI run. An image that bakes the
JARs in and names their paths avoids it; the spec chooses.

**Application Default Credentials** *(Google Cloud)*. ADC looks, in order, for the file that
`GOOGLE_APPLICATION_CREDENTIALS` names, then for the file `gcloud auth application-default
login` writes, `~/.config/gcloud/application_default_credentials.json` on Linux, then for the
service account of a Google Cloud machine. For this job:

- The container receives the file when it starts, mounted read-only from outside the
  repository, with `GOOGLE_APPLICATION_CREDENTIALS` naming its path inside the container, or
  mounted where gcloud's file is looked for. The file is never in the image, the repository or
  the build context; `.gitignore` already lists the usual names of credential files.
- Whose credentials, user or service account, and their privileges on the bucket, are the
  maintainer's. The job only documents what the identity needs (see Privileges above).
- `GcsWriter` itself never sees them: only the Cloud Storage connector in the JVM does.

### Known pitfalls

Each with what the job does about it.

1. **An empty batch empties the table, or fails.** `FullLoad` calls the writer whatever the
   batch holds: an empty DataFrame with columns overwrites the table with no row, and one with
   no column fails in Spark (docs/concepts/callbacks.md, docs/concepts/connectors.md). A
   missing file, an empty directory or a watermark that selects nothing gives such a batch.
   *The job* ends the load step with its own callback, raising `CancelStepException` when the
   batch is empty, before the write in the same step, and gives each source a `schema`. The
   spec says whether an empty source is a normal run or an error.
2. **An empty selection has no `_metadata` column**, with or without `schema`
   (docs/concepts/connectors.md). *The job* checks for an empty batch before any of its
   callbacks reads `_metadata`.
3. **One directory per IMDb file.** `path` is a directory, never a file, and the reader loads
   every candidate in it in one load (docs/concepts/connectors.md); each IMDb file has its own
   columns. `pathGlobFilter` is reserved, so `options` cannot select a file. A
   `watermark_pattern` matching one name also filters a shared directory, as the library
   documents, but its one group then becomes the file's watermark: a pattern such as
   `title\.basics\.tsv\.(gz)` captures the constant `gz`, and the file's last-modified time is
   no longer its watermark. *The spec* puts each file the job reads alone in a directory of its
   own, such as `<root>/title.basics/title.basics.tsv.gz`, unless it has no use for a
   watermark.
4. **One task per file.** A gzip file cannot be split *(Spark)*, so each file is read by one
   task, however many cores the session has; the largest IMDb files, such as
   `title.principals` and `title.akas`, then take one core each for their whole read, and every
   action on the DataFrame reads them again. *The spec* sizes the session (`local[N]`, driver
   memory) for the files it loads, and avoids actions that read the files once more than
   needed, such as a count before the write.
5. **TSV parsing is silent by default** *(Spark)*. With the default `quote`, a value that
   starts with `"` can swallow the fields after it; in `PERMISSIVE` mode a value that does not
   parse becomes null without a word. With `schema` and `header`, Spark's default
   (`enforceSchema` true) ignores the header's names and applies the schema by position;
   without `schema`, every column is a string unless `inferSchema` is set, which reads the files
   once more. Spark's CSV parser reads a `BOOLEAN` from `true` and `false`, not from IMDb's `0`
   and `1`, and IMDb's lists, such as `genres`, are comma-separated text. *The spec* chooses
   the `schema` of each file (such as `INT` for the `0`/`1` columns), sets `quote`, `nullValue`
   and `mode` deliberately, converts in a callback of its own what it wants typed otherwise,
   and has its tests write values with quotes, `\N` and `0`/`1` in them.
6. **Fixed names, daily snapshots, downloads in progress.** The IMDb files keep their names
   and each is a full snapshot *(IMDb)*, so a name pattern carries no watermark. The
   last-modified time is the time the file was written where the job runs, unless the download
   keeps the server's, as `curl --remote-time` does; a new download of an unchanged file gets
   a new time. A file still being written when `read()` lists the directory can be read in part
   (docs/concepts/connectors.md): *the download* writes under a name that starts with `.` or
   `_`, or ends with `._COPYING_`, and renames the file when it is complete. The download runs
   outside the ingestio process, as a step of the container or a script of the job: a callback
   that fetched files over HTTP would integrate an external system outside a connector
   (section 5, rule 5). *The spec* says how and when the files arrive.
7. **Full replace or incremental: what each costs.** Each IMDb file is a full snapshot, so:
   - **`FullLoad` per table** makes each table hold the latest snapshot. It is idempotent, and
     each run is one Delta commit per table. With a last-modified watermark it can also skip
     a file that has not changed since the last run, which is then an empty selection
     (pitfall 1); a lost watermark only makes the next run load the same snapshot again.
   - **`AppendOnly` per table** keeps every snapshot in the table, which only makes sense
     with a lineage column that names the snapshot; running the same file twice appends it
     twice. Where the job keeps its last watermark matters: in a file of the container, it is
     a second commit after the table's, and a crash between the two appends the snapshot again
     on the next run.
   - **An upsert or a merge** of a snapshot into the table is pitfall 8.

   *The spec* chooses per table and states the guarantee that follows.
8. **No merge, and no reading of the target, at 0.7.0.** No shipped callback merges, `SCD2`
   rejects `gcs`, and no shipped reader reads a Delta table at a `gs://` path: `GcsReader` and
   `DiskReader` reject `delta`, and `UnityCatalogReader` reads tables of a catalog by name
   (docs/concepts/callbacks.md, docs/concepts/connectors.md). A strategy that needs the rows
   already in the table, such as an upsert by `tconst`, a deduplication against the table or a
   watermark read back from the table, cannot be built with the public API: a job's own
   callback that read the table with `spark.read`, or merged with Delta Lake's own API, would
   reach GCS outside a connector. *The planner* records it as a library gap instead.
9. **The watermark is computed before the process is built.** `watermark_value` is an
   argument of `ReadSource`, so a step of the same process cannot compute it
   (docs/concepts/configuration.md). *The job* reads it in `main.py` before it builds the
   process, from where the spec keeps it.
10. **`path` must be absolute.** A configuration cannot name a directory relative to itself
    or to the repository: a relative path is rejected, because the driver's Python process and
    Spark can resolve it differently (docs/concepts/connectors.md). *The job's* configuration
    names the absolute directory inside the container; *the tests* write their own
    configuration with a `tmp_path` directory. A job that needs a directory relative to the
    repository records a library gap rather than rewriting the configuration in code.
11. **`options` go to Spark as they are, so no credential goes in them.** The target's
    `options` reach the DataFrame writer unchanged (docs/concepts/connectors.md); Spark and
    Delta Lake can hand file-system options, such as Hadoop's `fs.*` keys, to the file system
    of the write *(Spark, Delta Lake)*. An authentication setting there would put a credential,
    or the way to one, in `config.json`. *The job* sets authentication only in its session, and
    only to Application Default Credentials; the target's `options` hold Delta Lake's write
    options, if any (pitfall 14).
12. **The connector's default is not ADC.** Without `spark.hadoop.fs.gs.auth.type` set to
    `APPLICATION_DEFAULT`, the Cloud Storage connector, in releases 4.0.6 and 4.0.10, asks the
    metadata server of a Google Cloud machine, which a container elsewhere does not have
    *(Cloud Storage connector)*: the write fails when it runs, not when the session starts. The
    settings of the session are not checked before the first `gs://` access. *The job* sets it,
    and its documentation says which ADC file the container needs.
13. **The first real use of `GcsWriter` with Delta.** The library's CI runs neither its GCS nor
    its Delta integration tests (see How the library expects them to run here). A failure of the
    write that the library's documentation does not explain, such as a commit that the log
    store for `gs://` mishandles, is reported to the maintainer, with the versions of Spark,
    Delta Lake and the connector, and recorded as a gap when the job cannot follow the rules
    without a workaround.
14. **Delta Lake's own rules** *(Delta Lake)*. An overwrite whose schema differs from the
    table's fails unless the write option `overwriteSchema` is `true`, and an append with new
    columns fails unless `mergeSchema` is `true`: these are options of the target section, not
    write strategies, and the spec decides them. Column names with spaces or any of `,;{}()`,
    a newline, a tab or `=` are rejected without column mapping; IMDb's names have none. An
    overwrite keeps the files of earlier versions until a `VACUUM`, so the prefix grows with
    every `FullLoad`; ingestio offers no `VACUUM`, and running one from a callback would reach
    GCS outside a connector. *The spec* accepts the growth, or records a gap.
15. **No partitioning.** The writer never partitions, `options` cannot ask for it, and
    `replaceWhere` and `partitionOverwriteMode` are reserved (docs/concepts/connectors.md). A
    table that needs partitions is a library gap.
16. **No atomicity across tables.** Each table is its own commit. When a table's step fails,
    the process stops: the tables written before it keep their new version and the ones after
    it are not run. A `CancelStepException` in one table's load step skips that table only.
    *The spec* orders the tables and says what a failed run leaves behind.
17. **The session is the job's, and the first one.** The connectors never create a session;
    without one, `read()` fails with PySpark's error (docs/concepts/connectors.md).
    `SparkSession.builder...getOrCreate()` returns a session already running, with its own
    configuration *(Spark)*, and a JVM loads its packages once
    (tests/test_unity_catalog_connectors.py): *the job* builds its session, with Delta Lake and
    the Cloud Storage connector, in `main.py` before `Process.run()`, and *the tests* in one
    fixture for the whole run.
18. **Time zones.** Spark hands timestamps to Python as naive local times *(Spark)*, and
    `DiskReader` rejects a naive `watermark_value` (docs/concepts/connectors.md). *The job*
    sets `spark.sql.session.timeZone` to `UTC`, as the library's tests do
    (tests/test_disk_connectors.py), and makes every watermark it passes timezone-aware.
19. **Two consoles.** `log_to_console()` never touches the root logger: a job that also prints
    the root logger on the console sees every line twice (docs/concepts/lifecycle.md). *The
    job* chooses one.

### What the spec decides

This guidance leaves these choices to the job's planner, each with the pitfalls above:

- which IMDb files the job loads, how they reach their directories in the container, and
  that the job keeps to their license (pitfalls 3 and 6);
- the strategy of each table, `FullLoad` or `AppendOnly`, its watermark if any and where the
  job keeps it, and what a failed or repeated run leaves behind (pitfalls 1, 7, 8, 9 and 16);
- the TSV options and the `schema` of each file, and the conversions the job's own callbacks
  make (pitfall 5);
- the layout in the bucket: the prefix of each table, and the configuration file of each
  (section 3);
- the container: its image, Java, how the job, pyspark and the JARs get into it, how the IMDb
  directory and the credentials are mounted, and how it is started;
- the versions of pyspark, Delta Lake and the Cloud Storage connector, which must match
  (Prerequisites);
- the job's own callbacks, and where each is placed;
- what the tests cover with a local target, and what the documentation says to check on the
  first real run against the bucket.

## 5. Rules of a valid job

Every process of this job follows these rules. A process that can follow one only with a
workaround of the library is not written with the workaround: the gap is recorded in
`LIB_GAPS.md`, as `AGENTS.md` says. Under each rule, what it means at version
`0.7.0`.

1. **Only the public API of the library, never an internal import.**

   The public API is what `ingestio`, `ingestio.callback.all` and `ingestio.connector.all`
   export, through those import paths, and the documented attributes `Process.state`,
   `Process.run_id`, `Process.cancelled`, `Process.started_at` and `Callback.process`
   (AGENTS.md of the library), plus `Callback.name` (docs/concepts/callbacks.md). The
   connectors are exported by `ingestio.connector.all` only, never by `ingestio`. Everything
   else is internal: `ingestio.basics`, `ingestio.core`, `ingestio.console`,
   `ingestio.callback.core`, `ingestio.callback.io`, `ingestio.callback._configuration`,
   `ingestio.connector.core`, `ingestio.connector.local`, `ingestio.connector.gcs`,
   `ingestio.connector._files` and every other module, and every name that starts with `_`,
   even when Python lets the job reach it. pyspark, Delta Lake and the Cloud Storage connector
   are not ingestio: this rule does not cover them. The full list of names is in section 6.

2. **Custom connectors and callbacks only through the documented extension.**

   For callbacks: a subclass of `Callback` in the job's own code, `src/main.py`, nothing
   registered (docs/philosophy.md, docs/concepts/callbacks.md). For connectors, there is no
   documented extension at 0.7.0: only the shipped systems can be named in a configuration,
   and a job cannot add one (ADR 0015). What a job's own callback may do is build a shipped
   connector of `ingestio.connector.all` itself (docs/concepts/connectors.md, section Using a
   connector). A process that needs a system or a capability the library does not ship, such
   as reading or merging a Delta table at a `gs://` path, is a library gap.

3. **Criticality declared explicitly on every non-essential callback.**

   At 0.7.0 no callback can be declared non-essential: there is no declaration and no run
   result (docs/concepts/criticality.md). Every callback of this job is essential, and its
   failure fails the run. A process that needs a non-essential stage, one whose failure
   should let the run go on and be recorded, such as a table whose failure should not stop
   the others, cannot be built: it is a library gap. A callback that catches its own failure
   so that the run goes on is a non-essential stage without the record the philosophy
   requires: a workaround, not a solution. Raising `CancelStepException` for a condition the
   spec expects, such as an empty batch, is control flow, not criticality.

4. **No connector decides the write strategy.**

   The callback the job places chooses it: `AppendOnly` appends, `FullLoad` replaces, `SCD2`
   keeps a history (ADR 0016). The configuration has no mode, and `append`, `replace` and
   `mode` are not keys (docs/concepts/configuration.md); `replaceWhere` and
   `partitionOverwriteMode` are reserved because they would turn a replace into a partial one
   (docs/concepts/connectors.md). A job's own callback that builds a shipped writer chooses by
   calling `write(data)` or `write(data, replace=True)` itself. For this job the strategy of
   each table is `FullLoad` or `AppendOnly`, as the spec chooses, and no option of the `target`
   section changes it.

5. **No monkeypatching, no subclassing of internals, no other way around the library.**

   Legitimate at 0.7.0: subclassing `Callback`; the arguments of `Process` (`state`, `run_id`,
   `log`) and of `Step`; the documented arguments of the shipped callbacks (`into`, `key`,
   `connection`, `client`, `watermark_value`, `spark`); the sections and keys of the
   configuration, one file per source and target; the two cancel exceptions;
   `log_to_console`; `Reader` and `Writer` in annotations; building a shipped connector inside
   a callback of the job's own. The job's Spark session, with Delta Lake and the Cloud Storage
   connector, is not a way around the library: the library leaves the session and Spark's
   access to GCS to the environment (README.md, docs/concepts/connectors.md). Neither is a
   download of the IMDb files that runs before the process, nor tests that write
   configurations of their own under `tmp_path`, which is how the library tests itself.
   Working around the library: subclassing a shipped callback or connector to reach its
   private attributes; replacing a function of ingestio or of the libraries it calls, such as
   `SparkSession.active`, in the job's code; generating the job's configuration from code at run
   time to get around a rule of the configuration; writing a table with Spark's own writer
   (`df.write...save("gs://...")`) instead of `FullLoad` or `AppendOnly`; reading the files
   with `spark.read` instead of `ReadSource`; reading, merging or vacuuming the Delta table from
   a callback with Spark or Delta Lake's own API; and putting file-system or authentication
   settings in the `options` of a section to reach what the session should hold.

6. **The library's default callbacks whenever they fit,** append-only and SCD2 among them,
   instead of reimplementing them.

   At 0.7.0 the library ships `ReadSource`, to read the `source` of a configuration into the
   state; `AppendOnly`, to add a batch to the target; `FullLoad`, to make the target hold
   exactly the batch; and `SCD2`, to keep the history of a dimension on `unity_catalog`,
   `postgres`, `mysql` or `bigquery` (docs/concepts/callbacks.md). This job reads with
   `ReadSource` and writes with `FullLoad` or `AppendOnly`; it never reads the files or writes
   the tables with code of its own. `SCD2` does not fit a `gcs` target.

## 6. Public API at a glance

**`ingestio`** (src/ingestio/__init__.py)

| Name | Signature |
|---|---|
| `Process` | `Process(steps: Iterable[Step], *, state: Mapping[str, Any] \| None = None, run_id: str \| None = None, log: logging.Logger \| None = None)`; `run() -> Process`; attributes `state: dict[str, Any]`, `run_id: str`, `cancelled: bool`, `started_at: datetime \| None` |
| `Step` | `Step(name: str, callbacks: Iterable[Callback] = ())`, immutable; `ValueError` for a blank name |
| `Callback` | subclass it; `before() -> None`, `run() -> None`, `after() -> None`; attributes `process: Process`, `name: str` (a property, the class name by default) |
| `CancelStepException` | `Exception` subclass: skip the rest of the step |
| `CancelProcessException` | `Exception` subclass: stop the process cleanly |
| `ReadSource` | `ReadSource(config: str \| os.PathLike[str], *, into: str = "records", connection: PostgresConnection \| MongoDBConnection \| MySQLConnection \| None = None, client: object \| None = None, watermark_value: int \| Decimal \| str \| date \| None = None, spark: SparkSession \| None = None)` |
| `AppendOnly` | `AppendOnly(config: str \| os.PathLike[str], *, key: str = "records", connection: PostgresConnection \| MySQLConnection \| None = None, client: object \| None = None)` |
| `FullLoad` | `FullLoad(config, *, key="records", connection=None, client=None)`, as `AppendOnly` |
| `SCD2` | `SCD2(config, *, key="records", connection=None, client=None)`, as `AppendOnly`; reads the `target` and `scd2` sections |
| `Reader` | `Protocol[T_co]` with `read() -> T_co` |
| `Writer` | `Protocol[T_contra]` with `write(data: T_contra) -> None` |
| `log_to_console` | `log_to_console(*, stream: TextIO \| None = None, level: int = logging.INFO, color: bool \| None = None) -> logging.Handler`; `ValueError("level must be above NOTSET")` |
| `__version__` | `"0.7.0"` |

**`ingestio.callback.all`** (src/ingestio/callback/all.py): `SCD2`, `AppendOnly`, `Callback`,
`CancelProcessException`, `CancelStepException`, `FullLoad`, `ReadSource`, the same objects
as above.

**`ingestio.connector.all`** (src/ingestio/connector/all.py, docs/concepts/connectors.md).
Every reader below except `JsonApiReader` returns a `pyspark.sql.DataFrame`, and every writer
except `JsonLinesWriter` takes one, with `write(data: DataFrame, *, replace: bool = False) ->
None`.

| Name | `system` | Signature |
|---|---|---|
| `Reader`, `Writer` | | the protocols above |
| `JsonApiReader` | `json_api` | `JsonApiReader(url: str, *, timeout: float = 30.0, headers: dict[str, str] \| None = None, encoding: str = "utf-8")`; `read()` returns the decoded JSON |
| `JsonLinesWriter` | `json_lines` | `JsonLinesWriter(path: Path \| str, *, encoding: str = "utf-8")`; `write(data: Iterable[Mapping[str, Any]], *, replace: bool = False) -> None` |
| `DiskReader` | `disk` | `DiskReader(path: str \| PathLike[str], format: str, *, watermark_pattern: str \| None = None, watermark_value: str \| datetime \| None = None, schema: StructType \| str \| None = None, options: Mapping[...] \| None = None, spark: SparkSession \| None = None)` |
| `DiskWriter` | `disk` | `DiskWriter(path: str \| PathLike[str], format: str, *, options: Mapping[...] \| None = None)` |
| `GcsReader` | `gcs` | `GcsReader(path: str, format: str, *, watermark_pattern: str \| None = None, watermark_value: str \| datetime \| None = None, schema: StructType \| str \| None = None, options: Mapping[...] \| None = None, client: google.cloud.storage.Client \| None = None, spark: SparkSession \| None = None)` |
| `GcsWriter` | `gcs` | `GcsWriter(path: str, format: str, *, options: Mapping[...] \| None = None)` |
| `S3Reader` | `s3` | `S3Reader(path: str, format: str, *, watermark_pattern=None, watermark_value: str \| datetime \| None = None, schema=None, options=None, client: S3Client \| None = None, spark=None)` |
| `S3Writer` | `s3` | `S3Writer(path: str, format: str, *, options=None)` |
| `VolumeReader` | `volume` | `VolumeReader(volume: str, path: str, format: str, *, watermark_pattern=None, watermark_value: str \| datetime \| None = None, schema=None, options=None, client: WorkspaceClient \| None = None, spark=None)` |
| `VolumeWriter` | `volume` | `VolumeWriter(volume: str, path: str, format: str, *, options=None)` |
| `PostgresConnection` | | `PostgresConnection(host: str, database: str, user: str, password: str, *, port: int = 5432, sslmode: str \| None = None)`, frozen; attributes of the same names |
| `PostgresReader` | `postgres` | `PostgresReader(table_name: str, connection: PostgresConnection, *, watermark_column: str \| None = None, watermark_value: int \| Decimal \| str \| date \| None = None, options=None, spark=None)` |
| `PostgresWriter` | `postgres` | `PostgresWriter(table_name: str, connection: PostgresConnection, *, options=None)` |
| `MySQLConnection` | | `MySQLConnection(host: str, database: str, user: str, password: str, *, port: int = 3306, ssl_mode: str \| None = None)`, frozen; attributes of the same names |
| `MySQLReader` | `mysql` | `MySQLReader(table_name: str, connection: MySQLConnection, *, watermark_column: str \| None = None, watermark_value: int \| Decimal \| str \| date \| None = None, options=None, spark=None)` |
| `MySQLWriter` | `mysql` | `MySQLWriter(table_name: str, connection: MySQLConnection, *, options=None)` |
| `MongoDBConnection` | | `MongoDBConnection(uri: str)`, frozen |
| `MongoDBReader` | `mongodb` | `MongoDBReader(database: str, collection: str, connection: MongoDBConnection, *, watermark_column: str \| None = None, watermark_value: int \| Decimal \| str \| date \| None = None, schema_hints: str \| None = None, options=None, spark=None)`; a source only |
| `UnityCatalogReader` | `unity_catalog` | `UnityCatalogReader(table_name: str, *, watermark_column=None, watermark_value: int \| Decimal \| str \| date \| None = None, options=None, spark=None)` |
| `UnityCatalogWriter` | `unity_catalog` | `UnityCatalogWriter(table_name: str, *, options=None)` |
| `BigQueryReader` | `bigquery` | `BigQueryReader(table_name: str, *, watermark_column=None, watermark_value: int \| Decimal \| str \| date \| None = None, billing_project=None, options=None, client: bigquery.Client \| None = None, spark=None)` |
| `BigQueryWriter` | `bigquery` | `BigQueryWriter(table_name: str, *, billing_project=None, options=None, client: bigquery.Client \| None = None)` |

`Mapping[...]` is `Mapping[str, str | int | float | bool]` throughout. Building any connector
validates its arguments and touches neither Spark, nor the network, nor the file system; a
connector whose extra is missing raises `ImportError` naming the extra when it is built
(docs/concepts/connectors.md, section Extras).

## 7. Sources

Every file of the ingestio repository read to write this guidance, at tag `v0.7.0`:

- `README.md`, `AGENTS.md`, `CHANGELOG.md`, `pyproject.toml`, `.github/workflows/ci.yml`, and
  the entries of `uv.lock` for pyspark, py4j and google-cloud-storage
- `docs/philosophy.md`, `docs/index.md`, `docs/backlog.md`
- `docs/concepts/callbacks.md`, `docs/concepts/configuration.md`,
  `docs/concepts/connectors.md` (Capabilities, Using a connector, `JsonApiReader`,
  `JsonLinesWriter`, the opening of PostgreSQL with How a connector receives an ingestion's
  parameters, Disk, GCS, Differences from the disk connectors, Known divergence, Extras),
  `docs/concepts/criticality.md`, `docs/concepts/lifecycle.md`
- `docs/decisions/0002-zero-dependency-core.md`, `0003-connectors-as-protocols.md`,
  `0004-shared-process-state.md`, `0005-declaration-order.md`, `0006-three-pass-step.md`,
  `0007-cancellation-exceptions.md`, `0008-stdlib-logging-run-id.md`,
  `0009-typed-package.md`, `0011-callbacks-as-stages-with-criticality.md`,
  `0012-streaming-deferred.md`, `0013-connector-configuration-convention.md`,
  `0014-create-a-missing-target-only-when-derived-from-the-data.md`,
  `0015-choose-connectors-from-the-job-configuration.md`,
  `0016-make-each-write-strategy-one-callback.md`,
  `0017-read-a-library-callbacks-own-parameters-from-a-section-named-after-it.md`
- `specs/gcs_connectors.md` (summary, motivation, fit, scope, public API, behaviour, the `gcs`
  extra, supported environments, known limitations, notes for testing, integration tests, the
  writing criteria, decisions, open questions, history), `specs/disk_connectors.md` (supported
  environments, notes for testing, integration tests), `specs/config_connectors.md` (known
  limitations, notes for testing), `specs/append_only_callback.md` (an empty batch, known
  limitations, decisions 3 and 4)
- `src/ingestio/__init__.py`, `src/ingestio/basics.py`, `src/ingestio/core.py` (the engine),
  `src/ingestio/console.py` (its head and `log_to_console`), `src/ingestio/callback/__init__.py`,
  `src/ingestio/callback/all.py`, `src/ingestio/callback/core.py`,
  `src/ingestio/callback/io.py` (`ReadSource`, `AppendOnly`, `FullLoad`),
  `src/ingestio/callback/_configuration.py` (the table of systems and the checks),
  `src/ingestio/connector/__init__.py`, `src/ingestio/connector/all.py`,
  `src/ingestio/connector/core.py`, `src/ingestio/connector/local.py`,
  `src/ingestio/connector/gcs.py`, `src/ingestio/connector/_files.py`, and the signatures of
  the classes of `http.py`, `postgres.py`, `mysql.py`, `mongodb.py`, `s3.py`, `volume.py`,
  `unity_catalog.py` and `bigquery.py`
- `tests/conftest.py`; the header and the integration part of `tests/test_gcs_connectors.py`;
  the header of `tests/test_unity_catalog_connectors.py` and its session's configuration; the
  header and the Delta test of `tests/test_scd2_callback.py`; `tests/` searched for Delta Lake
  and for how the library gates its Spark tests (`INGESTIO_SPARK_TESTS`)
- the list of files of the tag, `git ls-tree -r --name-only v0.7.0`, and the comparison with
  `v0.5.0`, `git log --oneline v0.5.0..v0.7.0` and `git diff --stat v0.5.0 v0.7.0`

Outside the repository, on 2026-11-18: the release metadata of ingestio 0.7.0, pyspark (4.2.0,
4.2.1, 4.3.0) and delta-spark (4.0.0 to 4.4.1) on PyPI; the versions of
`com.google.cloud.bigdataoss:gcs-connector` and `io.delta:delta-spark_2.13` on Maven Central,
and the POM of `gcs-connector` 4.0.6 with its parent; `gcs/CONFIGURATION.md` of
GoogleCloudDataproc/hadoop-connectors at tags `v4.0.6` and `v4.0.10`; the page of the IMDb
non-commercial datasets, https://developer.imdb.com/non-commercial-datasets/; and the latest
release of `actions/setup-java`, v6.3.0.
