# AGENTS.md

Instructions for AI coding agents working on **ingestion_disk_to_gcs_delta**, a proof-of-concept ingestion
job built with [ingestio](https://github.com/gustavoueti/ingestio) `0.7.0`,
installed from PyPI: https://pypi.org/project/ingestio/0.7.0/. Start every task at [docs/index.md](docs/index.md), then read
[.claude/kb/ingestio.md](.claude/kb/ingestio.md).

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

This job exists for three reasons: to prove that ingestio can be used from the outside, by
someone who only has its public API and its documentation; to reveal design problems of the
library through real use; and to show what the library looks like in practice.

## The library

Everything this job needs to know about ingestio is in
[.claude/kb/ingestio.md](.claude/kb/ingestio.md), generated for version `0.7.0`:
its philosophy and concepts, how to extend it, how a job is structured (`src/main.py`, with
the job's own callbacks, and `src/config.json`), the pieces this job uses, their pitfalls,
and the **rules of a valid job**.

- The version in the stamp of that file and the version pinned in `pyproject.toml` are the
  same. If they differ, stop and report it: the guidance is stale.
- Never edit that file. When the job moves to another version of ingestio, the maintainer has
  it regenerated.

## Rules of the job

- **Only public or local sources:** public APIs, files, SQLite, databases in Docker. Never
  credentials, and never real data of third parties.
- **Only valid code**, as the rules of a valid job in the knowledge base define it: the
  public API of ingestio, its documented extension, no workaround.
- **A gap instead of a workaround.** When a process works only with a workaround of the
  library, the job does not accept the workaround: the gap is recorded in
  [LIB_GAPS.md](LIB_GAPS.md) and the maintainer decides what happens to the process.
- **Every change to the job updates its version and [CHANGELOG.md](CHANGELOG.md).** Tags
  `vX.Y.Z` are created by the maintainer.

## Ask the maintainer

When a task needs a definition that only the maintainer can give, ask for it before going
further. Never fill the gap with a guess, a convenient default or the most likely answer. Ask
everything at once, each question with the options you see and the one you recommend. An
agent of the workflow cannot reach the maintainer: it stops and reports the question, and the
main session takes it to the maintainer. In planning, the questions go under **Open
questions** in the spec.

## Constraints

- **Never write to git or GitHub.** Reading history and logs is fine: `git log`, `show`,
  `diff`, `status`, `blame`, `ls-files`, `rev-parse`, `for-each-ref`, and the read commands of
  `gh` such as `gh pr view` or `gh run view`. Anything that changes the index, refs, history,
  a remote or a repository is the maintainer's: add, commit, push, merge, rebase, reset,
  checkout, switch, restore, stash, tag, branches, worktrees, pull requests, releases,
  repositories.
- **Never use worktree isolation.** A worktree creates a branch.
- **Never add a `Co-Authored-By` trailer or an AI-generation footer** to a commit or a pull
  request, whatever the tool.
- **Write only inside this repository.** Never change the ingestio repository, the hub this
  job was generated from, or anything else outside this directory.
- **Never change the version of ingestio** pinned in `pyproject.toml`.
- **Leave the architecture as it is.** These files come from the hub and change only when the
  hub regenerates them: `AGENTS.md`, `CLAUDE.md`, `CONTRIBUTING.md`, `.claude/agents/`,
  `.claude/hooks/`, `.claude/kb/`, `.claude/settings.json`, `.pre-commit-config.yaml`,
  `docs/workflow.md` and `specs/_template.md`.

## Development workflow

Every change to the job goes through five stages, each with its own agent:

planning → tests → documentation → implementation → validation

The spec in `specs/` records where the change stands. Stages, statuses, write scopes and
returns are in [docs/workflow.md](docs/workflow.md); code, tests, commits and pull requests
follow [CONTRIBUTING.md](CONTRIBUTING.md).

## Library gaps

[LIB_GAPS.md](LIB_GAPS.md) records what this job could not do with the library as it is.
Any agent that finds one stops and reports it; the main session records it in the format the
file gives, and the hub consolidates the gaps of every job for the library's maintainers.

## Finishing a task

End with a summary of the changes, a suggested commit message in Conventional Commits form,
and the commands for the maintainer to run, as [CONTRIBUTING.md](CONTRIBUTING.md#finishing-a-change)
shows: the branch, the commit, the push and the pull request, with no co-author trailers.
