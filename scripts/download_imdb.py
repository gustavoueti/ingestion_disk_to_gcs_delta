"""Download the IMDb files the job loads, on the host, with the standard library only.

Usage: `uv run python scripts/download_imdb.py <directory> [--base-url <url>]`.

Each file goes to `<directory>/<name>/<name>.tsv.gz`, alone in its directory, under a temporary
name that starts with `.` until it is complete, so that the job's reader never selects a partial
file. The IMDb datasets are licensed for personal and non-commercial use only: never commit them.
"""

from __future__ import annotations

import argparse
import contextlib
import logging
import os
import sys
import urllib.request
from collections.abc import Sequence
from email.utils import parsedate_to_datetime
from pathlib import Path

logger = logging.getLogger("download_imdb")

DEFAULT_BASE_URL = "https://datasets.imdbws.com/"
# In load order. The names agree with the `source.path` of the job's configuration files.
NAMES = ("title.basics", "title.ratings")
TIMEOUT_SECONDS = 60


def download(url: str, destination: Path) -> None:
    """Download `url` to `destination`, through a hidden temporary file in its directory.

    A failure removes the temporary file and leaves an existing destination as it was.
    """
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(f".{destination.name}")
    try:
        with urllib.request.urlopen(url, timeout=TIMEOUT_SECONDS) as response:
            if response.status != 200:
                raise OSError(f"HTTP status {response.status}")
            with temporary.open("wb") as out:
                while chunk := response.read(1024 * 1024):
                    out.write(chunk)
            modified = response.headers.get("Last-Modified")
        if modified:
            seconds = parsedate_to_datetime(modified).timestamp()
            ns = round(seconds * 1_000_000_000)
            os.utime(temporary, ns=(ns, ns))
        temporary.replace(destination)
    except BaseException:
        with contextlib.suppress(FileNotFoundError):
            temporary.unlink()
        raise


def main(*, argv: Sequence[str] | None = None) -> int:
    """Download every file, in order; return the exit status."""
    parser = argparse.ArgumentParser(description="Download the IMDb files the job loads.")
    parser.add_argument("directory", type=Path, help="the directory that receives the files")
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL, help="where the files are served")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, stream=sys.stderr, format="%(levelname)s %(message)s")
    base_url = args.base_url if args.base_url.endswith("/") else f"{args.base_url}/"
    for name in NAMES:
        file_name = f"{name}.tsv.gz"
        url = f"{base_url}{file_name}"
        destination = args.directory / name / file_name
        try:
            download(url, destination)
        except Exception as error:
            logger.error("%s failed: %s: %s", url, type(error).__name__, error)
            return 1
        logger.info("%s -> %s", url, destination)
    return 0


if __name__ == "__main__":
    sys.exit(main())
