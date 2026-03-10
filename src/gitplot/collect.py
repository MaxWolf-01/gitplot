"""Shared blame data collection — the expensive part."""

import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

import polars as pl

from gitplot.data import SCHEMA, load_existing, save_data
from gitplot.git import blame_lines, get_all_commits, resolve_repo, sample_evenly, tracked_files


def _log(msg: str, quiet: bool = False) -> None:
    if not quiet:
        print(msg, file=sys.stderr)


def _analyze_commit(
    repo: str,
    commit: str,
    commit_date: datetime,
    exts: list[str] | None,
    file_workers: int = 4,
) -> list[tuple[str, float, int, str]]:
    """Blame every tracked file at a commit. Returns (hash, commit_ts, line_ts, author) rows."""
    files = tracked_files(repo, commit, exts)
    rows: list[tuple[str, float, int, str]] = []
    ct = commit_date.timestamp()

    with ThreadPoolExecutor(max_workers=file_workers) as ex:
        futs = {ex.submit(blame_lines, repo, commit, f): f for f in files}
        for fut in as_completed(futs):
            for ts, author in fut.result():
                rows.append((commit, ct, ts, author))
    return rows


def ensure_data(
    repo: str,
    samples: int,
    workers: int,
    extensions: str,
    output: str,
    quiet: bool,
) -> tuple[pl.DataFrame, Path, str]:
    """Ensure blame data is collected. Returns (dataframe, data_dir, repo_name).

    Shared by all blame-based chart types. Loads existing parquet,
    computes only missing commits, saves updated parquet.
    """
    repo_path, repo_name = resolve_repo(repo)
    _log(f"Resolving {repo}...", quiet)
    repo_str = str(repo_path)

    exts = [e.strip() for e in extensions.split(",") if e.strip()] or None
    exts_key = extensions.strip()

    data_dir = Path(output) / repo_name
    existing = load_existing(data_dir, exts_key)

    all_commits = get_all_commits(repo_str)
    sampled = sample_evenly(all_commits, samples)
    _log(f"{len(all_commits)} commits in repo, sampling {len(sampled)}.", quiet)

    already_done: set[str] = set()
    if existing is not None:
        already_done = set(existing["commit_hash"].unique().to_list())

    todo = [(h, d) for h, d in sampled if h not in already_done]

    if todo:
        _log(f"{len(todo)} commits to analyze.", quiet)
        new_rows: list[tuple[str, float, int, str]] = []
        total = len(todo)

        with ThreadPoolExecutor(max_workers=workers) as ex:
            futs = {ex.submit(_analyze_commit, repo_str, h, d, exts): h for h, d in todo}
            for i, fut in enumerate(as_completed(futs), 1):
                h = futs[fut]
                _log(f"  [{i}/{total}] {h[:8]}", quiet)
                new_rows.extend(fut.result())

        new_df = pl.DataFrame(new_rows, schema=SCHEMA, orient="row")
        df = pl.concat([existing, new_df]) if existing is not None else new_df
    else:
        df = existing
        if df is None:
            _log("No data and nothing to analyze.", quiet)
            raise SystemExit(1)
        _log("All sampled commits already analyzed.", quiet)

    save_data(data_dir, df, repo, exts_key)
    _log(f"Data saved: {data_dir / 'blame.parquet'}", quiet)
    return df, data_dir, repo_name
