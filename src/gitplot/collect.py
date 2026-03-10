"""Shared blame data collection — the expensive part."""

import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

import polars as pl

from gitplot.data import SCHEMA, load_existing, save_data
from gitplot.git import (
    blame_lines,
    blame_lines_with_hash,
    get_all_commits,
    get_coauthor_map,
    get_log_numstat,
    resolve_repo,
    sample_evenly,
    tracked_files,
)


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


LOG_SCHEMA = {
    "commit_hash": pl.Utf8,
    "commit_date": pl.Float64,
    "author": pl.Utf8,
    "file_path": pl.Utf8,
    "insertions": pl.Int64,
    "deletions": pl.Int64,
}


def ensure_log_data(
    repo: str,
    output: str,
    quiet: bool,
) -> tuple[pl.DataFrame, Path, str]:
    """Collect git log --numstat data. Returns (dataframe, data_dir, repo_name).

    Fast — single git log parse, no blame. No sampling needed.
    Re-collected each run (cheap enough that caching isn't worth the complexity).
    """
    repo_path, repo_name = resolve_repo(repo)
    _log(f"Resolving {repo}...", quiet)

    _log("Collecting git log data...", quiet)
    rows = get_log_numstat(str(repo_path))
    df = pl.DataFrame(rows, schema=LOG_SCHEMA, orient="row")
    _log(f"{df.height} file changes across {df['commit_hash'].n_unique()} commits.", quiet)

    data_dir = Path(output) / repo_name
    data_dir.mkdir(parents=True, exist_ok=True)
    df.write_parquet(data_dir / "log.parquet")

    return df, data_dir, repo_name


def ensure_busfactor_data(
    repo: str,
    extensions: str,
    workers: int,
    output: str,
    quiet: bool,
) -> tuple[pl.DataFrame, Path, str]:
    """Blame HEAD to get author-per-file data, including co-authors.

    Returns (dataframe, data_dir, repo_name). Schema: (file_path, author, line_count).
    Co-authors from commit trailers are counted as knowing the code too.
    """
    repo_path, repo_name = resolve_repo(repo)
    _log(f"Resolving {repo}...", quiet)
    repo_str = str(repo_path)

    exts = [e.strip() for e in extensions.split(",") if e.strip()] or None

    head = get_all_commits(repo_str)[-1][0]
    files = tracked_files(repo_str, head, exts)
    _log(f"Blaming {len(files)} files at HEAD ({head[:8]})...", quiet)

    coauthor_map = get_coauthor_map(repo_str)

    rows: list[tuple[str, str, int]] = []

    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(blame_lines_with_hash, repo_str, head, f): f for f in files}
        for i, fut in enumerate(as_completed(futs), 1):
            f = futs[fut]
            if not quiet and i % 50 == 0:
                _log(f"  [{i}/{len(files)}]", quiet)
            # Count lines per author (+ co-authors) per file
            author_counts: dict[str, int] = {}
            for commit_hash, _, author in fut.result():
                author_counts[author] = author_counts.get(author, 0) + 1
                for coauthor in coauthor_map.get(commit_hash, []):
                    author_counts[coauthor] = author_counts.get(coauthor, 0) + 1
            for author, count in author_counts.items():
                rows.append((f, author, count))

    df = pl.DataFrame(rows, schema={"file_path": pl.Utf8, "author": pl.Utf8, "line_count": pl.Int64}, orient="row")

    data_dir = Path(output) / repo_name
    data_dir.mkdir(parents=True, exist_ok=True)

    return df, data_dir, repo_name
