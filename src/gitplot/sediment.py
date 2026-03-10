"""Sediment chart: stacked area chart showing code age layers over time.

For each sampled commit, runs git blame on every tracked file to determine
when each line was originally written. The result is a stacked area chart
where each color band represents code from a specific time period.
"""

import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Annotated, Literal

import altair as alt
import polars as pl
import tyro

from gitplot.data import SCHEMA, load_existing, save_data
from gitplot.git import blame_timestamps, get_all_commits, resolve_repo, sample_evenly, tracked_files


def _log(msg: str, quiet: bool = False) -> None:
    if not quiet:
        print(msg, file=sys.stderr)


def _analyze_commit(
    repo: str,
    commit: str,
    commit_date: datetime,
    exts: list[str] | None,
    file_workers: int = 4,
) -> list[tuple[str, float, int]]:
    """Blame every tracked file at a commit. Returns (hash, commit_ts, line_ts) rows."""
    files = tracked_files(repo, commit, exts)
    rows: list[tuple[str, float, int]] = []
    ct = commit_date.timestamp()

    with ThreadPoolExecutor(max_workers=file_workers) as ex:
        futs = {ex.submit(blame_timestamps, repo, commit, f): f for f in files}
        for fut in as_completed(futs):
            for ts in fut.result():
                rows.append((commit, ct, ts))
    return rows


def collect(
    repo_path: str,
    sampled: list[tuple[str, datetime]],
    existing: pl.DataFrame | None,
    exts: list[str] | None,
    workers: int,
    quiet: bool,
) -> pl.DataFrame:
    """Collect blame data, skipping already-analyzed commits."""
    already_done: set[str] = set()
    if existing is not None:
        already_done = set(existing["commit_hash"].unique().to_list())

    todo = [(h, d) for h, d in sampled if h not in already_done]

    if not todo:
        _log("All sampled commits already analyzed.", quiet)
        return existing  # type: ignore[return-value]

    _log(f"{len(todo)} commits to analyze.", quiet)
    new_rows: list[tuple[str, float, int]] = []
    total = len(todo)

    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(_analyze_commit, repo_path, h, d, exts): h for h, d in todo}
        for i, fut in enumerate(as_completed(futs), 1):
            h = futs[fut]
            _log(f"  [{i}/{total}] {h[:8]}", quiet)
            new_rows.extend(fut.result())

    new_df = pl.DataFrame(new_rows, schema=SCHEMA, orient="row")
    return pl.concat([existing, new_df]) if existing is not None else new_df


def render(df: pl.DataFrame, granularity: Literal["year", "quarter"], since: datetime | None) -> alt.Chart:
    """Build the stacked area chart from blame data."""

    def period(ts: int) -> str:
        dt = datetime.fromtimestamp(ts)
        if granularity == "year":
            return str(dt.year)
        return f"{dt.year}-Q{(dt.month - 1) // 3 + 1}"

    plot_df = df
    if since is not None:
        plot_df = plot_df.filter(pl.col("commit_date") >= since.timestamp())

    plot_df = (
        plot_df.with_columns(
            pl.col("commit_date")
            .map_elements(lambda ts: datetime.fromtimestamp(ts), return_dtype=pl.Datetime)
            .alias("date"),
            pl.col("line_timestamp").map_elements(period, return_dtype=pl.Utf8).alias("period"),
        )
        .group_by(["date", "period"])
        .len()
        .rename({"len": "line_count"})
        .sort(["date", "period"])
    )

    label = "Year Added" if granularity == "year" else "Quarter Added"
    return (
        alt.Chart(plot_df)
        .mark_area()
        .encode(
            x=alt.X("date:T", title="Date"),
            y=alt.Y("line_count:Q", title="Lines of Code"),
            color=alt.Color("period:O", scale=alt.Scale(scheme="viridis"), title=label),
            order=alt.Order("period:O"),
            tooltip=["date:T", "period:O", "line_count:Q"],
        )
        .properties(width=800, height=500)
    )


@dataclass
class Sediment:
    """Stacked area chart of code age layers, like geological sediment."""

    repo: Annotated[str, tyro.conf.Positional]
    """Repository URL (HTTPS/SSH) or local path."""

    samples: int = 100
    """Number of commits to sample evenly across history."""

    workers: int = 4
    """Max parallel commit-analysis workers."""

    granularity: Literal["year", "quarter"] = "quarter"
    """Time bucket for grouping lines by age."""

    extensions: str = ".py,.js,.ts,.java,.c,.cpp,.h,.go,.rs,.rb,.md"
    """Comma-separated file extensions to analyze (empty string for all)."""

    since: str | None = None
    """Only render commits after this date (YYYY-MM-DD). Does not affect stored data."""

    output: str = "output"
    """Base output directory."""

    format: Literal["png", "svg", "json", "html"] = "png"
    """Output format."""

    quiet: bool = False
    """Suppress progress output."""


def run(args: Sediment) -> None:
    repo_path, repo_name = resolve_repo(args.repo)
    if not args.quiet:
        _log(f"Resolving {args.repo}...", args.quiet)
    repo_str = str(repo_path)

    exts = [e.strip() for e in args.extensions.split(",") if e.strip()] or None
    exts_key = args.extensions.strip()
    since = datetime.strptime(args.since, "%Y-%m-%d") if args.since else None

    data_dir = Path(args.output) / repo_name / "sediment"
    existing = load_existing(data_dir, exts_key)

    all_commits = get_all_commits(repo_str)
    sampled = sample_evenly(all_commits, args.samples)
    _log(f"{len(all_commits)} commits in repo, sampling {len(sampled)}.", args.quiet)

    df = collect(repo_str, sampled, existing, exts, args.workers, args.quiet)
    save_data(data_dir, df, args.repo, exts_key)
    _log(f"Data saved: {data_dir / 'blame.parquet'}", args.quiet)

    chart = render(df, args.granularity, since)

    parts = [f"{args.granularity[0]}{args.samples}"]
    if args.since:
        parts.append(f"since{args.since}")
    filename = "-".join(parts) + f".{args.format}"

    out_path = data_dir / filename
    chart.save(str(out_path))
    print(out_path)
