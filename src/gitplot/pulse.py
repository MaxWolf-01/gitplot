"""Pulse chart: commit activity over time."""

from dataclasses import dataclass
from datetime import datetime
from typing import Annotated, Literal

import altair as alt
import polars as pl
import tyro

from gitplot.collect import ensure_log_data


def render(df: pl.DataFrame, window: Literal["week", "month"], since: datetime | None) -> alt.Chart:
    commits = (
        df.select("commit_hash", "commit_date", "author")
        .unique()
        .with_columns(
            pl.col("commit_date")
            .map_elements(lambda ts: datetime.fromtimestamp(ts), return_dtype=pl.Datetime)
            .alias("date")
        )
    )

    if since is not None:
        commits = commits.filter(pl.col("commit_date") >= since.timestamp())

    if window == "week":
        commits = commits.with_columns(pl.col("date").dt.truncate("1w").alias("period"))
    else:
        commits = commits.with_columns(pl.col("date").dt.truncate("1mo").alias("period"))

    by_period = commits.group_by("period").len().rename({"len": "commits"}).sort("period")

    return (
        alt.Chart(by_period)
        .mark_bar()
        .encode(
            x=alt.X("period:T", title="Date"),
            y=alt.Y("commits:Q", title="Commits"),
            tooltip=["period:T", "commits:Q"],
        )
        .properties(width=800, height=400)
    )


@dataclass
class Pulse:
    """Commit activity over time."""

    repo: Annotated[str, tyro.conf.Positional]
    """Repository URL (HTTPS/SSH) or local path."""

    window: Literal["week", "month"] = "week"
    """Aggregation window."""

    since: str | None = None
    """Only show activity after this date (YYYY-MM-DD)."""

    output: str = "output"
    """Base output directory."""

    format: Literal["png", "svg", "json", "html"] = "png"
    """Output format."""

    quiet: bool = False
    """Suppress progress output."""


def run(args: Pulse) -> None:
    since = datetime.strptime(args.since, "%Y-%m-%d") if args.since else None
    df, data_dir, _ = ensure_log_data(args.repo, args.output, args.quiet)

    chart = render(df, args.window, since)

    parts = [args.window]
    if args.since:
        parts.append(f"since{args.since}")
    filename = "-".join(parts) + f".{args.format}"

    chart_dir = data_dir / "pulse"
    chart_dir.mkdir(parents=True, exist_ok=True)
    out_path = chart_dir / filename
    chart.save(str(out_path))
    print(out_path)
