"""Sediment chart: stacked area chart showing code age layers over time."""

from dataclasses import dataclass
from datetime import datetime
from typing import Annotated, Literal

import altair as alt
import polars as pl
import tyro

from gitplot.collect import ensure_data


def render(df: pl.DataFrame, granularity: Literal["year", "quarter"], since: datetime | None) -> alt.Chart:
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
    since = datetime.strptime(args.since, "%Y-%m-%d") if args.since else None
    df, data_dir, _ = ensure_data(args.repo, args.samples, args.workers, args.extensions, args.output, args.quiet)

    chart = render(df, args.granularity, since)

    parts = [f"{args.granularity[0]}{args.samples}"]
    if args.since:
        parts.append(f"since{args.since}")
    filename = "-".join(parts) + f".{args.format}"

    chart_dir = data_dir / "sediment"
    chart_dir.mkdir(parents=True, exist_ok=True)
    out_path = chart_dir / filename
    chart.save(str(out_path))
    print(out_path)
