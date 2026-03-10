"""Survivors chart: stacked area chart showing code ownership by author over time."""

from dataclasses import dataclass
from datetime import datetime
from typing import Annotated, Literal

import altair as alt
import polars as pl
import tyro

from gitplot.collect import ensure_data


def render(df: pl.DataFrame, top_n: int, since: datetime | None, granularity: Literal["year", "quarter"]) -> alt.Chart:
    plot_df = df
    if since is not None:
        plot_df = plot_df.filter(pl.col("commit_date") >= since.timestamp())

    # Find top N authors by total line count across all commits
    top_authors = plot_df.group_by("author").len().sort("len", descending=True).head(top_n)["author"].to_list()

    # Bucket everyone else as "other"
    plot_df = plot_df.with_columns(
        pl.when(pl.col("author").is_in(top_authors)).then(pl.col("author")).otherwise(pl.lit("other")).alias("author"),
        pl.col("commit_date")
        .map_elements(lambda ts: datetime.fromtimestamp(ts), return_dtype=pl.Datetime)
        .alias("date"),
    )

    plot_df = plot_df.group_by(["date", "author"]).len().rename({"len": "line_count"}).sort(["date", "author"])

    # Order: biggest contributor at bottom, "other" on top
    author_order = [a for a in top_authors if a != "other"] + ["other"]

    # Add numeric rank for stack ordering (lower rank = bottom of stack)
    rank_map = {a: i for i, a in enumerate(author_order)}
    plot_df = plot_df.with_columns(
        pl.col("author").replace_strict(rank_map, default=len(rank_map)).alias("stack_order")
    )

    return (
        alt.Chart(plot_df)
        .mark_area()
        .encode(
            x=alt.X("date:T", title="Date"),
            y=alt.Y("line_count:Q", title="Lines of Code"),
            color=alt.Color(
                "author:N",
                scale=alt.Scale(scheme="tableau20"),
                sort=author_order,
                title="Author",
            ),
            order=alt.Order("stack_order:Q"),
            tooltip=["date:T", "author:N", "line_count:Q"],
        )
        .properties(width=800, height=500)
    )


@dataclass
class Survivors:
    """Stacked area chart of code ownership by author over time."""

    repo: Annotated[str, tyro.conf.Positional]
    """Repository URL (HTTPS/SSH) or local path."""

    samples: int = 100
    """Number of commits to sample evenly across history."""

    workers: int = 4
    """Max parallel commit-analysis workers."""

    top_n: int = 10
    """Show top N contributors, bucket the rest as 'other'."""

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


def run(args: Survivors) -> None:
    since = datetime.strptime(args.since, "%Y-%m-%d") if args.since else None
    df, data_dir, _ = ensure_data(args.repo, args.samples, args.workers, args.extensions, args.output, args.quiet)

    chart = render(df, args.top_n, since, "quarter")

    parts = [f"top{args.top_n}-s{args.samples}"]
    if args.since:
        parts.append(f"since{args.since}")
    filename = "-".join(parts) + f".{args.format}"

    chart_dir = data_dir / "survivors"
    chart_dir.mkdir(parents=True, exist_ok=True)
    out_path = chart_dir / filename
    chart.save(str(out_path))
    print(out_path)
