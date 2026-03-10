"""Churn chart: which files change the most?"""

from dataclasses import dataclass
from typing import Annotated, Literal

import altair as alt
import polars as pl
import tyro

from gitplot.collect import ensure_log_data

DEFAULT_EXCLUDE = "*.lock,*-lock.json,*-lock.yaml,*.min.js,*.min.css"


def render(df: pl.DataFrame, top_n: int, exclude: list[str]) -> alt.Chart:
    filtered = df
    for pattern in exclude:
        if pattern.startswith("*"):
            filtered = filtered.filter(~pl.col("file_path").str.ends_with(pattern[1:]))
        else:
            filtered = filtered.filter(~pl.col("file_path").str.contains(pattern, literal=True))

    churn = (
        filtered.group_by("file_path")
        .agg(
            (pl.col("insertions") + pl.col("deletions")).sum().alias("total_churn"),
            pl.col("commit_hash").n_unique().alias("commits"),
            pl.col("insertions").sum().alias("insertions"),
            pl.col("deletions").sum().alias("deletions"),
        )
        .sort("total_churn", descending=True)
        .head(top_n)
    )

    return (
        alt.Chart(churn)
        .mark_bar()
        .encode(
            x=alt.X("total_churn:Q", title="Total Lines Changed"),
            y=alt.Y("file_path:N", title="File", sort="-x"),
            color=alt.Color("commits:Q", scale=alt.Scale(scheme="oranges"), title="Commits"),
            tooltip=["file_path:N", "total_churn:Q", "commits:Q", "insertions:Q", "deletions:Q"],
        )
        .properties(width=700, height=max(top_n * 20, 300))
    )


@dataclass
class Churn:
    """Which files change the most — maintenance hotspots."""

    repo: Annotated[str, tyro.conf.Positional]
    """Repository URL (HTTPS/SSH) or local path."""

    top_n: int = 30
    """Show top N files by total churn."""

    exclude: str = DEFAULT_EXCLUDE
    """Comma-separated glob patterns to exclude (e.g. '*.lock,*-lock.json')."""

    output: str = "output"
    """Base output directory."""

    format: Literal["png", "svg", "json", "html"] = "png"
    """Output format."""

    quiet: bool = False
    """Suppress progress output."""


def run(args: Churn) -> None:
    df, data_dir, _ = ensure_log_data(args.repo, args.output, args.quiet)

    exclude = [p.strip() for p in args.exclude.split(",") if p.strip()]
    chart = render(df, args.top_n, exclude)

    filename = f"top{args.top_n}.{args.format}"

    chart_dir = data_dir / "churn"
    chart_dir.mkdir(parents=True, exist_ok=True)
    out_path = chart_dir / filename
    chart.save(str(out_path))
    print(out_path)
