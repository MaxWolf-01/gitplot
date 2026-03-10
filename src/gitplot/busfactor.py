"""Bus factor chart: which files are only known by one person?"""

from dataclasses import dataclass
from typing import Annotated, Literal

import altair as alt
import polars as pl
import tyro

from gitplot.collect import ensure_busfactor_data


def render(df: pl.DataFrame, top_n: int, min_lines: int) -> alt.Chart:
    # Per file: number of unique authors, total lines
    file_stats = df.group_by("file_path").agg(
        pl.col("author").n_unique().alias("authors"),
        pl.col("line_count").sum().alias("total_lines"),
    )

    # Filter to files with enough code to matter
    file_stats = file_stats.filter(pl.col("total_lines") >= min_lines)

    # Sort by bus factor risk (fewest authors first), then by file size
    file_stats = file_stats.sort(["authors", "total_lines"], descending=[False, True]).head(top_n)

    return (
        alt.Chart(file_stats)
        .mark_bar()
        .encode(
            x=alt.X("total_lines:Q", title="Lines of Code"),
            y=alt.Y("file_path:N", title="File", sort=alt.EncodingSortField(field="authors", order="ascending")),
            color=alt.Color(
                "authors:O",
                scale=alt.Scale(scheme="redyellowgreen", domain=list(range(1, 8))),
                title="Unique Authors",
            ),
            tooltip=["file_path:N", "authors:Q", "total_lines:Q"],
        )
        .properties(width=700, height=max(top_n * 20, 300))
    )


@dataclass
class BusFactor:
    """Which files are only known by one person — knowledge risk."""

    repo: Annotated[str, tyro.conf.Positional]
    """Repository URL (HTTPS/SSH) or local path."""

    workers: int = 4
    """Max parallel blame workers."""

    top_n: int = 30
    """Show top N riskiest files."""

    min_lines: int = 20
    """Minimum lines of code to include a file."""

    extensions: str = ".py,.js,.ts,.java,.c,.cpp,.h,.go,.rs,.rb,.md"
    """Comma-separated file extensions to analyze (empty string for all)."""

    output: str = "output"
    """Base output directory."""

    format: Literal["png", "svg", "json", "html"] = "png"
    """Output format."""

    quiet: bool = False
    """Suppress progress output."""


def run(args: BusFactor) -> None:
    df, data_dir, _ = ensure_busfactor_data(args.repo, args.extensions, args.workers, args.output, args.quiet)

    chart = render(df, args.top_n, args.min_lines)

    filename = f"top{args.top_n}.{args.format}"

    chart_dir = data_dir / "busfactor"
    chart_dir.mkdir(parents=True, exist_ok=True)
    out_path = chart_dir / filename
    chart.save(str(out_path))
    print(out_path)
