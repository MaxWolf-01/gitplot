"""Coupling chart: which files always change together?"""

from collections import Counter
from dataclasses import dataclass
from itertools import combinations
from typing import Annotated, Literal

import altair as alt
import polars as pl
import tyro

from gitplot.collect import ensure_log_data

DEFAULT_EXCLUDE = "*.lock,*-lock.json,*-lock.yaml,*.min.js,*.min.css"


def _shorten(path: str) -> str:
    """Shorten a file path for display — keep last 2 components."""
    parts = path.split("/")
    return "/".join(parts[-2:]) if len(parts) > 2 else path


def render(df: pl.DataFrame, top_n: int, min_commits: int, exclude: list[str]) -> alt.Chart:
    filtered = df
    for pattern in exclude:
        if pattern.startswith("*"):
            filtered = filtered.filter(~pl.col("file_path").str.ends_with(pattern[1:]))
        else:
            filtered = filtered.filter(~pl.col("file_path").str.contains(pattern, literal=True))

    files_per_commit = filtered.group_by("commit_hash").agg(pl.col("file_path").alias("files"))

    file_counts = filtered.group_by("file_path").agg(pl.col("commit_hash").n_unique().alias("count"))
    file_count_map = dict(zip(file_counts["file_path"].to_list(), file_counts["count"].to_list(), strict=True))

    pair_counts: Counter[tuple[str, str]] = Counter()
    for row in files_per_commit.iter_rows(named=True):
        files = sorted(row["files"])
        if len(files) > 30:
            continue
        for a, b in combinations(files, 2):
            pair_counts[(a, b)] += 1

    rows = []
    for (a, b), co in pair_counts.items():
        if co < min_commits:
            continue
        min_count = min(file_count_map.get(a, 1), file_count_map.get(b, 1))
        score = co / min_count
        rows.append({"file_a": a, "file_b": b, "co_commits": co, "coupling": round(score, 2)})

    if not rows:
        empty = pl.DataFrame({"file_a": ["(no pairs found)"], "file_b": [""], "coupling": [0.0], "co_commits": [0]})
        return alt.Chart(empty).mark_text(text="No coupled file pairs found").properties(width=700, height=100)

    pairs_df = pl.DataFrame(rows).sort("coupling", descending=True).head(top_n)

    # Collect the files that appear in top pairs, build a heatmap
    top_files_set: set[str] = set()
    for row in pairs_df.iter_rows(named=True):
        top_files_set.add(row["file_a"])
        top_files_set.add(row["file_b"])

    # Build symmetric matrix data for heatmap
    heatmap_rows = []
    pair_lookup = {}
    for row in pairs_df.iter_rows(named=True):
        pair_lookup[(row["file_a"], row["file_b"])] = row["coupling"]
        pair_lookup[(row["file_b"], row["file_a"])] = row["coupling"]

    top_files = sorted(top_files_set)
    for a in top_files:
        for b in top_files:
            if a == b:
                continue
            score = pair_lookup.get((a, b), 0.0)
            if score > 0:
                heatmap_rows.append({"file_a": _shorten(a), "file_b": _shorten(b), "coupling": score})

    if not heatmap_rows:
        heatmap_rows.append({"file_a": "", "file_b": "", "coupling": 0.0})

    heatmap_df = pl.DataFrame(heatmap_rows)

    return (
        alt.Chart(heatmap_df)
        .mark_rect()
        .encode(
            x=alt.X("file_a:N", title=None, axis=alt.Axis(labelAngle=-45)),
            y=alt.Y("file_b:N", title=None),
            color=alt.Color("coupling:Q", scale=alt.Scale(scheme="blues", domain=[0, 1]), title="Coupling"),
            tooltip=["file_a:N", "file_b:N", "coupling:Q"],
        )
        .properties(width=500, height=500)
    )


@dataclass
class Coupling:
    """Which files always change together — hidden dependencies."""

    repo: Annotated[str, tyro.conf.Positional]
    """Repository URL (HTTPS/SSH) or local path."""

    top_n: int = 20
    """Show top N most coupled file pairs."""

    min_commits: int = 5
    """Minimum co-commits to consider a pair."""

    exclude: str = DEFAULT_EXCLUDE
    """Comma-separated glob patterns to exclude (e.g. '*.lock,*-lock.json')."""

    output: str = "output"
    """Base output directory."""

    format: Literal["png", "svg", "json", "html"] = "png"
    """Output format."""

    quiet: bool = False
    """Suppress progress output."""


def run(args: Coupling) -> None:
    df, data_dir, _ = ensure_log_data(args.repo, args.output, args.quiet)

    exclude = [p.strip() for p in args.exclude.split(",") if p.strip()]
    chart = render(df, args.top_n, args.min_commits, exclude)

    filename = f"top{args.top_n}.{args.format}"

    chart_dir = data_dir / "coupling"
    chart_dir.mkdir(parents=True, exist_ok=True)
    out_path = chart_dir / filename
    chart.save(str(out_path))
    print(out_path)
