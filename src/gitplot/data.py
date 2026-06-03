"""Parquet-based data persistence for incremental analysis."""

import json
from datetime import datetime
from pathlib import Path

import polars as pl

SCHEMA = {"commit_hash": pl.Utf8, "commit_date": pl.Float64, "line_timestamp": pl.Int64, "author": pl.Utf8}


def load_existing(data_dir: Path, include: str | None, exclude: str | None) -> pl.DataFrame | None:
    """Load existing blame data if the file filters match. Returns None on mismatch or missing."""
    parquet = data_dir / "blame.parquet"
    config = data_dir / "config.json"

    if not parquet.exists() or not config.exists():
        return None

    stored = json.loads(config.read_text())
    if stored.get("include") != include or stored.get("exclude") != exclude:
        return None

    return pl.read_parquet(parquet)


def save_data(data_dir: Path, df: pl.DataFrame, repo_source: str, include: str | None, exclude: str | None) -> None:
    data_dir.mkdir(parents=True, exist_ok=True)
    df.write_parquet(data_dir / "blame.parquet")
    (data_dir / "config.json").write_text(
        json.dumps(
            {
                "repo": repo_source,
                "include": include,
                "exclude": exclude,
                "updated": datetime.now().isoformat(),
            },
            indent=2,
        )
    )
