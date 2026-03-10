# gitplot

Git history visualizations from the command line.

## Quick Start

```bash
uvx gitplot sediment https://github.com/pola-rs/polars
uvx gitplot sediment /path/to/local/repo --samples 50 --workers 2
```

## Charts

### sediment

Stacked area chart showing code age layers over time. Each color band represents
lines of code from a specific time period, revealing how quickly code gets replaced.

Based on [gitcharts](https://github.com/koaning/gitcharts) by [Vincent D. Warmerdam](https://github.com/koaning).

```bash
gitplot sediment repo                              # default: quarter granularity, 100 samples, PNG
gitplot sediment repo --granularity year            # group by year instead of quarter
gitplot sediment repo --since 2023-01-01            # only show recent history
gitplot sediment repo --format svg                  # SVG output
gitplot sediment repo --samples 200                 # more data points (incremental)
open $(gitplot sediment repo --quiet)               # open the chart directly
```

Raw blame data is stored as parquet for incremental reuse. Re-running with more
samples only computes the delta. Changing visual settings (granularity, since,
format) is instant.

## Development

```bash
uv sync
make check
make test
```
