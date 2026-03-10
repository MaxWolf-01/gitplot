"""Git history visualizations from the command line.

Examples::

    gitplot sediment https://github.com/pola-rs/polars
    gitplot sediment /path/to/local/repo --samples 50 --workers 2
    gitplot sediment repo --granularity year --format svg
    open $(gitplot sediment repo --quiet)
"""

import tyro
from tyro.extras import SubcommandApp

from gitplot.sediment import Sediment
from gitplot.sediment import run as run_sediment

app = SubcommandApp()


@app.command(name="sediment")
def sediment(args: Sediment) -> None:
    """Stacked area chart of code age layers, like geological sediment."""
    run_sediment(args)


def main() -> None:
    app.cli(description=__doc__, config=(tyro.conf.OmitArgPrefixes,))
