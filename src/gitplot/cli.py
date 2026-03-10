"""Git history visualizations from the command line.

Examples::

    gitplot sediment https://github.com/pola-rs/polars
    gitplot survivors /path/to/local/repo --top-n 5
    open $(gitplot sediment repo --quiet)
"""

import tyro
from tyro.extras import SubcommandApp

from gitplot.sediment import Sediment
from gitplot.sediment import run as run_sediment
from gitplot.survivors import Survivors
from gitplot.survivors import run as run_survivors

app = SubcommandApp()


@app.command(name="sediment")
def sediment(args: Sediment) -> None:
    """Stacked area chart of code age layers, like geological sediment."""
    run_sediment(args)


@app.command(name="survivors")
def survivors(args: Survivors) -> None:
    """Stacked area chart of code ownership by author over time."""
    run_survivors(args)


def main() -> None:
    app.cli(description=__doc__, config=(tyro.conf.OmitArgPrefixes,))
