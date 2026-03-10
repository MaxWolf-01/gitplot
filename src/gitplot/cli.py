"""Git history visualizations from the command line.

Examples::

    gitplot sediment https://github.com/pola-rs/polars
    gitplot survivors /path/to/local/repo --top-n 5
    gitplot churn repo --top-n 30
    gitplot coupling repo --min-commits 5
    gitplot busfactor repo
    gitplot pulse repo --window month
    open $(gitplot sediment repo --quiet)
"""

import tyro
from tyro.extras import SubcommandApp

from gitplot.busfactor import BusFactor
from gitplot.busfactor import run as run_busfactor
from gitplot.churn import Churn
from gitplot.churn import run as run_churn
from gitplot.coupling import Coupling
from gitplot.coupling import run as run_coupling
from gitplot.pulse import Pulse
from gitplot.pulse import run as run_pulse
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


@app.command(name="churn")
def churn(args: Churn) -> None:
    """Which files change the most — maintenance hotspots."""
    run_churn(args)


@app.command(name="coupling")
def coupling(args: Coupling) -> None:
    """Which files always change together — hidden dependencies."""
    run_coupling(args)


@app.command(name="busfactor")
def busfactor(args: BusFactor) -> None:
    """Which files are only known by one person — knowledge risk."""
    run_busfactor(args)


@app.command(name="pulse")
def pulse(args: Pulse) -> None:
    """Commit activity over time."""
    run_pulse(args)


def main() -> None:
    app.cli(description=__doc__, config=(tyro.conf.OmitArgPrefixes,))
