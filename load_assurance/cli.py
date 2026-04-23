"""CLI entrypoint — `la run --config pipeline.yaml`"""

from __future__ import annotations

import sys

import click
from rich.console import Console
from rich.table import Table
from rich import box

from load_assurance.checks.result import Severity
from load_assurance.config import load_config
from load_assurance.engine import RunReport, run

console = Console()

SEVERITY_STYLE = {
    Severity.OK: "green",
    Severity.WARNING: "yellow",
    Severity.FAILURE: "red bold",
}


def _render_report(report: RunReport) -> None:
    table = Table(
        title=f"Load Assurance — [bold]{report.pipeline}[/bold]",
        box=box.ROUNDED,
        show_lines=True,
    )
    table.add_column("Check", style="cyan", no_wrap=True)
    table.add_column("Status", justify="center")
    table.add_column("Message")

    for result in report.results:
        style = SEVERITY_STYLE[result.severity]
        table.add_row(
            result.check_name,
            f"[{style}]{result.severity.value}[/{style}]",
            result.message,
        )

    console.print()
    console.print(table)
    console.print()

    if report.passed:
        console.print("[green bold]✓ All checks passed.[/green bold]")
    else:
        console.print(
            f"[red bold]✗ {len(report.failures)} check(s) failed.[/red bold]"
        )
        for f in report.failures:
            console.print(f"  [red]• {f.check_name}:[/red] {f.message}")

    console.print()


@click.group()
def main() -> None:
    """load-assurance: metadata-first data load validation."""


@main.command()
@click.option(
    "--config",
    "-c",
    required=True,
    type=click.Path(exists=True),
    help="Path to pipeline YAML config.",
)
@click.option(
    "--fail-on-warning",
    is_flag=True,
    default=False,
    help="Exit with code 1 if any warnings are present.",
)
def run_cmd(config: str, fail_on_warning: bool) -> None:
    """Run load assurance checks for a pipeline."""
    try:
        cfg = load_config(config)
    except Exception as e:
        console.print(f"[red]Config error:[/red] {e}")
        sys.exit(2)

    try:
        report = run(cfg)
    except Exception as e:
        console.print(f"[red]Runtime error:[/red] {e}")
        sys.exit(2)

    _render_report(report)

    if not report.passed:
        sys.exit(1)
    if fail_on_warning and report.warnings:
        sys.exit(1)


# Register `run` as `la run`
main.add_command(run_cmd, name="run")