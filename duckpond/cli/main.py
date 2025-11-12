"""Main CLI entry point for DuckPond."""

import sys
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console

from duckpond import __version__
from duckpond.config import Settings, get_settings
from duckpond.exceptions import DuckPondError
from duckpond.logging_config import get_logger, setup_logging

app = typer.Typer(
    name="duckpond",
    help="DuckPond - Multi-account data platform with DuckDB and DuckLake",
    add_completion=True,
    rich_markup_mode="rich",
    no_args_is_help=False,
)

console = Console()
console_err = Console(stderr=True)
logger = get_logger(__name__)


class CLIState:
    """Global CLI state."""

    output_format: str = "table"
    verbose: bool = False
    quiet: bool = False
    settings: Optional[Settings] = None


state = CLIState()


def version_callback(value: bool) -> None:
    """Callback for --version flag."""
    if value:
        console.print(f"DuckPond version: {__version__}")
        console.print(f"Python: {sys.version.split()[0]}")
        raise typer.Exit(0)


@app.callback()
def main(
    ctx: typer.Context,
    version: bool = typer.Option(
        False,
        "--version",
        "-V",
        help="Show version information and exit",
        callback=version_callback,
        is_eager=True,
    ),
    config: Optional[Path] = typer.Option(
        None,
        "--config",
        "-c",
        help="Configuration file path",
        exists=True,
        file_okay=True,
        dir_okay=False,
        readable=True,
    ),
    output: str = typer.Option(
        "table",
        "--output",
        "-o",
        help="Output format: table, json, csv",
    ),
    verbose: bool = typer.Option(
        False,
        "--verbose",
        "-v",
        help="Enable verbose logging",
    ),
    quiet: bool = typer.Option(
        False,
        "--quiet",
        "-q",
        help="Suppress non-error output",
    ),
) -> None:
    """
    DuckPond - Multi-account data platform

    A high-performance data platform with file upload, streaming ingestion,
    and unified queries using DuckDB and DuckLake.
    """
    state.output_format = output
    state.verbose = verbose
    state.quiet = quiet

    if config:
        console.print(f"[yellow]Loading config from: {config}[/yellow]")

    state.settings = get_settings()

    if verbose:
        state.settings.log_level = "DEBUG"

    setup_logging()

    if output not in ["table", "json", "csv"]:
        console_err.print(f"[red]Error:[/red] Invalid output format: {output}")
        console_err.print("Valid formats: table, json, csv")
        raise typer.Exit(1)

    ctx.obj = state


def handle_error(error: Exception) -> None:
    """Handle CLI errors with user-friendly messages.

    Args:
        error: Exception to handle
    """
    if isinstance(error, DuckPondError):
        console_err.print(f"\n[red]Error:[/red] {error.message}")

        if state.verbose and error.context:
            console_err.print("\n[yellow]Context:[/yellow]")
            for key, value in error.context.items():
                console_err.print(f"  {key}: {value}")
    else:
        console_err.print(f"\n[red]Unexpected Error:[/red] {str(error)}")

        if state.verbose:
            import traceback

            console_err.print("\n[yellow]Traceback:[/yellow]")
            console_err.print(traceback.format_exc())

    raise typer.Exit(1)


try:
    from duckpond.cli import (
        account,
        api,
        config,
        dataset,
        db,
        init,
        query,
        stream,
    )

    # Register init function directly in main app
    app.command(name="init", help="Initialize DuckPond application")(init.init)
    
    # Register command groups
    app.add_typer(config.app, name="config", help="Configuration management")
    app.add_typer(account.app, name="accounts", help="Manage accounts")
    app.add_typer(dataset.app, name="dataset", help="Manage datasets")
    app.add_typer(query.app, name="query", help="Execute SQL queries")
    app.add_typer(stream.app, name="stream", help="Manage streams")

    app.add_typer(db.app, name="db", help="Database management commands")
    app.add_typer(api.app, name="api", help="API server management")
except ImportError as e:
    logger.debug(f"Failed to import CLI modules: {e}")
    pass


def main_cli() -> None:
    """Entry point for CLI application with error handling."""
    try:
        app()
    except DuckPondError as e:
        handle_error(e)
    except KeyboardInterrupt:
        console.print("\n[yellow]Operation cancelled by user[/yellow]")
        sys.exit(1)
    except Exception as e:
        handle_error(e)


if __name__ == "__main__":
    main_cli()
