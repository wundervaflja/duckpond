"""CLI commands for S3 secrets management."""

import logging
from pathlib import Path
from typing import Optional

import duckdb
import typer
from rich.console import Console
from rich.panel import Panel
from rich.syntax import Syntax
from rich.table import Table

from duckpond.catalog.secrets import (
    S3Credentials,
    S3Role,
    create_reader_secret,
    create_superuser_secret,
    create_writer_secret,
)
from duckpond.config import get_settings

app = typer.Typer(help="Manage S3 secrets for DuckDB access control")
console = Console()
logger = logging.getLogger(__name__)


@app.command()
def create(
    account_id: str = typer.Argument(..., help="Account ID"),
    role: S3Role = typer.Option(
        S3Role.SUPERUSER,
        "--role",
        "-r",
        help="Access control role (superuser/writer/reader)",
    ),
    s3_access_key_id: Optional[str] = typer.Option(
        None, "--s3-key-id", help="S3 access key ID (or use config)"
    ),
    s3_secret_access_key: Optional[str] = typer.Option(
        None, "--s3-secret-key", help="S3 secret access key (or use config)"
    ),
    s3_region: Optional[str] = typer.Option(None, "--s3-region", help="S3 region (or use config)"),
    s3_bucket: Optional[str] = typer.Option(None, "--s3-bucket", help="S3 bucket (or use config)"),
    s3_endpoint_url: Optional[str] = typer.Option(
        None, "--s3-endpoint", help="S3 endpoint URL (for MinIO)"
    ),
    s3_session_token: Optional[str] = typer.Option(
        None, "--s3-token", help="S3 session token (for temporary credentials)"
    ),
    output_file: Optional[Path] = typer.Option(
        None, "--output", "-o", help="Write SQL to file (default: execute immediately)"
    ),
) -> None:
    """
    Create DuckDB S3 secrets for access control.

    Generates and executes DuckDB secrets with S3 credentials for role-based
    access control. Different roles can have different IAM credentials with
    varying permissions.

    By default, secrets are created immediately in a DuckDB connection.
    Use --output to save SQL to a file instead.

    Examples:
        # Create superuser secret (executed immediately)
        duckpond secrets create acct-123 --role superuser \\
          --s3-key-id AKIA... --s3-secret-key secret... \\
          --s3-bucket my-bucket --s3-region us-east-1

        # Use credentials from config (executed immediately)
        duckpond secrets create acct-123 --role superuser

        # Save SQL to file instead of executing
        duckpond secrets create acct-123 -o secrets.sql

        # Create writer role secret (executed immediately)
        duckpond secrets create acct-123 --role writer
    """
    settings = get_settings()

    # Build S3 credentials from CLI args or config
    s3_key_id = s3_access_key_id or settings.catalog_s3_access_key_id
    s3_secret_key = s3_secret_access_key or settings.catalog_s3_secret_access_key
    s3_reg = s3_region or settings.s3_region
    s3_bkt = s3_bucket or settings.s3_bucket

    if not all([s3_key_id, s3_secret_key, s3_reg, s3_bkt]):
        console.print(
            "[red]Error:[/red] S3 credentials required.",
            style="bold red",
        )
        console.print("\nProvide via CLI args:")
        console.print(
            "  --s3-key-id AKIA... --s3-secret-key secret... --s3-region us-east-1 --s3-bucket my-bucket"
        )
        console.print("\nOr set in config:")
        console.print("  catalog:")
        console.print("    s3_access_key_id: AKIA...")
        console.print("    s3_secret_access_key: secret...")
        console.print("  storage:")
        console.print("    s3_bucket: my-bucket")
        console.print("    s3_region: us-east-1")
        raise typer.Exit(1)

    s3_creds = S3Credentials(
        access_key_id=s3_key_id,
        secret_access_key=s3_secret_key,
        region=s3_reg,
        endpoint_url=s3_endpoint_url or settings.s3_endpoint_url,
        session_token=s3_session_token or settings.catalog_s3_session_token,
    )

    # Create secret manager based on role
    if role == S3Role.SUPERUSER:
        manager = create_superuser_secret(
            account_id=account_id,
            s3_creds=s3_creds,
            s3_bucket=s3_bkt,
        )
    elif role == S3Role.WRITER:
        manager = create_writer_secret(
            account_id=account_id,
            s3_creds=s3_creds,
            s3_bucket=s3_bkt,
        )
    else:  # READER
        manager = create_reader_secret(
            account_id=account_id,
            s3_creds=s3_creds,
            s3_bucket=s3_bkt,
        )

    # Generate SQL
    sql = manager.generate_secret_sql()

    if output_file:
        # Write SQL to file
        try:
            output_file.parent.mkdir(parents=True, exist_ok=True)
            output_file.write_text(sql)

            console.print(
                f"✓ S3 secret SQL written to: [cyan]{output_file}[/cyan]", style="bold green"
            )

            # Show preview
            console.print("\n[bold]Preview:[/bold]")
            syntax = Syntax(sql, "sql", theme="monokai", line_numbers=True)
            console.print(syntax)

        except Exception as e:
            console.print(f"[red]Error writing file:[/red] {e}", style="bold red")
            raise typer.Exit(1)

    else:
        # Default: Execute secret immediately
        try:
            conn = duckdb.connect()
            conn.execute("INSTALL httpfs")
            conn.execute("LOAD httpfs")

            secret_name = manager.create_secret(conn)

            conn.close()

            console.print(
                Panel.fit(
                    f"✓ S3 secret created successfully!\n\n"
                    f"Secret name: [cyan]{secret_name}[/cyan]\n\n"
                    f"Secret is now available in DuckDB connections.\n"
                    f"DuckDB will automatically use it for S3 access.\n\n"
                    f"To save SQL instead: [cyan]duckpond secrets create {account_id} --output file.sql[/cyan]",
                    title=f"[green]{role.value.upper()} S3 Secret Created[/green]",
                    border_style="green",
                )
            )

        except Exception as e:
            console.print(f"[red]Error creating secret:[/red] {e}", style="bold red")
            raise typer.Exit(1)


@app.command()
def list_roles() -> None:
    """
    List available S3 access control roles and their permissions.

    Shows the three S3 access control roles (superuser, writer, reader) and
    what S3 permissions each role should have via IAM policies.
    """
    table = Table(title="S3 Access Control Roles", show_header=True, header_style="bold cyan")

    table.add_column("Role", style="yellow", width=15)
    table.add_column("S3 IAM Permissions", style="white", width=35)
    table.add_column("S3 Resource Path", style="green", width=40)
    table.add_column("Use Case", style="white", width=30)

    table.add_row(
        "SUPERUSER",
        "s3:ListBucket\ns3:GetObject\ns3:PutObject\ns3:DeleteObject",
        "arn:aws:s3:::bucket\narn:aws:s3:::bucket/*",
        "Full access to entire bucket\n(used by DuckPond)",
    )

    table.add_row(
        "WRITER",
        "s3:ListBucket\ns3:GetObject\ns3:PutObject\ns3:DeleteObject",
        "arn:aws:s3:::bucket\narn:aws:s3:::bucket/acct-123/schema/*",
        "Read/write access to\nspecific schema paths",
    )

    table.add_row(
        "READER",
        "s3:GetObject",
        "arn:aws:s3:::bucket\narn:aws:s3:::bucket/acct-123/schema/table/*",
        "Read-only access to\nspecific table paths",
    )

    console.print()
    console.print(table)
    console.print()

    console.print(
        Panel.fit(
            "[bold]AWS IAM Policy Example (Superuser):[/bold]\n\n"
            "[cyan]{\n"
            '  "Version": "2012-10-17",\n'
            '  "Statement": [{\n'
            '    "Effect": "Allow",\n'
            '    "Action": [\n'
            '      "s3:ListBucket",\n'
            '      "s3:GetObject",\n'
            '      "s3:PutObject",\n'
            '      "s3:DeleteObject"\n'
            "    ],\n"
            '    "Resource": [\n'
            '      "arn:aws:s3:::my-bucket",\n'
            '      "arn:aws:s3:::my-bucket/*"\n'
            "    ]\n"
            "  }]\n"
            "}[/cyan]\n\n"
            "[bold]Writer/Reader Policies:[/bold]\n"
            "Use more restrictive Resource paths:\n"
            "• Writer: [yellow]arn:aws:s3:::bucket/acct-123/schema/*[/yellow]\n"
            "• Reader: [yellow]arn:aws:s3:::bucket/acct-123/schema/table/*[/yellow]",
            title="[bold]AWS IAM Setup[/bold]",
            border_style="blue",
        )
    )


@app.command()
def example() -> None:
    """
    Show example configuration for S3 secrets.

    Displays a complete example config.yaml with all required S3 settings
    for DuckDB secret creation.
    """
    example_config = """
# S3 credentials for DuckDB secrets
catalog:
  # S3 credentials for catalog data access (superuser)
  s3_access_key_id: AKIAIOSFODNN7EXAMPLE
  s3_secret_access_key: wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
  # s3_session_token: optional_temporary_token

storage:
  default_backend: s3
  s3_bucket: my-duckpond-bucket
  s3_region: us-east-1
  # s3_endpoint_url: http://localhost:9000  # For MinIO
    """

    console.print(
        Panel.fit(
            "Add this to your [cyan]~/.duckpond/config.yaml[/cyan]:",
            title="[bold]Example Configuration[/bold]",
            border_style="blue",
        )
    )

    console.print()
    syntax = Syntax(example_config.strip(), "yaml", theme="monokai", line_numbers=True)
    console.print(syntax)

    console.print()
    console.print(
        Panel.fit(
            "[bold]Environment Variables Alternative:[/bold]\n\n"
            "You can also set via environment:\n\n"
            "[cyan]export CATALOG_S3_ACCESS_KEY_ID=AKIA...[/cyan]\n"
            "[cyan]export CATALOG_S3_SECRET_ACCESS_KEY=secret[/cyan]\n"
            "[cyan]export S3_BUCKET=my-bucket[/cyan]\n"
            "[cyan]export S3_REGION=us-east-1[/cyan]",
            title="[bold]Alternative: Environment Variables[/bold]",
            border_style="blue",
        )
    )

    console.print()
    console.print("[bold]Usage Examples:[/bold]")
    console.print()
    console.print("# Create superuser secret (full access)")
    console.print("[cyan]duckpond secrets create acct-123 --role superuser[/cyan]")
    console.print()
    console.print("# Create writer secret (schema-level access)")
    console.print("[cyan]duckpond secrets create acct-123 --role writer \\[/cyan]")
    console.print("[cyan]  --s3-key-id WRITER_KEY --s3-secret-key WRITER_SECRET[/cyan]")
    console.print()
    console.print("# Create reader secret (table-level access)")
    console.print("[cyan]duckpond secrets create acct-123 --role reader \\[/cyan]")
    console.print("[cyan]  --s3-key-id READER_KEY --s3-secret-key READER_SECRET[/cyan]")


if __name__ == "__main__":
    app()
