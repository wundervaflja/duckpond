"""Dataset management commands for DuckPond - Refactored to use DuckLakeCatalogManager."""

import asyncio
import sys
from datetime import UTC, datetime
from pathlib import Path

import typer
from rich.console import Console

from duckpond.accounts.manager import AccountManager
from duckpond.catalog.manager import create_catalog_manager
from duckpond.cli.output import (
    confirm,
    print_dict,
    print_error,
    print_info,
    print_success,
    print_table,
    print_warning,
)
from duckpond.config import get_settings
from duckpond.db.session import create_session_factory, get_engine, get_session
from duckpond.logging_config import get_logger

app = typer.Typer(help="Manage datasets")
console = Console()
logger = get_logger(__name__)


@app.command()
def list(
    account: str = typer.Option(..., "--account", "-a", help="Account ID"),
) -> None:
    """List all datasets for a account (both files in storage and tables in catalog)."""

    async def _list() -> None:
        try:
            settings = get_settings()

            def _format_size(size_bytes: int) -> str:
                size = float(size_bytes)
                for unit in ["B", "KB", "MB", "GB", "TB"]:
                    if size < 1024.0:
                        return f"{size:.1f} {unit}"
                    size /= 1024.0
                return f"{size:.1f} PB"

            catalog_manager = await create_catalog_manager(account, settings=settings)
            response = await catalog_manager.list_datasets()
            catalog_datasets = {ds.name: ds for ds in response.datasets}

            from duckpond.storage import get_storage_backend

            if settings.default_storage_backend == "local":
                storage_path = Path(settings.local_storage_path) / "accounts"
                storage_backend = get_storage_backend(
                    backend_type="local", config={"path": str(storage_path)}
                )
            else:
                storage_backend = get_storage_backend(
                    backend_type=settings.default_storage_backend, config=None
                )

            tables_files = await storage_backend.list_files(
                prefix="tables", account_id=account, recursive=True
            )

            storage_datasets = {}
            for file_path in tables_files:
                if file_path.endswith(".parquet"):
                    parts = Path(file_path).parts
                    if len(parts) >= 2:
                        dataset_name = parts[1]

                        if dataset_name not in storage_datasets:
                            storage_datasets[dataset_name] = {
                                "files": [],
                                "total_size": 0,
                            }

                        storage_datasets[dataset_name]["files"].append(file_path)

                        try:
                            file_size = await storage_backend.get_file_size(file_path, account)
                            storage_datasets[dataset_name]["total_size"] += file_size
                        except Exception:
                            logger.error(f"Can't get file size for {tenant} - {file_path}")

            all_datasets = {}

            for name, ds in catalog_datasets.items():
                all_datasets[name] = {
                    "name": name,
                    "type": ds.type.value if hasattr(ds.type, "value") else str(ds.type),
                    "format": ds.format.value
                    if ds.format and hasattr(ds.format, "value")
                    else (ds.format or "—"),
                    "rows": ds.row_count,
                    "size": ds.size_bytes,
                    "created": ds.created_at,
                    "in_catalog": True,
                    "file_count": 0,
                }

            for name, info in storage_datasets.items():
                if name in all_datasets:
                    all_datasets[name]["file_count"] = len(info["files"])
                    if not all_datasets[name]["size"]:
                        all_datasets[name]["size"] = info["total_size"]
                else:
                    all_datasets[name] = {
                        "name": name,
                        "type": "file",
                        "format": "parquet",
                        "rows": None,
                        "size": info["total_size"],
                        "created": None,
                        "in_catalog": False,
                        "file_count": len(info["files"]),
                    }

            if not all_datasets:
                print_warning(f"No datasets found for account '{account}'")
                return

            table_data = []
            for ds in sorted(all_datasets.values(), key=lambda x: x["name"]):
                table_data.append(
                    {
                        "Name": ds["name"],
                        "Type": ds["type"],
                        "Files": str(ds["file_count"]) if ds["file_count"] > 0 else "—",
                        "Rows": f"{ds['rows']:,}" if ds["rows"] else "—",
                        "Size": _format_size(ds["size"]) if ds["size"] else "—",
                        "In Catalog": "✓" if ds["in_catalog"] else "✗",
                        "Created": ds["created"].strftime("%Y-%m-%d %H:%M:%S")
                        if ds["created"]
                        else "—",
                    }
                )

            print_table(
                table_data,
                title=f"Datasets for account '{account}'",
                columns=[
                    "Name",
                    "Type",
                    "Files",
                    "Rows",
                    "Size",
                    "In Catalog",
                    "Created",
                ],
            )
            print_info(f"Total datasets: {len(all_datasets)}")
            print_info(
                f"  - In catalog: {sum(1 for ds in all_datasets.values() if ds['in_catalog'])}"
            )
            print_info(
                f"  - Storage only: {sum(1 for ds in all_datasets.values() if not ds['in_catalog'])}"
            )

        except Exception as e:
            logger.error(f"Failed to list datasets: {e}")
            print_error(f"Failed to list datasets: {e}")
            raise typer.Exit(1)

    asyncio.run(_list())


@app.command()
def get(
    dataset_name: str = typer.Argument(..., help="Dataset name"),
    account: str = typer.Option(..., "--account", "-a", help="Account ID"),
) -> None:
    """Get detailed information about a dataset."""

    async def _get() -> None:
        try:
            settings = get_settings()
            catalog_manager = await create_catalog_manager(account, settings=settings)

            metadata = await catalog_manager.get_dataset_metadata(dataset_name)

            if not metadata:
                print_error(f"Dataset '{dataset_name}' not found for account '{account}'")
                raise typer.Exit(1)

            def _format_size(size_bytes: int) -> str:
                size = float(size_bytes)
                for unit in ["B", "KB", "MB", "GB", "TB"]:
                    if size < 1024.0:
                        return f"{size:.1f} {unit}"
                    size /= 1024.0
                return f"{size:.1f} PB"

            dataset_info = {
                "Name": metadata.name,
                "Type": metadata.type.value
                if hasattr(metadata.type, "value")
                else str(metadata.type),
                "Format": metadata.format.value
                if metadata.format and hasattr(metadata.format, "value")
                else (metadata.format or "—"),
                "Location": metadata.location or "—",
                "Row Count": f"{metadata.row_count:,}" if metadata.row_count else "—",
                "Size": _format_size(metadata.size_bytes) if metadata.size_bytes else "—",
                "Created": metadata.created_at.strftime("%Y-%m-%d %H:%M:%S")
                if metadata.created_at
                else "—",
                "Updated": metadata.updated_at.strftime("%Y-%m-%d %H:%M:%S")
                if metadata.updated_at
                else "—",
                "Description": metadata.description or "—",
            }

            print_dict(dataset_info, title="Dataset Information")

            if metadata.schema and metadata.schema.columns:
                schema_data = []
                for col in metadata.schema.columns:
                    schema_data.append(
                        {
                            "Column": col.name,
                            "Type": col.type,
                            "Nullable": "Yes" if col.nullable else "No",
                            "Default": col.default or "—",
                        }
                    )
                print_table(
                    schema_data,
                    title="Schema",
                    columns=["Column", "Type", "Nullable", "Default"],
                )

            if metadata.properties:
                print_dict(metadata.properties, title="Properties")

        except Exception as e:
            logger.error(f"Failed to get dataset: {e}")
            print_error(f"Failed to get dataset: {e}")
            raise typer.Exit(1)

    asyncio.run(_get())


@app.command()
def delete(
    dataset_name: str = typer.Argument(..., help="Dataset name"),
    account: str = typer.Option(..., "--account", "-a", help="Account ID"),
    force: bool = typer.Option(False, "--force", "-f", help="Skip confirmation"),
) -> None:
    """Delete a dataset."""

    async def _delete() -> None:
        try:
            settings = get_settings()
            catalog_manager = await create_catalog_manager(account, settings=settings)

            if not force:
                if not sys.stdin.isatty():
                    print_error(
                        "Cannot confirm deletion in non-interactive mode. Use --force to skip confirmation."
                    )
                    raise typer.Exit(1)

                confirmed = confirm(
                    f"Are you sure you want to delete dataset '{dataset_name}' from account '{account}'?",
                    default=False,
                )

                if not confirmed:
                    print_warning("Deletion cancelled")
                    raise typer.Exit(0)

            await catalog_manager.delete_dataset(dataset_name)
            print_success(f"Deleted dataset '{dataset_name}' from account '{account}'")

        except Exception as e:
            logger.error(f"Failed to delete dataset: {e}")
            print_error(f"Failed to delete dataset: {e}")
            raise typer.Exit(1)

    asyncio.run(_delete())


@app.command()
def upload(
    dataset_name: str = typer.Argument(..., help="Dataset name"),
    file_path: Path = typer.Argument(
        ...,
        help="File to upload (CSV or Parquet)",
        exists=True,
        file_okay=True,
        dir_okay=False,
        readable=True,
    ),
    account_id: str = typer.Option(..., "--account", "-a", help="Account ID"),
    catalog: bool = typer.Option(False, "--catalog", "-c", help="Register in catalog"),
) -> None:
    """Upload a file (CSV or Parquet) with optional catalog registration."""

    async def _upload() -> None:
        try:
            settings = get_settings()

            file_size_bytes = file_path.stat().st_size

            def format_size(size_bytes: int) -> str:
                size = float(size_bytes)
                for unit in ["B", "KB", "MB", "GB", "TB"]:
                    if size < 1024.0:
                        return f"{size:.1f} {unit}"
                    size /= 1024.0
                return f"{size:.1f} PB"

            print_info(f"Uploading file: {file_path}")
            print_info(f"Dataset: {dataset_name}")
            print_info(f"File size: {format_size(file_size_bytes)}")

            from duckpond.conversion.config import ConversionConfig
            from duckpond.conversion.factory import ConverterFactory

            if not ConverterFactory.is_supported(file_path):
                print_error(
                    f"Unsupported file format: {file_path.suffix}. "
                    f"Supported: .csv, .json, .jsonl, .parquet"
                )
                raise typer.Exit(1)

            from duckpond.storage import get_storage_backend

            engine = get_engine()
            session_factory = create_session_factory(engine)

            account = None
            async with get_session(session_factory) as session:
                manager = AccountManager(session)

                account = await manager.get_account_by_id(account_id)
            if not account or not account.storage_backend:
                if settings.default_storage_backend == "local":
                    storage_path = Path(settings.local_storage_path) / "accounts"
                    storage_backend = get_storage_backend(
                        backend_type="local", config={"path": str(storage_path)}
                    )
                else:
                    storage_backend = get_storage_backend(
                        backend_type=settings.default_storage_backend, config=None
                    )
            else:
                storage_backend = get_storage_backend(
                    backend_type=account.storage_backend, config=account.storage_config
                )

            try:
                remote_key = f"{dataset_name}/{file_path.stem}.parquet"
                conversion_config = ConversionConfig(
                    threads=4,
                    compression="snappy",
                    max_file_size_bytes=5 * 1024**3,
                    timeout_seconds=600,
                    validate_row_count=True,
                )

                result = await storage_backend.upload_file(
                    local_path=file_path,
                    remote_key=remote_key,
                    account_id=account_id,
                    metadata={
                        "original_filename": file_path.name,
                        "dataset_name": dataset_name,
                        "uploaded_at": datetime.now(UTC).isoformat(),
                        "file_size": str(file_size_bytes),
                    },
                    convert_to_parquet=True,
                    conversion_config=conversion_config,
                )

                print_success("File uploaded successfully")
                print_info(f"Location: {result['remote_path']}")

                if result.get("metrics"):
                    metrics = result["metrics"]
                    print_info("\n📊 Conversion Metrics:")
                    print_info(f"  Rows: {metrics['row_count']:,}")
                    print_info(f"  Source size: {format_size(metrics['source_size_bytes'])}")
                    print_info(f"  Parquet size: {format_size(metrics['dest_size_bytes'])}")
                    print_info(f"  Compression: {metrics['compression_ratio']:.2%}")
                    print_info(f"  Duration: {metrics['duration_seconds']:.2f}s")
                    print_info(f"  Throughput: {metrics['throughput_mbps']:.2f} MB/s")
                    print_info(f"  Schema fingerprint: {metrics['schema_fingerprint']}")

                if catalog:
                    catalog_manager = await create_catalog_manager(account_id, settings=settings)
                    result_path = result["remote_path"] if isinstance(result, dict) else result
                    if not account or not account.storage_backend:
                        if settings.default_storage_backend == "local":
                            storage_path = Path(settings.local_storage_path) / "accounts"
                            abs_parquet_path = storage_path / result_path
                        else:
                            abs_parquet_path = f"s3://{settings.s3_bucket}/{result_path}"
                    else:
                        abs_parquet_path = f"s3://{account.storage_config['bucket']}/{result_path}"

                    full_name = f'"{catalog_manager.catalog_name}".{dataset_name}'
                    create_sql = f"""
                        CREATE OR REPLACE TABLE {full_name} AS
                        SELECT * FROM read_parquet('{abs_parquet_path}')
                    """

                    await catalog_manager._execute_sql(create_sql)

                    print_success(f"Registered dataset '{dataset_name}' in catalog")

                    print_info("\nNext steps:")
                    print_info(
                        f"  - View dataset: duckpond dataset get {dataset_name} --account {account_id}"
                    )
                    print_info(
                        f"  - Query: duckpond query execute --sql 'SELECT * FROM {full_name} LIMIT 10' --account {account_id}"
                    )
                else:
                    print_info("File uploaded but not registered in catalog")
                    print_info(
                        f"To register later, use: duckpond dataset upload {dataset_name} {file_path} -a {account_id} --catalog"
                    )

            finally:
                pass

        except Exception as e:
            logger.error(f"Failed to upload file: {e}")
            print_error(f"Failed to upload file: {e}")
            raise typer.Exit(1)

    asyncio.run(_upload())


@app.command()
def register(
    dataset_name: str = typer.Argument(..., help="Dataset name from storage to register"),
    account: str = typer.Option(..., "--account", "-a", help="Account ID"),
    catalog: str = typer.Option("default", "--catalog", "-c", help="Catalog name"),
) -> None:
    """Register a dataset from storage as a view in the catalog.

    This creates a view that queries the Parquet files directly without copying data.
    The dataset should already exist in storage (use 'dataset list' to see available datasets).

    If multiple Parquet files exist for the dataset, they will all be included in the view.

    Examples:
        duckpond dataset register wines_info -a test

        duckpond dataset register wines_ducklake -a test -c default
    """

    async def _register() -> None:
        try:
            settings = get_settings()

            print_info(f"Looking for dataset: {dataset_name}")

            from duckpond.storage import get_storage_backend

            if settings.default_storage_backend == "local":
                storage_path = Path(settings.local_storage_path) / "accounts"
                storage_backend = get_storage_backend(
                    backend_type="local", config={"path": str(storage_path)}
                )
            else:
                storage_backend = get_storage_backend(
                    backend_type=settings.default_storage_backend, config=None
                )

            tables_files = await storage_backend.list_files(
                prefix=f"tables/{dataset_name}", account_id=account, recursive=True
            )

            parquet_files = [f for f in tables_files if f.endswith(".parquet")]

            if not parquet_files:
                print_error(f"No Parquet files found for dataset '{dataset_name}' in storage")
                print_info(f"Use 'duckpond dataset list -a {account}' to see available datasets")
                raise typer.Exit(1)

            print_info(f"Found {len(parquet_files)} Parquet file(s)")

            if len(parquet_files) == 1:
                file_path = str(
                    (
                        Path(settings.local_storage_path) / "accounts" / account / parquet_files[0]
                    ).absolute()
                )
            else:
                file_path = str(
                    (
                        Path(settings.local_storage_path)
                        / "accounts"
                        / account
                        / f"tables/{dataset_name}/*.parquet"
                    ).absolute()
                )

            print_info(f"File pattern: {file_path}")

            catalog_manager = await create_catalog_manager(
                account, catalog_name=catalog, settings=settings
            )

            await catalog_manager.register_parquet_file(
                dataset_name=dataset_name,
                file_path=file_path,
            )

            print_success(f"Registered '{dataset_name}' as view in catalog '{catalog}'")
            print_info(
                f"You can now query it with: duckpond query execute -a {account} -c {catalog} -s 'SELECT * FROM \"{catalog}\".{dataset_name}'"
            )

        except Exception as e:
            logger.error(f"Failed to register dataset: {e}")
            print_error(f"Failed to register dataset: {e}")
            raise typer.Exit(1)

    asyncio.run(_register())


@app.command()
def snapshots(
    dataset_name: str = typer.Argument(..., help="Dataset name"),
    account: str = typer.Option(..., "--account", "-a", help="Account ID"),
) -> None:
    """List all snapshots for a dataset."""

    async def _snapshots() -> None:
        try:
            settings = get_settings()
            catalog_manager = await create_catalog_manager(account, settings=settings)

            snapshots = await catalog_manager.list_snapshots(dataset_name)

            if not snapshots:
                print_warning(f"No snapshots found for dataset '{dataset_name}'")
                return

            table_data = []
            for snapshot in snapshots:
                table_data.append(
                    {
                        "Begin snapshot ID": snapshot.get("begin_snapshot", ""),
                        "End snapshot ID": snapshot.get("end_snapshot", ""),
                        "Begin time": snapshot.get("begin_time", ""),
                        "End time": snapshot.get("end_time", ""),
                        "Table path": snapshot.get("path", ""),
                    }
                )

            print_table(
                table_data,
                title=f"Snapshots for '{dataset_name}'",
                columns=[
                    "Begin snapshot ID",
                    "End snapshot ID",
                    "Begin time",
                    "End time",
                    "Table path",
                ],
            )
            print_info(f"Total snapshots: {len(snapshots)}")

        except Exception as e:
            logger.error(f"Failed to list snapshots: {e}")
            print_error(f"Failed to list snapshots: {e}")
            raise typer.Exit(1)

    asyncio.run(_snapshots())
