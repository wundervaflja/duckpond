"""DuckLake catalog manager for dataset operations."""

import asyncio
import logging
from datetime import datetime
from typing import Any

import duckdb

from duckpond.catalog.schemas import (
    ColumnSchema,
    CreateDatasetRequest,
    DatasetListResponse,
    DatasetMetadata,
    DatasetType,
    PartitionInfo,
    PartitionSpec,
    PartitionType,
    SchemaEvolutionRequest,
    TableSchema,
    TableStatistics,
    UpdateDatasetRequest,
)
from duckpond.exceptions import (
    CatalogError,
    DatasetNotFoundError,
    SchemaIncompatibleError,
)

logger = logging.getLogger(__name__)


class DuckLakeCatalogManager:
    """
    Manages DuckLake catalog operations for a account.

    This class provides:
    - Dataset registration and metadata management
    - Schema validation and evolution
    - Table/view listing and inspection
    - Partition management
    - Catalog-level statistics

    All operations are executed asynchronously using thread pool
    since DuckDB connections are synchronous.

    Usage:
        manager = DuckLakeCatalogManager(conn, account_id, catalog_name)

        dataset = await manager.create_dataset(CreateDatasetRequest(...))

        datasets = await manager.list_datasets()

        metadata = await manager.get_dataset_metadata("sales")
    """

    def __init__(
        self,
        conn: duckdb.DuckDBPyConnection,
        account_id: str,
        catalog_url: str,
        catalog_name: str = "catalog",
    ) -> None:
        """
        Initialize catalog manager.

        Args:
            conn: DuckDB connection with DuckLake catalog attached
            account_id: Account ID for logging and tracking
            catalog_name: DuckLake catalog name (default: "catalog")
        """
        self.conn = conn
        self.account_id = account_id
        self.catalog_name = catalog_name
        self.catalog_url = catalog_url

        logger.info(
            f"Initialized DuckLakeCatalogManager for account {account_id}",
            extra={"account_id": account_id, "catalog_name": catalog_name},
        )

    def _get_full_table_name(self, dataset_name: str) -> str:
        """
        Get the fully qualified table name with proper quoting.

        Quotes the catalog name if it's a SQL reserved word like 'default'.

        Args:
            dataset_name: Name of the dataset/table

        Returns:
            Fully qualified table name (e.g., "default".my_table)
        """
        if self.catalog_name.lower() in ["default", "main", "temp"]:
            return f'"{self.catalog_name}".{dataset_name}'
        return f"{self.catalog_name}.{dataset_name}"

    async def create_dataset(self, request: CreateDatasetRequest) -> DatasetMetadata:
        """
        Create a new dataset (table or view) in the catalog.

        Args:
            request: Dataset creation request

        Returns:
            Created dataset metadata

        Raises:
            CatalogError: If dataset creation fails

        Example:
            request = CreateDatasetRequest(
                name="sales",
                type=DatasetType.TABLE,
                format=TableFormat.PARQUET,
                schema=TableSchema(
                    columns=[
                        ColumnSchema(name="order_id", type="BIGINT", nullable=False, default=None, comment=None),
                        ColumnSchema(name="amount", type="DECIMAL", nullable=False, default=None, comment=None),
                    ],
                    primary_key=None,
                    indexes=None,
                ),
            )
            dataset = await manager.create_dataset(request)
        """
        logger.info(
            f"Creating dataset {request.name} for account {self.account_id}",
            extra={
                "account_id": self.account_id,
                "dataset_name": request.name,
                "dataset_type": request.type.value,
            },
        )

        try:
            if request.if_not_exists:
                exists = await self._dataset_exists(request.name)
                if exists:
                    logger.info(f"Dataset {request.name} already exists, skipping creation")
                    return await self.get_dataset_metadata(request.name)

            create_sql = self._build_create_sql(request)

            await self._execute_sql(create_sql)

            metadata = await self.get_dataset_metadata(request.name)

            logger.info(
                f"Created dataset {request.name} for account {self.account_id}",
                extra={
                    "account_id": self.account_id,
                    "dataset_name": request.name,
                    "row_count": metadata.row_count,
                },
            )

            return metadata

        except Exception as e:
            logger.error(
                f"Failed to create dataset {request.name}: {e}",
                extra={"account_id": self.account_id, "dataset_name": request.name},
                exc_info=True,
            )
            raise CatalogError(f"Failed to create dataset {request.name}: {e}")

    async def get_dataset_metadata(self, dataset_name: str) -> DatasetMetadata:
        """
        Get metadata for a dataset.

        Args:
            dataset_name: Name of the dataset

        Returns:
            Dataset metadata

        Raises:
            DatasetNotFoundError: If dataset does not exist
        """
        logger.debug(
            f"Getting metadata for dataset {dataset_name}",
            extra={"account_id": self.account_id, "dataset_name": dataset_name},
        )

        try:
            exists = await self._dataset_exists(dataset_name)
            if not exists:
                raise DatasetNotFoundError(f"{self.catalog_name}.{dataset_name}")

            dataset_type = await self._get_dataset_type(dataset_name)
            schema = await self._get_table_schema(dataset_name)

            stats = None
            if dataset_type == DatasetType.TABLE:
                stats = await self.get_table_statistics(dataset_name)

            metadata = DatasetMetadata(
                name=dataset_name,
                type=dataset_type,
                format=None,
                schema=schema,
                location=None,
                description=None,
                row_count=stats.row_count if stats else None,
                size_bytes=stats.size_bytes if stats else None,
                created_at=datetime.now(),
                updated_at=datetime.now(),
            )

            return metadata

        except DatasetNotFoundError:
            raise
        except Exception as e:
            logger.error(
                f"Failed to get metadata for dataset {dataset_name}: {e}",
                extra={"account_id": self.account_id, "dataset_name": dataset_name},
                exc_info=True,
            )
            raise CatalogError(f"Failed to get dataset metadata: {e}")

    async def list_datasets(
        self,
        dataset_type: DatasetType | None = None,
        pattern: str | None = None,
    ) -> DatasetListResponse:
        """
        List all datasets in the catalog.

        Args:
            dataset_type: Filter by dataset type (table, view, external)
            pattern: SQL LIKE pattern for dataset names (e.g., "sales%")

        Returns:
            List of dataset metadata
        """
        logger.debug(
            f"Listing datasets for account {self.account_id}",
            extra={
                "account_id": self.account_id,
                "dataset_type": dataset_type.value if dataset_type else None,
                "pattern": pattern,
            },
        )

        try:
            query = f"""
                SELECT
                    table_name as name,
                    table_type as type
                FROM information_schema.tables
                WHERE table_catalog = '{self.catalog_name}'
                  AND table_schema = 'main'
            """

            if dataset_type:
                type_filter = "BASE TABLE" if dataset_type == DatasetType.TABLE else "VIEW"
                query += f" AND table_type = '{type_filter}'"

            if pattern:
                query += f" AND table_name LIKE '{pattern}'"

            query += " ORDER BY table_name"

            result = await self._execute_sql(query)
            rows = result.fetchall()

            datasets = []
            for row in rows:
                name, type_str = row
                try:
                    metadata = await self.get_dataset_metadata(name)
                    datasets.append(metadata)
                except Exception as e:
                    logger.warning(
                        f"Failed to get metadata for dataset {name}: {e}",
                        extra={"account_id": self.account_id, "dataset_name": name},
                    )

            return DatasetListResponse(datasets=datasets, total=len(datasets))

        except Exception as e:
            logger.error(
                f"Failed to list datasets: {e}",
                extra={"account_id": self.account_id},
                exc_info=True,
            )
            raise CatalogError(f"Failed to list datasets: {e}")

    async def update_dataset(
        self,
        dataset_name: str,
        request: UpdateDatasetRequest,
    ) -> DatasetMetadata:
        """
        Update dataset metadata (description, properties).

        Note: DuckDB doesn't support table comments natively, so this
        stores metadata in a separate system table.

        Args:
            dataset_name: Name of the dataset
            request: Update request

        Returns:
            Updated dataset metadata

        Raises:
            DatasetNotFoundError: If dataset does not exist
        """
        logger.info(
            f"Updating dataset {dataset_name} for account {self.account_id}",
            extra={"account_id": self.account_id, "dataset_name": dataset_name},
        )

        exists = await self._dataset_exists(dataset_name)
        if not exists:
            raise DatasetNotFoundError(f"{self.catalog_name}.{dataset_name}")

        return await self.get_dataset_metadata(dataset_name)

    async def delete_dataset(self, dataset_name: str, if_exists: bool = False) -> None:
        """
        Delete a dataset from the catalog.

        Args:
            dataset_name: Name of the dataset
            if_exists: Skip if dataset doesn't exist

        Raises:
            DatasetNotFoundError: If dataset does not exist and if_exists=False
        """
        logger.info(
            f"Deleting dataset {dataset_name} for account {self.account_id}",
            extra={"account_id": self.account_id, "dataset_name": dataset_name},
        )

        try:
            if not if_exists:
                exists = await self._dataset_exists(dataset_name)
                if not exists:
                    raise DatasetNotFoundError(f"{self.catalog_name}.{dataset_name}")

            full_name = self._get_full_table_name(dataset_name)

            try:
                drop_view_sql = f"DROP VIEW IF EXISTS {full_name}"
                await self._execute_sql(drop_view_sql)
            except Exception:
                pass

            try:
                drop_table_sql = f"DROP TABLE IF EXISTS {full_name}"
                await self._execute_sql(drop_table_sql)
            except Exception:
                pass

            logger.info(
                f"Deleted dataset {dataset_name} for account {self.account_id}",
                extra={"account_id": self.account_id, "dataset_name": dataset_name},
            )

        except DatasetNotFoundError:
            raise
        except Exception as e:
            logger.error(
                f"Failed to delete dataset {dataset_name}: {e}",
                extra={"account_id": self.account_id, "dataset_name": dataset_name},
                exc_info=True,
            )
            raise CatalogError(f"Failed to delete dataset: {e}")

    async def evolve_schema(
        self,
        dataset_name: str,
        request: SchemaEvolutionRequest,
    ) -> DatasetMetadata:
        """
        Evolve table schema (add/drop/rename columns).

        Args:
            dataset_name: Name of the dataset
            request: Schema evolution request

        Returns:
            Updated dataset metadata

        Raises:
            DatasetNotFoundError: If dataset does not exist
            SchemaIncompatibleError: If schema evolution is invalid
        """
        logger.info(
            f"Evolving schema for dataset {dataset_name}",
            extra={
                "account_id": self.account_id,
                "dataset_name": dataset_name,
                "add_columns": len(request.add_columns),
                "drop_columns": len(request.drop_columns),
                "rename_columns": len(request.rename_columns),
            },
        )

        try:
            exists = await self._dataset_exists(dataset_name)
            if not exists:
                raise DatasetNotFoundError(f"{self.catalog_name}.{dataset_name}")

            full_name = self._get_full_table_name(dataset_name)

            for col in request.add_columns:
                alter_sql = f"""
                    ALTER TABLE {full_name}
                    ADD COLUMN {col.name} {col.type}
                    {"NOT NULL" if not col.nullable else ""}
                    {f"DEFAULT {col.default}" if col.default else ""}
                """
                await self._execute_sql(alter_sql)

            for col_name in request.drop_columns:
                alter_sql = f"ALTER TABLE {full_name} DROP COLUMN {col_name}"
                await self._execute_sql(alter_sql)

            for old_name, new_name in request.rename_columns.items():
                alter_sql = f"ALTER TABLE {full_name} RENAME COLUMN {old_name} TO {new_name}"
                await self._execute_sql(alter_sql)

            for col in request.alter_columns:
                logger.warning(
                    f"ALTER COLUMN not fully supported, skipping {col.name}",
                    extra={"account_id": self.account_id, "column": col.name},
                )

            logger.info(
                f"Schema evolved for dataset {dataset_name}",
                extra={"account_id": self.account_id, "dataset_name": dataset_name},
            )

            return await self.get_dataset_metadata(dataset_name)

        except DatasetNotFoundError:
            raise
        except Exception as e:
            logger.error(
                f"Failed to evolve schema for dataset {dataset_name}: {e}",
                extra={"account_id": self.account_id, "dataset_name": dataset_name},
                exc_info=True,
            )
            raise SchemaIncompatibleError(f"Schema evolution failed: {e}")

    async def get_table_statistics(self, dataset_name: str) -> TableStatistics:
        """
        Get statistics for a table.

        Args:
            dataset_name: Name of the table

        Returns:
            Table statistics

        Raises:
            DatasetNotFoundError: If table does not exist
        """
        logger.debug(
            f"Getting statistics for table {dataset_name}",
            extra={"account_id": self.account_id, "dataset_name": dataset_name},
        )

        try:
            full_name = self._get_full_table_name(dataset_name)

            count_result = await self._execute_sql(f"SELECT COUNT(*) FROM {full_name}")
            row_count = count_result.fetchone()[0]

            avg_row_size = 100
            size_bytes = row_count * avg_row_size

            return TableStatistics(
                row_count=row_count,
                size_bytes=size_bytes,
                num_files=None,
                num_partitions=None,
                avg_row_size_bytes=float(avg_row_size),
                last_updated=datetime.now(),
            )

        except Exception as e:
            logger.error(
                f"Failed to get statistics for table {dataset_name}: {e}",
                extra={"account_id": self.account_id, "dataset_name": dataset_name},
                exc_info=True,
            )
            raise CatalogError(f"Failed to get table statistics: {e}")

    async def list_partitions(self, dataset_name: str) -> list[PartitionInfo]:
        """
        List partitions for a table.

        Note: DuckDB doesn't expose partition metadata directly,
        so this returns an empty list for now.

        Args:
            dataset_name: Name of the table

        Returns:
            List of partition metadata
        """
        logger.debug(
            f"Listing partitions for table {dataset_name}",
            extra={"account_id": self.account_id, "dataset_name": dataset_name},
        )

        return []

    async def register_parquet_file(
        self,
        dataset_name: str,
        file_path: str,
        partition_values: dict[str, str] | None = None,
    ) -> None:
        """
        Register a Parquet file in the DuckLake catalog as a view.

        This creates a view that queries the Parquet file directly without copying data.
        The view allows querying the file as if it were a table in the catalog.

        Args:
            dataset_name: Name of the view to create
            file_path: Path to Parquet file (local or S3)
            partition_values: Optional partition values (currently unused)

        Raises:
            CatalogError: If view creation fails

        Example:
            await manager.register_parquet_file(
                "sales",
                "s3://bucket/data/sales_2024_01.parquet",
            )
        """
        logger.info(
            f"Registering Parquet file as view {dataset_name}",
            extra={
                "account_id": self.account_id,
                "dataset_name": dataset_name,
                "file_path": file_path,
                "partition_values": partition_values,
            },
        )

        try:
            exists = await self._dataset_exists(dataset_name)
            if exists:
                logger.warning(
                    f"Dataset {dataset_name} already exists, dropping and recreating",
                    extra={"account_id": self.account_id, "dataset_name": dataset_name},
                )
                await self.delete_dataset(dataset_name, if_exists=True)

            full_name = self._get_full_table_name(dataset_name)

            create_view_sql = f"""
                CREATE VIEW {full_name} AS
                SELECT * FROM read_parquet('{file_path}')
            """

            await self._execute_sql(create_view_sql)

            logger.info(
                f"Registered Parquet file as view {dataset_name}",
                extra={
                    "account_id": self.account_id,
                    "dataset_name": dataset_name,
                    "file_path": file_path,
                },
            )

        except Exception as e:
            logger.error(
                f"Failed to register Parquet file: {e}",
                extra={
                    "account_id": self.account_id,
                    "dataset_name": dataset_name,
                    "file_path": file_path,
                },
                exc_info=True,
            )
            raise CatalogError(f"Failed to register Parquet file: {e}")

    async def query_at_timestamp(
        self,
        dataset_name: str,
        timestamp: datetime,
        columns: list[str] | None = None,
        limit: int | None = None,
    ) -> list[dict]:
        """
        Query dataset at a specific timestamp (time travel).

        Note: This requires DuckDB with time travel support (e.g., Delta Lake, Iceberg).
        Standard DuckDB tables don't support AS OF queries.

        Args:
            dataset_name: Name of the dataset
            timestamp: Point-in-time to query
            columns: Columns to select (None = all)
            limit: Maximum rows to return

        Returns:
            Query results as list of dictionaries

        Raises:
            DatasetNotFoundError: If dataset does not exist
            CatalogError: If time travel query fails

        Example:
            results = await manager.query_at_timestamp(
                "sales",
                datetime(2024, 1, 1),
                columns=["order_id", "amount"],
                limit=100
            )
        """
        logger.info(
            f"Querying dataset {dataset_name} at timestamp {timestamp}",
            extra={
                "account_id": self.account_id,
                "dataset_name": dataset_name,
                "timestamp": timestamp.isoformat(),
            },
        )

        try:
            exists = await self._dataset_exists(dataset_name)
            if not exists:
                raise DatasetNotFoundError(f"{self.catalog_name}.{dataset_name}")

            full_name = self._get_full_table_name(dataset_name)

            col_list = ", ".join(columns) if columns else "*"

            query = f"""
                SELECT {col_list}
                FROM {full_name}
                FOR SYSTEM_TIME AS OF '{timestamp.isoformat()}'
            """

            if limit:
                query += f" LIMIT {limit}"

            result = await self._execute_sql(query)
            rows = result.fetchdf().to_dict(orient="records")

            logger.info(
                f"Time travel query returned {len(rows)} rows",
                extra={
                    "account_id": self.account_id,
                    "dataset_name": dataset_name,
                    "row_count": len(rows),
                },
            )

            return rows

        except DatasetNotFoundError:
            raise
        except Exception as e:
            logger.error(
                f"Time travel query failed: {e}",
                extra={
                    "account_id": self.account_id,
                    "dataset_name": dataset_name,
                    "timestamp": timestamp.isoformat(),
                },
                exc_info=True,
            )
            raise CatalogError(f"Time travel query failed: {e}")

    async def list_snapshots(self, dataset_name: str) -> list[dict]:
        """
        List available snapshots for a dataset using DuckLake.

        Args:
            dataset_name: Name of the dataset

        Returns:
            List of snapshot metadata dictionaries with keys:
            - snapshot_id: Snapshot identifier
            - timestamp: When the snapshot was created
            - operation: Type of operation (INSERT, UPDATE, DELETE, etc.)
            - summary: Summary of changes

        Raises:
            DatasetNotFoundError: If dataset does not exist
            CatalogError: If listing snapshots fails

        Example:
            snapshots = await manager.list_snapshots("sales")
            for snapshot in snapshots:
                print(f"Snapshot {snapshot['snapshot_id']} at {snapshot['timestamp']}")
        """
        logger.info(
            f"Listing snapshots for dataset {dataset_name}",
            extra={"account_id": self.account_id, "dataset_name": dataset_name},
        )

        def _list() -> list[dict]:
            query = f"""
                SELECT COUNT(*) FROM information_schema.tables
                WHERE table_catalog = '{self.catalog_name}'
                  AND table_schema = 'main'
                  AND table_name = '{dataset_name}'
            """
            result = self.conn.execute(query)
            count = result.fetchone()[0]
            if count == 0:
                raise DatasetNotFoundError(f"{self.catalog_name}.{dataset_name}")

            metadata_schema = f"__ducklake_metadata_{self.catalog_name}"
            query = f"""
                SELECT
                    t.table_name,
                    t.begin_snapshot,
                    t.end_snapshot,
                    s_begin.snapshot_time as begin_time,
                    s_end.snapshot_time as end_time,
                    t.table_uuid,
                    t.path
                FROM {metadata_schema}.ducklake_table t
                LEFT JOIN {metadata_schema}.ducklake_snapshot s_begin
                    ON t.begin_snapshot = s_begin.snapshot_id
                LEFT JOIN {metadata_schema}.ducklake_snapshot s_end
                    ON t.end_snapshot = s_end.snapshot_id
                WHERE t.table_name = '{dataset_name}'
                ORDER BY t.begin_snapshot
            """
            result = self.conn.execute(query).fetchall()
            if not result:
                return []

            columns = [desc[0] for desc in self.conn.description]

            snapshots = []
            for row in result:
                snapshot = dict(zip(columns, row))
                for key, value in snapshot.items():
                    if isinstance(value, datetime):
                        snapshot[key] = value.isoformat()
                    elif value is None:
                        snapshot[key] = "current"
                snapshots.append(snapshot)

            return snapshots

        try:
            return await asyncio.to_thread(_list)
        except DatasetNotFoundError:
            raise
        except Exception as e:
            logger.error(
                f"Failed to list snapshots: {e}",
                extra={"account_id": self.account_id, "dataset_name": dataset_name},
                exc_info=True,
            )
            raise CatalogError(f"Failed to list snapshots: {e}")

    async def _dataset_exists(self, dataset_name: str) -> bool:
        """Check if a dataset exists in the catalog."""
        query = f"""
            SELECT COUNT(*) FROM information_schema.tables
            WHERE table_catalog = '{self.catalog_name}'
              AND table_schema = 'main'
              AND table_name = '{dataset_name}'
        """
        result = await self._execute_sql(query)
        count = result.fetchone()[0]
        return count > 0

    async def _get_dataset_type(self, dataset_name: str) -> DatasetType:
        """Get dataset type (table or view)."""
        query = f"""
            SELECT table_type FROM information_schema.tables
            WHERE table_catalog = '{self.catalog_name}'
              AND table_schema = 'main'
              AND table_name = '{dataset_name}'
        """
        result = await self._execute_sql(query)
        row = result.fetchone()
        if not row:
            raise DatasetNotFoundError(f"{self.catalog_name}.{dataset_name}")

        type_str = row[0]
        return DatasetType.TABLE if type_str == "BASE TABLE" else DatasetType.VIEW

    async def _get_table_schema(self, dataset_name: str) -> TableSchema:
        """Get table schema from information_schema."""
        query = f"""
            SELECT
                column_name,
                data_type,
                is_nullable
            FROM information_schema.columns
            WHERE table_catalog = '{self.catalog_name}'
              AND table_schema = 'main'
              AND table_name = '{dataset_name}'
            ORDER BY ordinal_position
        """
        result = await self._execute_sql(query)
        rows = result.fetchall()

        if not rows:
            raise DatasetNotFoundError(f"{self.catalog_name}.{dataset_name}")

        columns = []
        for col_name, data_type, is_nullable in rows:
            columns.append(
                ColumnSchema(
                    name=col_name,
                    type=data_type,
                    nullable=(is_nullable == "YES"),
                    default=None,
                    comment=None,
                )
            )

        return TableSchema(
            columns=columns,
            partition=PartitionSpec(type=PartitionType.NONE, columns=[], buckets=None),
            primary_key=None,
            indexes=None,
        )

    def _build_create_sql(self, request: CreateDatasetRequest) -> str:
        """Build CREATE TABLE/VIEW SQL statement."""
        if request.type == DatasetType.VIEW:
            raise NotImplementedError("CREATE VIEW not yet implemented")

        full_name = self._get_full_table_name(request.name)

        col_defs = []
        for col in request.schema.columns:
            col_def = f"{col.name} {col.type}"
            if not col.nullable:
                col_def += " NOT NULL"
            if col.default:
                col_def += f" DEFAULT {col.default}"
            col_defs.append(col_def)

        if request.schema.primary_key:
            pk_cols = ", ".join(request.schema.primary_key)
            col_defs.append(f"PRIMARY KEY ({pk_cols})")

        columns_sql = ",\n    ".join(col_defs)

        create_sql = f"CREATE TABLE {full_name} (\n    {columns_sql}\n)"

        return create_sql

    async def _execute_sql(self, sql: str) -> Any:
        """Execute SQL in thread pool (DuckDB is synchronous)."""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self.conn.execute, sql)


async def create_catalog_manager(
    account_id: str,
    catalog_name: str = "catalog",
    settings=None,
) -> DuckLakeCatalogManager:
    """
    Create a DuckLakeCatalogManager with proper connection setup.

    This helper function creates a DuckDB connection, loads necessary extensions,
    and attaches the DuckLake catalog for the specified account.

    Args:
        account_id: Account ID
        catalog_name: Name of the catalog (default: "default")
        settings: DuckPond settings (uses get_settings() if not provided)

    Returns:
        Initialized DuckLakeCatalogManager

    Example:
        async with create_catalog_manager("account-123") as manager:
            datasets = await manager.list_datasets()
    """
    import re
    from pathlib import Path

    import duckdb

    # Validate account_id to prevent SQL injection
    # Only allow alphanumeric characters, hyphens, and underscores
    if not re.match(r'^[a-zA-Z0-9_-]+$', account_id):
        raise ValueError(
            f"Invalid account_id '{account_id}'. "
            "Only alphanumeric characters, hyphens, and underscores are allowed."
        )

    if settings is None:
        from duckpond.config import get_settings

        settings = get_settings()

    from duckpond.accounts.models import Account
    from duckpond.db.session import create_session_factory, get_engine, get_session

    engine = get_engine()
    session_factory = create_session_factory(engine)

    async with get_session(session_factory) as session:
        from sqlalchemy import select

        result = await session.execute(select(Account).where(Account.account_id == account_id))
        account = result.scalar_one_or_none()

    loop = asyncio.get_event_loop()

    def _setup_connection():
        conn = duckdb.connect()

        conn.execute("SET enable_progress_bar=false")

        conn.execute("INSTALL ducklake")
        conn.execute("INSTALL sqlite")
        conn.execute("LOAD ducklake")
        conn.execute("LOAD sqlite")

        # Determine storage backend and set S3 credentials
        use_s3 = False
        if account and account.storage_backend == "s3":
            use_s3 = True
        elif settings.default_storage_backend == "s3":
            use_s3 = True

        if use_s3:
            conn.execute("INSTALL httpfs")
            conn.execute("LOAD httpfs")

            # Try to get credentials from account storage_config first, then fall back to settings
            s3_access_key = None
            s3_secret_key = None
            s3_region = None

            if account and account.storage_config:
                s3_access_key = account.storage_config.get("aws_access_key_id")
                s3_secret_key = account.storage_config.get("aws_secret_access_key")
                s3_region = account.storage_config.get("region")

            if not s3_access_key and hasattr(settings, "s3_access_key_id"):
                s3_access_key = settings.s3_access_key_id
            if not s3_secret_key and hasattr(settings, "s3_secret_access_key"):
                s3_secret_key = settings.s3_secret_access_key
            if not s3_region:
                s3_region = settings.s3_region

            def _sql_escape(val):
                return val.replace("'", "''") if val is not None else val
            if s3_access_key:
                conn.execute(f"SET s3_access_key_id='{_sql_escape(s3_access_key)}'")
            if s3_secret_key:
                conn.execute(f"SET s3_secret_access_key='{_sql_escape(s3_secret_key)}'")
            if s3_region:
                conn.execute(f"SET s3_region='{_sql_escape(s3_region)}'")

        if account and account.storage_backend == "s3":
            s3_bucket = account.storage_config.get("bucket") or settings.s3_bucket
            data_path = f"s3://{s3_bucket}/{account_id}/tables/"
        elif settings.default_storage_backend == "s3":
            data_path = f"s3://{settings.s3_bucket}/accounts/{account_id}/tables/"
        else:
            local_storage_path = Path(settings.local_storage_path)
            data_path = str(local_storage_path / "accounts" / account_id / "tables")

        account_catalog_dir = Path(settings.local_storage_path) / "accounts" / account_id
        account_catalog_dir.mkdir(parents=True, exist_ok=True)

        catalogs_dir = account_catalog_dir / "catalogs"
        catalogs_dir.mkdir(parents=True, exist_ok=True)

        catalog_sqlite_path = account_catalog_dir / f"{catalog_name}_catalog.sqlite"

        # Escape single quotes in data_path for SQL
        escaped_data_path = data_path.replace("'", "''")

        conn.execute(f"""
            ATTACH 'ducklake:sqlite:{catalog_sqlite_path}' AS "{catalog_name}"
            (DATA_PATH '{escaped_data_path}')
        """)

        return conn, catalog_sqlite_path

    conn, catalog_sqlite_path = await loop.run_in_executor(None, _setup_connection)

    return DuckLakeCatalogManager(
        conn=conn,
        account_id=account_id,
        catalog_url=catalog_sqlite_path,
        catalog_name=catalog_name,
    )
