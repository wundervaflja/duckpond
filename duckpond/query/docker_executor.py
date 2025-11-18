"""Docker-based query executor for isolated query execution.

This module provides a query executor that runs queries in isolated Docker
containers for enhanced security and resource control.
"""

import asyncio
import base64
import logging
import time
from pathlib import Path
from typing import Literal, Optional

import pyarrow as pa

from duckpond.config import get_settings
from duckpond.docker.exceptions import ContainerExecutionException
from duckpond.docker.runners import QueryRunner
from duckpond.exceptions import QueryExecutionError, QueryTimeoutError
from duckpond.query.models import QueryMetrics, QueryResult
from duckpond.query.validator import SQLValidator, sanitize_for_logging

logger = logging.getLogger(__name__)


class DockerQueryExecutor:
    """
    Execute SQL queries in isolated Docker containers.

    This executor provides enhanced security and resource isolation by running
    queries in ephemeral Docker containers. Each query execution:
    - Runs in a fresh container
    - Has enforced resource limits (CPU, memory)
    - Cannot affect other queries or the host system
    - Automatically cleans up after completion

    Features:
    - SQL validation (blocks dangerous operations)
    - Multiple output formats (JSON, Arrow, CSV)
    - Query timeout enforcement
    - Resource isolation via Docker
    - Error handling and logging
    - Query metrics collection

    Usage:
        executor = DockerQueryExecutor(
            account_id="account123",
            account_data_dir=Path("/data/account123"),
            catalog_path=Path("/data/account123/catalog.sqlite"),
        )
        result = await executor.execute_query(
            sql="SELECT * FROM catalog.sales",
            output_format="json",
            timeout_seconds=30
        )
    """

    def __init__(
        self,
        account_id: str,
        account_data_dir: Path,
        catalog_path: Path,
        docker_image: str = "duckpond:25.1",
        memory_limit_mb: int = 4096,
        cpu_limit: float = 2.0,
        validator: Optional[SQLValidator] = None,
        aws_credentials: Optional[dict] = None,
    ) -> None:
        """
        Initialize Docker-based query executor.

        Args:
            account_id: Account identifier
            account_data_dir: Directory containing account data
            catalog_path: Path to DuckLake catalog file
            docker_image: Docker image to use for execution
            memory_limit_mb: Memory limit for containers in MB
            cpu_limit: CPU limit (1.0 = 1 core)
            validator: SQL validator (uses default if not provided)
            aws_credentials: Optional AWS credentials dict from account storage_config
        """
        self.account_id = account_id
        self.account_data_dir = account_data_dir
        self.catalog_path = catalog_path
        self.docker_image = docker_image
        self.memory_limit_mb = memory_limit_mb
        self.cpu_limit = cpu_limit
        self.validator = validator or SQLValidator()
        self.aws_credentials = aws_credentials

        logger.debug(
            f"Initialized DockerQueryExecutor for account {self.account_id}",
            extra={"account_id": self.account_id},
        )

    async def execute_query(
        self,
        sql: str,
        output_format: Literal["json", "arrow", "csv"] = "json",
        limit: Optional[int] = None,
        timeout_seconds: int = 30,
        attach_catalog: Optional[str] = None,
    ) -> QueryResult:
        """
        Execute SQL query in isolated Docker container.

        Args:
            sql: SQL query string
            output_format: Output format (json, arrow, csv)
            limit: Optional row limit to apply
            timeout_seconds: Query timeout in seconds
            attach_catalog: Optional catalog to attach (format: "name")

        Returns:
            QueryResult with data in requested format

        Raises:
            SQLValidationError: If SQL validation fails
            QueryTimeoutError: If query exceeds timeout
            QueryExecutionError: If query execution fails
        """
        start_time = time.time()
        query_hash = self.validator.hash_query(sql)
        sanitized_sql = sanitize_for_logging(sql)

        logger.info(
            "Executing query in Docker container",
            extra={
                "account_id": self.account_id,
                "query_hash": query_hash,
                "output_format": output_format,
                "limit": limit,
                "timeout_seconds": timeout_seconds,
                "query": sanitized_sql,
            },
        )

        try:
            # Validate SQL
            self.validator.validate(sql)

            # Create and start query runner
            runner = QueryRunner(
                account_data_dir=self.account_data_dir,
                account_id=self.account_id,
                catalog_path=self.catalog_path,
                docker_image=self.docker_image,
                memory_limit_mb=self.memory_limit_mb,
                cpu_limit=self.cpu_limit,
                aws_credentials=self.aws_credentials,
            )

            try:
                # Start container
                await runner.start()

                # Execute query with timeout
                result_dict = await asyncio.wait_for(
                    runner.execute_query(
                        sql=sql,
                        output_format=output_format,
                        limit=limit,
                        timeout_seconds=timeout_seconds,
                        attach_catalog=attach_catalog,
                    ),
                    timeout=timeout_seconds + 10,  # Add buffer for container overhead
                )

                # Convert result based on format
                data, row_count = self._convert_result(result_dict, output_format)

                execution_time = time.time() - start_time

                self._log_metrics(
                    query_hash=query_hash,
                    execution_time=execution_time,
                    row_count=row_count,
                    output_format=output_format,
                    success=True,
                )

                logger.info(
                    "Query executed successfully in Docker",
                    extra={
                        "account_id": self.account_id,
                        "query_hash": query_hash,
                        "row_count": row_count,
                        "execution_time": execution_time,
                    },
                )

                return QueryResult(
                    data=data,
                    row_count=row_count,
                    execution_time_seconds=execution_time,
                    query=sql,
                    format=output_format,
                )

            finally:
                # Always cleanup container
                try:
                    await runner.stop()
                except Exception as e:
                    logger.warning(
                        f"Failed to stop query container: {e}",
                        extra={"account_id": self.account_id},
                    )

        except asyncio.TimeoutError:
            execution_time = time.time() - start_time
            self._log_metrics(
                query_hash=query_hash,
                execution_time=execution_time,
                row_count=0,
                output_format=output_format,
                success=False,
                error_type="timeout",
            )

            logger.error(
                "Query timeout in Docker",
                extra={
                    "account_id": self.account_id,
                    "query_hash": query_hash,
                    "timeout_seconds": timeout_seconds,
                },
            )
            raise QueryTimeoutError(timeout_seconds)

        except ContainerExecutionException as e:
            execution_time = time.time() - start_time
            self._log_metrics(
                query_hash=query_hash,
                execution_time=execution_time,
                row_count=0,
                output_format=output_format,
                success=False,
                error_type="container_execution",
            )

            logger.error(
                f"Container execution failed: {e}",
                extra={
                    "account_id": self.account_id,
                    "query_hash": query_hash,
                    "error": str(e),
                },
            )
            raise QueryExecutionError(f"Query execution failed: {e}", query=sql) from e

        except Exception as e:
            execution_time = time.time() - start_time
            self._log_metrics(
                query_hash=query_hash,
                execution_time=execution_time,
                row_count=0,
                output_format=output_format,
                success=False,
                error_type=type(e).__name__,
            )

            logger.error(
                f"Query execution failed: {e}",
                extra={
                    "account_id": self.account_id,
                    "query_hash": query_hash,
                    "error": str(e),
                },
            )
            raise

    def _convert_result(
        self, result_dict: dict, output_format: str
    ) -> tuple[list[dict] | pa.Table | str, int]:
        """
        Convert query result from container to appropriate format.

        Args:
            result_dict: Result dictionary from container
            output_format: Target format

        Returns:
            Tuple of (converted data, row count)
        """
        data = result_dict.get("data")
        row_count = result_dict.get("row_count", 0)

        if output_format == "json":
            # Data is already in JSON format
            return data, row_count

        elif output_format == "csv":
            # Data is already in CSV format
            return data, row_count

        elif output_format == "arrow":
            # Data is base64-encoded Arrow IPC stream
            arrow_bytes = base64.b64decode(data)
            import pyarrow as pa

            reader = pa.ipc.open_stream(arrow_bytes)
            table = reader.read_all()
            return table, row_count

        else:
            raise ValueError(f"Unsupported output format: {output_format}")

    async def explain_query(self, sql: str, attach_catalog: Optional[str] = None) -> str:
        """
        Get query execution plan in Docker container.

        Args:
            sql: SQL query to explain
            attach_catalog: Optional catalog to attach

        Returns:
            Query execution plan as string

        Raises:
            SQLValidationError: If SQL validation fails
            QueryExecutionError: If EXPLAIN fails
        """
        self.validator.validate(sql)

        logger.info(
            "Explaining query in Docker",
            extra={
                "account_id": self.account_id,
                "query": sanitize_for_logging(sql),
            },
        )

        runner = QueryRunner(
            account_data_dir=self.account_data_dir,
            account_id=self.account_id,
            catalog_path=self.catalog_path,
            docker_image=self.docker_image,
            memory_limit_mb=self.memory_limit_mb,
            cpu_limit=self.cpu_limit,
        )

        try:
            await runner.start()
            plan = await runner.explain_query(sql, attach_catalog=attach_catalog)
            return plan

        finally:
            try:
                await runner.stop()
            except Exception as e:
                logger.warning(
                    f"Failed to stop explain container: {e}",
                    extra={"account_id": self.account_id},
                )

    def _log_metrics(
        self,
        query_hash: str,
        execution_time: float,
        row_count: int,
        output_format: str,
        success: bool,
        error_type: Optional[str] = None,
    ) -> None:
        """
        Log query metrics.

        Args:
            query_hash: Hash of query
            execution_time: Execution time in seconds
            row_count: Number of rows returned
            output_format: Output format used
            success: Whether query succeeded
            error_type: Type of error if failed
        """
        metrics = QueryMetrics(
            account_id=self.account_id,
            query_hash=query_hash,
            execution_time_seconds=execution_time,
            row_count=row_count,
            output_format=output_format,
            success=success,
            error_type=error_type,
        )

        logger.info(
            "Query metrics (Docker)",
            extra={"metrics": metrics.to_dict()},
        )

    @classmethod
    def from_account(
        cls,
        account,
        docker_image: str = "duckpond:25.1",
        memory_limit_mb: int = 4096,
        cpu_limit: float = 2.0,
    ) -> "DockerQueryExecutor":
        """
        Create executor from account model.

        Args:
            account: Account model
            docker_image: Docker image to use
            memory_limit_mb: Memory limit in MB
            cpu_limit: CPU limit

        Returns:
            DockerQueryExecutor instance
        """
        settings = get_settings()

        account_data_dir = (
            Path(settings.local_storage_path).expanduser() / "accounts" / account.account_id
        )

        # Determine catalog path from account's ducklake_catalog_url
        # Format can be either "sqlite:/path/to/catalog.sqlite" or "/path/to/catalog.sqlite"
        catalog_url = account.ducklake_catalog_url
        if catalog_url.startswith("sqlite:"):
            catalog_path = Path(catalog_url[7:])  # Remove "sqlite:" prefix
        else:
            # Treat as plain file path
            catalog_path = Path(catalog_url)

        return cls(
            account_id=account.account_id,
            account_data_dir=account_data_dir,
            catalog_path=catalog_path,
            docker_image=docker_image,
            memory_limit_mb=memory_limit_mb,
            cpu_limit=cpu_limit,
            aws_credentials=account.storage_config if account.storage_backend == "s3" else None,
        )
