"""Query execution Docker runner.

This module provides a specialized Docker runner for executing SQL queries
in isolated containers with proper security and resource limits.
"""

import json
from pathlib import Path
from typing import Dict, Literal, Optional

import structlog

from duckpond.docker.config import ContainerConfig
from duckpond.docker.container import DockerContainer
from duckpond.docker.exceptions import ContainerExecutionException

logger = structlog.get_logger(__name__)


class QueryRunner:
    """
    Docker runner for isolated query execution.

    Provides a high-level interface for executing SQL queries in isolated
    Docker containers with DuckDB, ensuring security and resource isolation.

    Features:
    - Isolated query execution environment
    - Configurable resource limits
    - Automatic catalog mounting
    - AWS credentials passing for S3 access
    - Result retrieval in multiple formats
    - Query timeout enforcement

    Usage:
        runner = QueryRunner(
            account_data_dir=Path("/data/account123"),
            account_id="account123",
            catalog_path=Path("/data/account123/catalog.sqlite"),
        )
        await runner.start()
        result = await runner.execute_query(
            sql="SELECT * FROM catalog.sales LIMIT 10",
            output_format="json",
        )
        await runner.stop()
    """

    def __init__(
        self,
        account_data_dir: Path,
        account_id: str,
        catalog_path: Path,
        docker_image: str = "duckpond:25.1",
        memory_limit_mb: int = 4096,
        cpu_limit: float = 2.0,
        startup_timeout: int = 10,
        aws_credentials: Optional[Dict[str, str]] = None,
    ):
        """
        Initialize query execution runner.

        Args:
            account_data_dir: Working directory with data files
            account_id: Tenant identifier for container naming
            catalog_path: Path to DuckLake catalog file
            docker_image: Docker image to use
            memory_limit_mb: Memory limit in megabytes
            cpu_limit: CPU limit (1.0 = 1 core)
            startup_timeout: Maximum seconds to wait for startup
            aws_credentials: Optional AWS credentials dict with keys:
                aws_access_key_id, aws_secret_access_key, region
        """
        self.account_data_dir = account_data_dir
        self.account_id = account_id
        self.catalog_path = catalog_path
        self.docker_image = docker_image
        self.memory_limit_mb = memory_limit_mb
        self.cpu_limit = cpu_limit
        self.startup_timeout = startup_timeout
        self.aws_credentials = aws_credentials

        # Build container configuration
        self.config = self._build_config()
        self.container = DockerContainer(self.config)

    def _build_config(self) -> ContainerConfig:
        """
        Build Docker container configuration for query execution.

        Returns:
            ContainerConfig with query execution settings
        """
        container_name = f"query-{self.account_id}"

        # Pre-built image has all dependencies, just keep container running
        startup_command = [
            "sh",
            "-c",
            "echo 'READY' && tail -f /dev/null",
        ]

        config = ContainerConfig(
            image=self.docker_image,
            command=startup_command,
            name=container_name,
            startup_timeout_seconds=self.startup_timeout,
        )

        config.add_volume(
            host_path=self.account_data_dir,
            container_path=str(self.account_data_dir),
            read_only=False,
        )

        config.working_dir = str(self.account_data_dir)

        config.use_bridge_network()

        config.set_resources(
            memory_mb=self.memory_limit_mb,
            cpu_limit=self.cpu_limit,
        )

        # Add AWS credentials from account storage_config or environment
        if self.aws_credentials:
            # Use credentials from account storage_config
            aws_env = {
                "AWS_ACCESS_KEY_ID": self.aws_credentials.get("aws_access_key_id", ""),
                "AWS_SECRET_ACCESS_KEY": self.aws_credentials.get("aws_secret_access_key", ""),
            }
            if "region" in self.aws_credentials:
                aws_env["AWS_REGION"] = self.aws_credentials["region"]
                aws_env["AWS_DEFAULT_REGION"] = self.aws_credentials["region"]

            config.add_env_from_dict(aws_env)
            logger.debug(
                "added_aws_credentials_from_account",
                container_name=container_name,
                keys=list(aws_env.keys()),
            )
        else:
            # Fall back to environment variables
            aws_env = DockerContainer.get_aws_credentials_env()
            if aws_env:
                config.add_env_from_dict(aws_env)
                logger.debug(
                    "added_aws_credentials_from_env",
                    container_name=container_name,
                    keys=list(aws_env.keys()),
                )

        return config

    async def start(self) -> str:
        """
        Start query execution container.

        Returns:
            The container ID

        Raises:
            ContainerStartupException: If container fails to start
        """
        logger.info(
            "starting_query_runner",
            account_id=self.account_id,
            memory_mb=self.memory_limit_mb,
            cpu_limit=self.cpu_limit,
            catalog_path=str(self.catalog_path),
        )

        logger.info(
            "starting_query_container",
            account_id=self.account_id,
        )

        # Start container without health check (pre-built image is ready immediately)
        container_id = await self.container.start(health_check_url=None)

        logger.info(
            "container_started",
            container_id=container_id[:12],
            account_id=self.account_id,
        )

        # Pre-built image is ready immediately, just verify it works
        try:
            await self._execute_test_query()
            logger.info(
                "query_runner_ready",
                container_id=container_id[:12],
                account_id=self.account_id,
            )
        except Exception as e:
            logs = await self.container.get_logs()
            logger.error(
                "query_runner_startup_failed",
                account_id=self.account_id,
                error=str(e),
                logs=logs,
            )
            await self.container.stop()
            raise ContainerExecutionException(
                f"Query runner failed to start: {e}\n\nContainer logs:\n{logs}"
            )

        return container_id

    async def _execute_test_query(self) -> None:
        """
        Execute a simple test query to verify container is ready.

        Raises:
            ContainerExecutionException: If test query fails
        """
        test_script = "import duckdb; print('ready')"
        stdout, stderr, returncode = await self.container.execute(
            ["python", "-c", test_script],
            timeout=10,
        )

        if returncode != 0 or "ready" not in stdout:
            raise ContainerExecutionException(
                f"Test query failed. stdout: {stdout}, stderr: {stderr}"
            )

    async def execute_query(
        self,
        sql: str,
        output_format: Literal["json", "arrow", "csv"] = "json",
        limit: Optional[int] = None,
        timeout_seconds: int = 30,
        attach_catalog: Optional[str] = None,
    ) -> Dict:
        """
        Execute SQL query in isolated container.

        Args:
            sql: SQL query string
            output_format: Output format (json, arrow, csv)
            limit: Optional row limit to apply
            timeout_seconds: Query timeout in seconds
            attach_catalog: Optional additional catalog to attach

        Returns:
            Dictionary with query results:
            {
                "data": <query results>,
                "row_count": <number of rows>,
                "format": <output format>
            }

        Raises:
            ContainerExecutionException: If query execution fails
        """
        logger.info(
            "executing_query_in_container",
            account_id=self.account_id,
            output_format=output_format,
            limit=limit,
            timeout_seconds=timeout_seconds,
            sql_preview=sql[:100] if len(sql) > 100 else sql,
        )

        # Verify catalog files exist before trying to use them
        if not self.catalog_path.exists():
            raise ContainerExecutionException(
                f"Main catalog file not found: {self.catalog_path}. "
                f"Cannot execute query without catalog."
            )

        if attach_catalog:
            attach_catalog_path = self.account_data_dir / f"{attach_catalog}_catalog.sqlite"
            if not attach_catalog_path.exists():
                raise ContainerExecutionException(
                    f"Additional catalog file not found: {attach_catalog_path}. "
                    f"Available files in {self.account_data_dir}: "
                    f"{list(self.account_data_dir.glob('*_catalog.sqlite'))}"
                )

        # Build DuckDB commands: ATTACH catalog(s) then execute query
        commands = []

        # Install and load DuckLake extension
        commands.append("INSTALL ducklake;")
        commands.append("LOAD ducklake;")

        # Attach main catalog
        ducklake_url = f"sqlite:{self.catalog_path}"
        commands.append(f"ATTACH '{ducklake_url}' AS catalog (TYPE ducklake);")

        # Attach additional catalog if specified
        if attach_catalog:
            attach_catalog_path = self.account_data_dir / f"{attach_catalog}_catalog.sqlite"
            attach_ducklake_url = f"sqlite:{attach_catalog_path}"
            commands.append(
                f"ATTACH '{attach_ducklake_url}' AS \"{attach_catalog}\" (TYPE ducklake);"
            )

        # Apply limit if specified
        final_sql = sql
        if limit:
            final_sql = f"SELECT * FROM ({sql}) AS limited_query LIMIT {limit}"

        # Add the query
        commands.append(final_sql)

        # Join all commands with newlines
        full_command = "\n".join(commands)

        # Execute using DuckDB CLI with appropriate output format
        if output_format == "json":
            duckdb_cmd = ["duckdb", "-json", "-c", full_command]
        elif output_format == "csv":
            duckdb_cmd = ["duckdb", "-csv", "-c", full_command]
        elif output_format == "arrow":
            # For Arrow, we need to use a different approach
            # DuckDB CLI doesn't support Arrow output directly, so we'll use JSON
            duckdb_cmd = ["duckdb", "-json", "-c", full_command]
        else:
            raise ValueError(f"Unsupported output format: {output_format}")

        logger.debug(
            "executing_duckdb_command",
            account_id=self.account_id,
            command_preview=full_command[:200],
            output_format=output_format,
        )

        stdout, stderr, returncode = await self.container.execute(
            duckdb_cmd,
            timeout=timeout_seconds,
        )

        if returncode != 0:
            logger.error(
                "query_execution_failed",
                account_id=self.account_id,
                stderr=stderr,
                stdout=stdout,
                returncode=returncode,
            )
            error_msg = stderr if stderr else stdout
            raise ContainerExecutionException(f"Query execution failed: {error_msg}")

        # Parse result based on format
        try:
            if output_format == "json":
                # DuckDB -json outputs JSON array directly
                data = json.loads(stdout)
                row_count = len(data) if isinstance(data, list) else 0
            elif output_format == "csv":
                # DuckDB -csv outputs CSV string
                data = stdout
                # Count rows (subtract 1 for header)
                row_count = len(stdout.strip().split("\n")) - 1 if stdout.strip() else 0
            elif output_format == "arrow":
                # Convert JSON to Arrow table
                import base64
                from io import BytesIO

                import pyarrow as pa

                json_data = json.loads(stdout)
                # Convert to Arrow table then to base64 IPC stream
                table = pa.Table.from_pylist(json_data)
                sink = BytesIO()
                writer = pa.ipc.new_stream(sink, table.schema)
                writer.write_table(table)
                writer.close()

                arrow_bytes = sink.getvalue()
                data = base64.b64encode(arrow_bytes).decode("utf-8")
                row_count = table.num_rows

            logger.info(
                "query_executed_successfully",
                account_id=self.account_id,
                row_count=row_count,
                format=output_format,
            )

            # Return in expected format
            return {
                "data": data,
                "row_count": row_count,
                "format": output_format,
            }
        except (json.JSONDecodeError, Exception) as e:
            logger.error(
                "failed_to_parse_result",
                account_id=self.account_id,
                error=str(e),
                stdout_preview=stdout[:500],
            )
            raise ContainerExecutionException(
                f"Failed to parse query result: {e}. Output preview: {stdout[:500]}"
            )

    async def explain_query(
        self,
        sql: str,
        attach_catalog: Optional[str] = None,
    ) -> str:
        """
        Get query execution plan.

        Args:
            sql: SQL query to explain
            attach_catalog: Optional additional catalog to attach

        Returns:
            Query execution plan as string

        Raises:
            ContainerExecutionException: If EXPLAIN fails
        """
        # Build DuckDB commands for EXPLAIN
        commands = []

        # Install and load DuckLake extension
        commands.append("INSTALL ducklake;")
        commands.append("LOAD ducklake;")

        # Attach main catalog
        ducklake_url = f"sqlite:{self.catalog_path}"
        commands.append(f"ATTACH '{ducklake_url}' AS catalog (TYPE ducklake);")

        # Attach additional catalog if specified
        if attach_catalog:
            attach_catalog_path = self.account_data_dir / f"{attach_catalog}_catalog.sqlite"
            attach_ducklake_url = f"sqlite:{attach_catalog_path}"
            commands.append(
                f"ATTACH '{attach_ducklake_url}' AS \"{attach_catalog}\" (TYPE ducklake);"
            )

        # Add EXPLAIN query
        commands.append(f"EXPLAIN {sql}")

        full_command = "\n".join(commands)

        # Execute using DuckDB CLI
        duckdb_cmd = ["duckdb", "-c", full_command]

        stdout, stderr, returncode = await self.container.execute(
            duckdb_cmd,
            timeout=30,
        )

        if returncode != 0:
            error_msg = stderr if stderr else stdout
            raise ContainerExecutionException(f"EXPLAIN failed: {error_msg}")

        return stdout

    async def stop(self, timeout: int = 10) -> None:
        """
        Stop query execution container gracefully.

        Args:
            timeout: Seconds to wait for graceful shutdown
        """
        logger.info(
            "stopping_query_runner",
            account_id=self.account_id,
        )

        await self.container.stop(timeout=timeout)

        logger.info(
            "query_runner_stopped",
            account_id=self.account_id,
        )

    async def get_logs(self, tail: int = 100) -> str:
        """
        Get container logs.

        Args:
            tail: Number of lines to read from end

        Returns:
            Container logs as string
        """
        return await self.container.get_logs(tail=tail)

    def is_alive(self) -> bool:
        """
        Check if container exists (synchronous check).

        Returns:
            True if container was started, False otherwise
        """
        return self.container.is_alive()

    async def is_running(self) -> bool:
        """
        Check if container is currently running.

        Returns:
            True if container is running, False otherwise
        """
        return await self.container.is_running()

    def get_container_id(self) -> Optional[str]:
        """
        Get container ID.

        Returns:
            Container ID if started, None otherwise
        """
        return self.container.get_container_id()
