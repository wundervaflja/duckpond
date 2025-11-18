"""DuckDB secret management for S3 access control.

This module provides utilities for creating DuckDB S3 secrets for
role-based access control with DuckLake catalogs.

Secrets enable fine-grained S3 access control with different IAM credentials
for different roles (superuser, writer, reader).

See: https://ducklake.select/docs/stable/duckdb/guides/access_control
"""

import logging
from dataclasses import dataclass
from enum import Enum
from typing import Optional

import duckdb

logger = logging.getLogger(__name__)


class S3Role(str, Enum):
    """S3 access control roles."""

    SUPERUSER = "superuser"  # Full S3 access
    WRITER = "writer"  # Read/write access to specific paths
    READER = "reader"  # Read-only access to specific paths


@dataclass
class S3Credentials:
    """S3 storage credentials."""

    access_key_id: str
    secret_access_key: str
    region: str
    endpoint_url: Optional[str] = None
    session_token: Optional[str] = None


@dataclass
class S3SecretConfig:
    """Configuration for S3 secret creation."""

    account_id: str
    role: S3Role
    s3_creds: S3Credentials
    s3_bucket: str


class S3SecretManager:
    """
    Manages DuckDB S3 secrets for access control.

    Creates S3 secrets with appropriate IAM credentials for different roles.

    Example:
        s3_creds = S3Credentials(
            access_key_id="AKIAIOSFODNN7EXAMPLE",
            secret_access_key="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            region="us-east-1"
        )

        config = S3SecretConfig(
            account_id="acct-123",
            role=S3Role.SUPERUSER,
            s3_creds=s3_creds,
            s3_bucket="my-bucket"
        )

        manager = S3SecretManager(config)
        secret_name = manager.create_secret(conn)
        # Returns: "s3_acct_123_superuser"
    """

    def __init__(self, config: S3SecretConfig) -> None:
        """
        Initialize secret manager.

        Args:
            config: S3 secret configuration
        """
        self.config = config
        self.account_id_safe = config.account_id.replace("-", "_")

    def get_secret_name(self) -> str:
        """Get S3 secret name."""
        return f"s3_{self.account_id_safe}_{self.config.role.value}"

    def generate_secret_sql(self) -> str:
        """
        Generate SQL to create S3 secret.

        Returns:
            SQL statement to create S3 secret
        """
        secret_name = self.get_secret_name()
        creds = self.config.s3_creds

        def escape_sql(value: Optional[str]) -> str:
            if value is None:
                return ""
            return value.replace("'", "''")

        sql_parts = [
            f"CREATE OR REPLACE SECRET {secret_name} (",
            "    TYPE s3,",
            "    PROVIDER config,",
            f"    KEY_ID '{escape_sql(creds.access_key_id)}',",
            f"    SECRET '{escape_sql(creds.secret_access_key)}',",
            f"    REGION '{escape_sql(creds.region)}'",
        ]

        if creds.endpoint_url:
            sql_parts.insert(-1, f"    ENDPOINT '{escape_sql(creds.endpoint_url)}',")

        if creds.session_token:
            sql_parts.insert(-1, f"    SESSION_TOKEN '{escape_sql(creds.session_token)}',")

        sql_parts.append(")")

        return "\n".join(sql_parts)

    def create_secret(self, conn: duckdb.DuckDBPyConnection) -> str:
        """
        Create S3 secret in the provided DuckDB connection.

        Args:
            conn: DuckDB connection

        Returns:
            Name of the created S3 secret

        Raises:
            Exception: If secret creation fails
        """
        logger.info(
            f"Creating {self.config.role.value} S3 secret for account {self.config.account_id}",
            extra={
                "account_id": self.config.account_id,
                "role": self.config.role.value,
            },
        )

        try:
            sql = self.generate_secret_sql()
            conn.execute(sql)

            secret_name = self.get_secret_name()
            logger.info(
                f"Created S3 secret: {secret_name}",
                extra={
                    "account_id": self.config.account_id,
                    "role": self.config.role.value,
                    "secret_name": secret_name,
                },
            )

            return secret_name

        except Exception as e:
            logger.error(
                f"Failed to create S3 secret for account {self.config.account_id}: {e}",
                extra={
                    "account_id": self.config.account_id,
                    "role": self.config.role.value,
                },
                exc_info=True,
            )
            raise

    def drop_secret(self, conn: duckdb.DuckDBPyConnection) -> None:
        """
        Drop S3 secret from the connection.

        Args:
            conn: DuckDB connection

        Raises:
            Exception: If secret deletion fails
        """
        logger.info(
            f"Dropping {self.config.role.value} S3 secret for account {self.config.account_id}",
            extra={
                "account_id": self.config.account_id,
                "role": self.config.role.value,
            },
        )

        try:
            conn.execute(f"DROP SECRET IF EXISTS {self.get_secret_name()}")

            logger.info(
                f"Dropped S3 secret for account {self.config.account_id}",
                extra={
                    "account_id": self.config.account_id,
                    "role": self.config.role.value,
                },
            )

        except Exception as e:
            logger.error(
                f"Failed to drop S3 secret for account {self.config.account_id}: {e}",
                extra={
                    "account_id": self.config.account_id,
                    "role": self.config.role.value,
                },
                exc_info=True,
            )
            raise


def create_superuser_secret(
    account_id: str,
    s3_creds: S3Credentials,
    s3_bucket: str,
) -> S3SecretManager:
    """
    Create a superuser S3 secret manager.

    The superuser has full S3 access to the entire bucket.

    Args:
        account_id: Account ID
        s3_creds: S3 credentials with full bucket access
        s3_bucket: S3 bucket name

    Returns:
        Configured secret manager
    """
    config = S3SecretConfig(
        account_id=account_id,
        role=S3Role.SUPERUSER,
        s3_creds=s3_creds,
        s3_bucket=s3_bucket,
    )
    return S3SecretManager(config)


def create_writer_secret(
    account_id: str,
    s3_creds: S3Credentials,
    s3_bucket: str,
) -> S3SecretManager:
    """
    Create a writer S3 secret manager.

    The writer has read/write access to specific paths (schema-level).

    Args:
        account_id: Account ID
        s3_creds: S3 credentials with schema-level access
        s3_bucket: S3 bucket name

    Returns:
        Configured secret manager
    """
    config = S3SecretConfig(
        account_id=account_id,
        role=S3Role.WRITER,
        s3_creds=s3_creds,
        s3_bucket=s3_bucket,
    )
    return S3SecretManager(config)


def create_reader_secret(
    account_id: str,
    s3_creds: S3Credentials,
    s3_bucket: str,
) -> S3SecretManager:
    """
    Create a reader S3 secret manager.

    The reader has read-only access to specific paths (table-level).

    Args:
        account_id: Account ID
        s3_creds: S3 credentials with read-only access
        s3_bucket: S3 bucket name

    Returns:
        Configured secret manager
    """
    config = S3SecretConfig(
        account_id=account_id,
        role=S3Role.READER,
        s3_creds=s3_creds,
        s3_bucket=s3_bucket,
    )
    return S3SecretManager(config)


def print_secret_sql(manager: S3SecretManager) -> None:
    """
    Print S3 secret SQL statement for manual execution or inspection.

    Args:
        manager: Secret manager instance
    """
    print("=" * 80)
    print(f"S3 Secret for Account: {manager.config.account_id}")
    print(f"Role: {manager.config.role.value}")
    print(f"Bucket: {manager.config.s3_bucket}")
    print("=" * 80)
    print()
    print(manager.generate_secret_sql())
    print()
    print("=" * 80)
