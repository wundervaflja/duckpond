"""S3-compatible storage backend for DuckPond.

This module provides an S3-compatible implementation of the StorageBackend
interface with support for:
- AWS S3, MinIO, DigitalOcean Spaces, and other S3-compatible services
- Async operations using aioboto3
- Batch deletion for large datasets
- Presigned URL generation for temporary access
- S3 pagination for large file listings
"""

import os
import tempfile
from pathlib import Path
from typing import Any

import aioboto3
import duckdb
from botocore.exceptions import ClientError

from duckpond.conversion.config import ConversionConfig
from duckpond.conversion.exceptions import ConversionError, UnsupportedFormatError
from duckpond.conversion.factory import ConverterFactory
from duckpond.exceptions import (
    FileNotFoundError as DuckPondFileNotFoundError,
)
from duckpond.exceptions import (
    StorageBackendError,
)
from duckpond.storage.backend import StorageBackend


class S3Backend(StorageBackend):
    """S3-compatible storage backend.

    Stores files in S3 bucket at: s3://{bucket}/{account_id}/{remote_key}

    Features:
    - Async operations via aioboto3
    - Support for custom endpoints (MinIO, DigitalOcean Spaces)
    - Batch deletion (up to 1000 objects per request)
    - Presigned URL generation
    - Automatic pagination for large file listings

    Example:
        backend = S3Backend(bucket="my-bucket", region="us-east-1")
        await backend.upload_file(Path("local.csv"), "data/file.csv", "account-123")

        backend = S3Backend(
            bucket="my-bucket",
            endpoint_url="http://localhost:9000"
        )
    """

    def __init__(
        self,
        bucket: str,
        region: str = "us-east-1",
        endpoint_url: str | None = None,
        aws_access_key_id: str | None = None,
        aws_secret_access_key: str | None = None,
    ) -> None:
        """Initialize S3 storage backend.

        Args:
            bucket: S3 bucket name
            region: AWS region (default: us-east-1)
            endpoint_url: Custom S3 endpoint URL (for MinIO, DigitalOcean, etc.)
            aws_access_key_id: AWS access key ID (optional, uses environment if not provided)
            aws_secret_access_key: AWS secret access key (optional, uses environment if not provided)
        """
        self.bucket = bucket
        self.region = region
        self.endpoint_url = endpoint_url
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self._session = aioboto3.Session()

    def _get_client_kwargs(self) -> dict[str, Any]:
        """Get kwargs for creating S3 client.

        Returns:
            Dictionary of client configuration parameters
        """
        kwargs: dict[str, Any] = {
            "service_name": "s3",
            "region_name": self.region,
        }

        if self.endpoint_url:
            kwargs["endpoint_url"] = self.endpoint_url

        if self.aws_access_key_id:
            kwargs["aws_access_key_id"] = self.aws_access_key_id

        if self.aws_secret_access_key:
            kwargs["aws_secret_access_key"] = self.aws_secret_access_key

        return kwargs

    async def _convert_to_parquet(
        self, local_path: Path, config: ConversionConfig | None = None
    ) -> tuple[Path, dict]:
        """Convert a file to Parquet format using DuckDB.


        Args:
            local_path: Path to the source file
            config: Optional conversion configuration

        Returns:
            Tuple of (parquet_path, metrics_dict)

        Raises:
            StorageBackendError: If conversion fails
        """
        temp_parquet = None
        converter = None

        try:
            temp_fd = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False, mode="wb")
            temp_parquet = Path(temp_fd.name)
            temp_fd.close()

            try:
                converter = ConverterFactory.create_converter(local_path, config)
            except UnsupportedFormatError as e:
                raise StorageBackendError("s3", "convert_to_parquet", str(e)) from e

            try:
                result = await converter.convert_to_parquet(local_path, temp_parquet)
            except ConversionError as e:
                raise StorageBackendError("s3", "convert_to_parquet", str(e)) from e

            metrics = {
                "row_count": result.row_count,
                "source_size_bytes": result.source_size_bytes,
                "dest_size_bytes": result.dest_size_bytes,
                "compression_ratio": result.compression_ratio,
                "duration_seconds": result.duration_seconds,
                "throughput_mbps": result.throughput_mbps,
                "schema_fingerprint": result.schema_fingerprint,
                "compression": result.compression,
            }

            return temp_parquet, metrics
        except Exception as e:
            if temp_parquet and temp_parquet.exists():
                try:
                    os.unlink(temp_parquet)
                except OSError:
                    pass
            if isinstance(e, StorageBackendError):
                raise
            raise StorageBackendError(
                "s3",
                "convert_to_parquet",
                f"Failed to convert {local_path} to Parquet: {str(e)}",
            ) from e
        finally:
            if converter:
                converter.shutdown()

    async def upload_file(
        self,
        local_path: Path,
        remote_key: str,
        account_id: str,
        metadata: dict[str, str] | None = None,
        convert_to_parquet: bool = True,
        conversion_config: ConversionConfig | None = None,
    ) -> dict:
        """Upload a file to S3.

        Args:
            local_path: Local file path to upload
            remote_key: Remote key WITHOUT account prefix
            account_id: Account ID for automatic prefixing
            metadata: Optional metadata dictionary to attach to file
            convert_to_parquet: Whether to convert the file to Parquet format using DuckDB

        Returns:
            Dictionary containing:
                - remote_path: Full remote path WITH account prefix
                - metrics: Conversion metrics (if converted)

        Raises:
            FileNotFoundError: If local file doesn't exist
            StorageBackendError: If upload fails
        """
        if not local_path.exists():
            raise DuckPondFileNotFoundError(str(local_path))

        if not local_path.exists():
            raise DuckPondFileNotFoundError(str(local_path))

        original_extension = local_path.suffix.lower()
        upload_remote_key = remote_key
        table_remote_key = Path(remote_key).with_suffix(".parquet").as_posix()

        temp_parquet_path: Path | None = None
        conversion_metrics = None

        try:
            async with self._session.client(**self._get_client_kwargs()) as s3:
                upload_full_key = self._build_uploads_account_key(account_id, upload_remote_key)

                await s3.upload_file(
                    str(local_path),
                    self.bucket,
                    upload_full_key,
                )

                if convert_to_parquet:
                    table_full_key = self._build_tables_account_key(account_id, table_remote_key)

                    if original_extension == ".parquet":
                        await s3.upload_file(
                            str(local_path),
                            self.bucket,
                            table_full_key,
                        )
                    else:
                        try:
                            (
                                temp_parquet_path,
                                conversion_metrics,
                            ) = await self._convert_to_parquet(local_path, conversion_config)

                            await s3.upload_file(
                                str(temp_parquet_path),
                                self.bucket,
                                table_full_key,
                            )
                        finally:
                            if temp_parquet_path and temp_parquet_path.exists():
                                try:
                                    os.unlink(temp_parquet_path)
                                except OSError:
                                    pass

                    try:
                        await s3.delete_object(Bucket=self.bucket, Key=upload_full_key)
                    except ClientError:
                        pass

                    return {
                        "remote_path": table_full_key,
                        "metrics": conversion_metrics,
                    }
                else:
                    return {
                        "remote_path": upload_full_key,
                        "metrics": None,
                    }

        except ClientError as e:
            if temp_parquet_path and temp_parquet_path.exists():
                try:
                    os.unlink(temp_parquet_path)
                except OSError:
                    pass
            raise StorageBackendError("s3", "upload_file", str(e)) from e
        except Exception as e:
            if temp_parquet_path and temp_parquet_path.exists():
                try:
                    os.unlink(temp_parquet_path)
                except OSError:
                    pass
            if isinstance(e, DuckPondFileNotFoundError):
                raise
            raise StorageBackendError("s3", "upload_file", str(e)) from e

    async def download_file(
        self,
        remote_key: str,
        local_path: Path,
        account_id: str,
    ) -> None:
        """Download a file from S3.

        Args:
            remote_key: Remote key WITHOUT account prefix
            local_path: Local file path to download to
            account_id: Account ID for automatic prefixing

        Raises:
            FileNotFoundError: If remote file doesn't exist
            StorageBackendError: If download fails
        """
        full_key = self._build_account_key(account_id, remote_key)

        try:
            if not await self.file_exists(remote_key, account_id):
                raise DuckPondFileNotFoundError(full_key)

            local_path.parent.mkdir(parents=True, exist_ok=True)

            async with self._session.client(**self._get_client_kwargs()) as s3:
                await s3.download_file(
                    self.bucket,
                    full_key,
                    str(local_path),
                )

        except ClientError as e:
            if e.response.get("Error", {}).get("Code") == "NoSuchKey":
                raise DuckPondFileNotFoundError(full_key) from e
            raise StorageBackendError("s3", "download_file", str(e)) from e
        except Exception as e:
            if isinstance(e, DuckPondFileNotFoundError):
                raise
            raise StorageBackendError("s3", "download_file", str(e)) from e

    async def delete_file(self, remote_key: str, account_id: str) -> None:
        """Delete a file from S3.

        Args:
            remote_key: Remote key WITHOUT account prefix
            account_id: Account ID for automatic prefixing

        Raises:
            FileNotFoundError: If file doesn't exist
            StorageBackendError: If deletion fails
        """
        full_key = self._build_account_key(account_id, remote_key)

        try:
            if not await self.file_exists(remote_key, account_id):
                raise DuckPondFileNotFoundError(full_key)

            async with self._session.client(**self._get_client_kwargs()) as s3:
                await s3.delete_object(
                    Bucket=self.bucket,
                    Key=full_key,
                )

        except ClientError as e:
            if e.response.get("Error", {}).get("Code") == "NoSuchKey":
                raise DuckPondFileNotFoundError(full_key) from e
            raise StorageBackendError("s3", "delete_file", str(e)) from e
        except Exception as e:
            if isinstance(e, DuckPondFileNotFoundError):
                raise
            raise StorageBackendError("s3", "delete_file", str(e)) from e

    async def delete_prefix(self, account_id: str) -> int:
        """Delete all files with a account prefix.

        Uses batch deletion (max 1000 objects per request) for efficiency.

        Args:
            account_id: Account ID - all files matching this prefix will be deleted

        Returns:
            Number of files deleted

        Raises:
            StorageBackendError: If deletion fails
        """
        prefix = f"{account_id}/"
        deleted_count = 0

        try:
            async with self._session.client(**self._get_client_kwargs()) as s3:
                paginator = s3.get_paginator("list_objects_v2")

                async for page in paginator.paginate(
                    Bucket=self.bucket,
                    Prefix=prefix,
                ):
                    if "Contents" not in page:
                        continue

                    objects = [{"Key": obj["Key"]} for obj in page["Contents"]]

                    for i in range(0, len(objects), 1000):
                        chunk = objects[i : i + 1000]

                        response = await s3.delete_objects(
                            Bucket=self.bucket,
                            Delete={"Objects": chunk},
                        )

                        deleted_count += len(response.get("Deleted", []))

            return deleted_count

        except ClientError as e:
            raise StorageBackendError("s3", "delete_prefix", str(e)) from e
        except Exception as e:
            raise StorageBackendError("s3", "delete_prefix", str(e)) from e

    async def list_files(
        self,
        prefix: str,
        account_id: str,
        recursive: bool = True,
    ) -> list[str]:
        """List files matching a prefix.

        Handles S3 pagination automatically for large file listings.

        Args:
            prefix: Prefix to match (WITHOUT account prefix)
            account_id: Account ID for automatic prefixing
            recursive: If True, list all files recursively; if False, only immediate children

        Returns:
            List of file paths WITHOUT account prefix

        Raises:
            StorageBackendError: If listing fails
        """
        full_prefix = self._build_account_key(account_id, prefix)
        matching_files = []

        try:
            async with self._session.client(**self._get_client_kwargs()) as s3:
                paginator = s3.get_paginator("list_objects_v2")

                list_kwargs = {
                    "Bucket": self.bucket,
                    "Prefix": full_prefix,
                }

                if not recursive:
                    list_kwargs["Delimiter"] = "/"

                async for page in paginator.paginate(**list_kwargs):
                    if "Contents" in page:
                        for obj in page["Contents"]:
                            key = obj["Key"]
                            relative_key = key[len(f"{account_id}/") :]

                            if not recursive:
                                remainder = relative_key[len(prefix) :].lstrip("/")
                                if "/" not in remainder:
                                    matching_files.append(relative_key)
                            else:
                                matching_files.append(relative_key)

            return sorted(matching_files)

        except ClientError as e:
            raise StorageBackendError("s3", "list_files", str(e)) from e
        except Exception as e:
            raise StorageBackendError("s3", "list_files", str(e)) from e

    async def file_exists(self, remote_key: str, account_id: str) -> bool:
        """Check if a file exists in S3.

        Uses HEAD object to check existence without downloading.

        Args:
            remote_key: Remote key WITHOUT account prefix
            account_id: Account ID for automatic prefixing

        Returns:
            True if file exists, False otherwise

        Raises:
            StorageBackendError: If check fails
        """
        full_key = self._build_account_key(account_id, remote_key)

        try:
            async with self._session.client(**self._get_client_kwargs()) as s3:
                try:
                    await s3.head_object(Bucket=self.bucket, Key=full_key)
                    return True
                except ClientError as e:
                    if e.response.get("Error", {}).get("Code") == "404":
                        return False
                    raise

        except ClientError as e:
            if e.response.get("Error", {}).get("Code") != "404":
                raise StorageBackendError("s3", "file_exists", str(e)) from e
            return False
        except Exception as e:
            raise StorageBackendError("s3", "file_exists", str(e)) from e

    async def get_file_size(self, remote_key: str, account_id: str) -> int:
        """Get file size in bytes.

        Uses HEAD object to get size without downloading.

        Args:
            remote_key: Remote key WITHOUT account prefix
            account_id: Account ID for automatic prefixing

        Returns:
            File size in bytes

        Raises:
            FileNotFoundError: If file doesn't exist
            StorageBackendError: If operation fails
        """
        full_key = self._build_account_key(account_id, remote_key)

        try:
            async with self._session.client(**self._get_client_kwargs()) as s3:
                try:
                    response = await s3.head_object(Bucket=self.bucket, Key=full_key)
                    return response["ContentLength"]
                except ClientError as e:
                    if e.response.get("Error", {}).get("Code") == "404":
                        raise DuckPondFileNotFoundError(full_key) from e
                    raise

        except ClientError as e:
            if e.response.get("Error", {}).get("Code") == "404":
                raise DuckPondFileNotFoundError(full_key) from e
            raise StorageBackendError("s3", "get_file_size", str(e)) from e
        except Exception as e:
            if isinstance(e, DuckPondFileNotFoundError):
                raise
            raise StorageBackendError("s3", "get_file_size", str(e)) from e

    async def get_file_metadata(self, remote_key: str, account_id: str) -> dict[str, Any]:
        """Get file metadata.

        Args:
            remote_key: Remote key WITHOUT account prefix
            account_id: Account ID for automatic prefixing

        Returns:
            Dictionary containing metadata (size, modified_time, content_type, etc.)

        Raises:
            FileNotFoundError: If file doesn't exist
            StorageBackendError: If operation fails
        """
        full_key = self._build_account_key(account_id, remote_key)

        try:
            async with self._session.client(**self._get_client_kwargs()) as s3:
                try:
                    response = await s3.head_object(Bucket=self.bucket, Key=full_key)

                    metadata = {
                        "size": response["ContentLength"],
                        "content_type": response.get("ContentType", "application/octet-stream"),
                        "last_modified": response["LastModified"].isoformat(),
                        "etag": response.get("ETag", "").strip('"'),
                    }

                    if "Metadata" in response:
                        metadata.update(response["Metadata"])

                    return metadata

                except ClientError as e:
                    if e.response.get("Error", {}).get("Code") == "404":
                        raise DuckPondFileNotFoundError(full_key) from e
                    raise

        except ClientError as e:
            if e.response.get("Error", {}).get("Code") == "404":
                raise DuckPondFileNotFoundError(full_key) from e
            raise StorageBackendError("s3", "get_file_metadata", str(e)) from e
        except Exception as e:
            if isinstance(e, DuckPondFileNotFoundError):
                raise
            raise StorageBackendError("s3", "get_file_metadata", str(e)) from e

    async def get_storage_usage(self, account_id: str) -> int:
        """Calculate total storage usage for a account.

        Args:
            account_id: Account identifier

        Returns:
            Total storage usage in bytes

        Raises:
            StorageBackendError: If calculation fails
        """
        prefix = f"{account_id}/"
        total_size = 0

        try:
            async with self._session.client(**self._get_client_kwargs()) as s3:
                paginator = s3.get_paginator("list_objects_v2")

                async for page in paginator.paginate(
                    Bucket=self.bucket,
                    Prefix=prefix,
                ):
                    if "Contents" in page:
                        for obj in page["Contents"]:
                            total_size += obj["Size"]

            return total_size

        except ClientError as e:
            raise StorageBackendError("s3", "get_storage_usage", str(e)) from e
        except Exception as e:
            raise StorageBackendError("s3", "get_storage_usage", str(e)) from e

    async def generate_presigned_url(
        self,
        remote_key: str,
        account_id: str,
        expires_seconds: int = 3600,
        method: str = "GET",
    ) -> str:
        """Generate a presigned URL for direct file access.

        Args:
            remote_key: Remote key WITHOUT account prefix
            account_id: Account ID for automatic prefixing
            expires_seconds: URL expiration time in seconds (default: 1 hour)
            method: HTTP method (GET, PUT, etc.)

        Returns:
            Presigned URL string

        Raises:
            FileNotFoundError: If file doesn't exist (for GET URLs)
            StorageBackendError: If URL generation fails
        """
        full_key = self._build_account_key(account_id, remote_key)

        if method.upper() == "GET":
            if not await self.file_exists(remote_key, account_id):
                raise DuckPondFileNotFoundError(full_key)

        try:
            async with self._session.client(**self._get_client_kwargs()) as s3:
                operation_map = {
                    "GET": "get_object",
                    "PUT": "put_object",
                    "DELETE": "delete_object",
                }

                operation = operation_map.get(method.upper(), "get_object")

                url = s3.generate_presigned_url(
                    operation,
                    Params={"Bucket": self.bucket, "Key": full_key},
                    ExpiresIn=expires_seconds,
                )

                return url

        except ClientError as e:
            raise StorageBackendError("s3", "generate_presigned_url", str(e)) from e
        except Exception as e:
            if isinstance(e, DuckPondFileNotFoundError):
                raise
            raise StorageBackendError("s3", "generate_presigned_url", str(e)) from e
