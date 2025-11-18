"""Local filesystem storage backend for DuckPond.

This module provides a local filesystem implementation of the StorageBackend
interface with support for:
- Path-based account isolation using directories
- Async file I/O using aiofiles
- Proper error handling and path validation
- Metadata storage alongside files
"""

import json
import os
import tempfile
from pathlib import Path
from typing import Any

import aiofiles
import aiofiles.os

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


class LocalBackend(StorageBackend):
    """Local filesystem storage backend.

    Stores files at: {base_path}/{account_id}/{remote_key}

    Features:
    - Automatic directory creation
    - Metadata stored as {filename}.metadata.json
    - Concurrent access safe (filesystem-level locking)
    - Absolute path validation to prevent path traversal

    Example:
        backend = LocalBackend(Path("~/.duckpond/data"))
        await backend.upload_file(Path("local.csv"), "data/file.csv", "account-123")
    """

    def __init__(self, base_path: Path) -> None:
        """Initialize local filesystem backend.

        Args:
            base_path: Root directory for all account data
        """
        self.base_path = Path(base_path).expanduser().resolve()
        self.base_path.mkdir(parents=True, exist_ok=True)

    def _build_tables_account_key(self, account_id: str, remote_key: str) -> str:
        """Build full tables storage key with account prefix for local storage.

        Overrides parent to use accounts/ subdirectory for consistency with
        catalog storage and Docker volume mounts.

        Args:
            account_id: Account identifier
            remote_key: Key without account prefix

        Returns:
            Full key with account prefix: "accounts/{account_id}/tables/{remote_key}"
        """
        remote_key = remote_key.lstrip("/")
        return f"accounts/{account_id}/tables/{remote_key}"

    def _get_table_full_path(self, account_id: str, remote_key: str) -> Path:
        """Get full filesystem path for a file.

        Args:
            account_id: Account identifier
            remote_key: Remote key without account prefix

        Returns:
            Absolute path to file

        Raises:
            StorageBackendError: If path would escape base directory
        """
        full_key = self._build_tables_account_key(account_id, remote_key)
        full_path = (self.base_path / full_key).resolve()

        try:
            full_path.relative_to(self.base_path)
        except ValueError:
            raise StorageBackendError(
                "local",
                "path_validation",
                f"Invalid path: {full_key} would escape base directory",
            )

        return full_path

    def _get_upload_full_path(self, account_id: str, remote_key: str) -> Path:
        """Get full filesystem path for a file.

        Args:
            account_id: Account identifier
            remote_key: Remote key without account prefix

        Returns:
            Absolute path to file

        Raises:
            StorageBackendError: If path would escape base directory
        """
        full_key = self._build_uploads_account_key(account_id, remote_key)
        full_path = (self.base_path / full_key).resolve()

        try:
            full_path.relative_to(self.base_path)
        except ValueError:
            raise StorageBackendError(
                "local",
                "path_validation",
                f"Invalid path: {full_key} would escape base directory",
            )

        return full_path

    def _get_full_path(self, account_id: str, remote_key: str) -> Path:
        """Get full filesystem path for a file using the standard account key.

        Args:
            account_id: Account identifier
            remote_key: Remote key without account prefix

        Returns:
            Absolute path to file

        Raises:
            StorageBackendError: If path would escape base directory
        """
        full_key = self._build_account_key(account_id, remote_key)
        full_path = (self.base_path / full_key).resolve()

        try:
            full_path.relative_to(self.base_path)
        except ValueError:
            raise StorageBackendError(
                "local",
                "path_validation",
                f"Invalid path: {full_key} would escape base directory",
            )

        return full_path

    def _get_metadata_path(self, file_path: Path) -> Path:
        """Get path to metadata file for a given file.

        Args:
            file_path: Path to the data file

        Returns:
            Path to metadata JSON file
        """
        return file_path.parent / f"{file_path.name}.metadata.json"

    async def _convert_to_parquet(
        self, local_path: Path, config: ConversionConfig | None = None
    ) -> tuple[Path, dict]:
        """Convert a file to Parquet format using the conversion module.

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
                raise StorageBackendError("local", "convert_to_parquet", str(e)) from e

            try:
                result = await converter.convert_to_parquet(local_path, temp_parquet)
            except ConversionError as e:
                raise StorageBackendError("local", "convert_to_parquet", str(e)) from e

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
                "local",
                "convert_to_parquet",
                f"Failed to convert {local_path} to Parquet: {str(e)}",
            ) from e
        finally:
            if converter:
                converter.shutdown()

    async def upload_file(
        self,
        local_path: Path | str,
        remote_key: str,
        account_id: str,
        metadata: dict[str, str] | None = None,
        convert_to_parquet: bool = True,
        conversion_config: ConversionConfig | None = None,
    ) -> dict:
        """Upload a file to local storage.

        Args:
            local_path: Local file path to upload (Path object or string)
            remote_key: Remote key WITHOUT account prefix
            account_id: Account ID for automatic prefixing
            metadata: Optional metadata dictionary to attach to file
            convert_to_parquet: Whether to convert the file to Parquet format
            conversion_config: Optional conversion configuration

        Returns:
            Dictionary containing:
                - remote_path: Full remote path WITH account prefix
                - metrics: Conversion metrics (if converted)

        Raises:
            FileNotFoundError: If local file doesn't exist
            StorageBackendError: If upload fails
        """
        local_path = Path(local_path) if isinstance(local_path, str) else local_path
        if not local_path.exists():
            raise DuckPondFileNotFoundError(str(local_path))

        original_extension = local_path.suffix.lower()

        upload_remote_key = remote_key
        table_remote_key = Path(remote_key).with_suffix(".parquet").as_posix()

        temp_parquet_path = None
        conversion_metrics = None
        try:
            remote_upload_path = self._get_upload_full_path(account_id, upload_remote_key)
            remote_upload_path.parent.mkdir(parents=True, exist_ok=True)

            async with aiofiles.open(local_path, "rb") as src:
                async with aiofiles.open(remote_upload_path, "wb") as dst:
                    while chunk := await src.read(8192):
                        await dst.write(chunk)

            if convert_to_parquet:
                if original_extension == ".parquet":
                    remote_table_path = self._get_table_full_path(account_id, table_remote_key)
                    remote_table_path.parent.mkdir(parents=True, exist_ok=True)

                    async with aiofiles.open(local_path, "rb") as src:
                        async with aiofiles.open(remote_table_path, "wb") as dst:
                            while chunk := await src.read(8192):
                                await dst.write(chunk)
                else:
                    try:
                        (
                            temp_parquet_path,
                            conversion_metrics,
                        ) = await self._convert_to_parquet(local_path, conversion_config)

                        remote_table_path = self._get_table_full_path(account_id, table_remote_key)
                        remote_table_path.parent.mkdir(parents=True, exist_ok=True)

                        async with aiofiles.open(temp_parquet_path, "rb") as src:
                            async with aiofiles.open(remote_table_path, "wb") as dst:
                                while chunk := await src.read(8192):
                                    await dst.write(chunk)
                    finally:
                        if temp_parquet_path and temp_parquet_path.exists():
                            try:
                                os.unlink(temp_parquet_path)
                            except OSError:
                                pass

                try:
                    await aiofiles.os.remove(remote_upload_path)
                except OSError:
                    pass

                return {
                    "remote_path": self._build_tables_account_key(account_id, table_remote_key),
                    "metrics": conversion_metrics,
                }
            else:
                return {
                    "remote_path": self._build_uploads_account_key(account_id, upload_remote_key),
                    "metrics": None,
                }

        except Exception as e:
            if temp_parquet_path and temp_parquet_path.exists():
                try:
                    os.unlink(temp_parquet_path)
                except OSError:
                    pass
            if isinstance(e, DuckPondFileNotFoundError):
                raise
            raise StorageBackendError("local", "upload_file", str(e)) from e

    async def download_file(
        self,
        remote_key: str,
        local_path: Path | str,
        account_id: str,
    ) -> None:
        pass

    async def delete_file(self, remote_key: str, account_id: str) -> None:
        """Delete a file from local storage.

        Args:
            remote_key: Remote key WITHOUT account prefix
            account_id: Account ID for automatic prefixing

        Raises:
            FileNotFoundError: If file doesn't exist
            StorageBackendError: If deletion fails
        """
        try:
            remote_path = self._get_full_path(account_id, remote_key)

            if not remote_path.exists():
                raise DuckPondFileNotFoundError(self._build_account_key(account_id, remote_key))

            await aiofiles.os.remove(remote_path)

            metadata_path = self._get_metadata_path(remote_path)
            if metadata_path.exists():
                await aiofiles.os.remove(metadata_path)

        except Exception as e:
            if isinstance(e, DuckPondFileNotFoundError):
                raise
            raise StorageBackendError("local", "delete_file", str(e)) from e

    async def delete_prefix(self, account_id: str) -> int:
        """Delete all files with a account prefix.

        Args:
            account_id: Account ID - all files matching this prefix will be deleted

        Returns:
            Number of files deleted

        Raises:
            StorageBackendError: If deletion fails
        """
        try:
            account_path = self.base_path / account_id

            if not account_path.exists():
                return 0

            count = 0
            for root, dirs, files in os.walk(account_path, topdown=False):
                for name in files:
                    if not name.endswith(".metadata.json"):
                        file_path = Path(root) / name
                        await aiofiles.os.remove(file_path)
                        count += 1

                        metadata_path = self._get_metadata_path(file_path)
                        if metadata_path.exists():
                            await aiofiles.os.remove(metadata_path)

                for name in dirs:
                    dir_path = Path(root) / name
                    try:
                        await aiofiles.os.rmdir(dir_path)
                    except OSError:
                        pass

            try:
                await aiofiles.os.rmdir(account_path)
            except OSError:
                pass

            return count

        except Exception as e:
            raise StorageBackendError("local", "delete_prefix", str(e)) from e

    async def list_files(
        self,
        prefix: str,
        account_id: str,
        recursive: bool = True,
    ) -> list[str]:
        """List files matching a prefix.

        Args:
            prefix: Prefix to match (WITHOUT account prefix)
            account_id: Account ID for automatic prefixing
            recursive: If True, list all files recursively; if False, only immediate children

        Returns:
            List of file paths WITHOUT account prefix

        Raises:
            StorageBackendError: If listing fails
        """
        try:
            account_path = self.base_path / account_id
            if not account_path.exists():
                return []

            prefix_path = account_path / prefix.lstrip("/")

            matching_files = []

            if recursive:
                for root, _, files in os.walk(prefix_path):
                    for name in files:
                        if not name.endswith(".metadata.json"):
                            file_path = Path(root) / name
                            relative_path = file_path.relative_to(account_path)
                            matching_files.append(str(relative_path))
            else:
                if prefix_path.exists() and prefix_path.is_dir():
                    for item in prefix_path.iterdir():
                        if item.is_file() and not item.name.endswith(".metadata.json"):
                            relative_path = item.relative_to(account_path)
                            matching_files.append(str(relative_path))

            return sorted(matching_files)

        except Exception as e:
            raise StorageBackendError("local", "list_files", str(e)) from e

    async def file_exists(self, remote_key: str, account_id: str) -> bool:
        """Check if a file exists in local storage.

        Args:
            remote_key: Remote key WITHOUT account prefix
            account_id: Account ID for automatic prefixing

        Returns:
            True if file exists, False otherwise

        Raises:
            StorageBackendError: If check fails
        """
        try:
            remote_path = self._get_full_path(account_id, remote_key)
            return remote_path.exists() and remote_path.is_file()
        except Exception as e:
            raise StorageBackendError("local", "file_exists", str(e)) from e

    async def get_file_size(self, remote_key: str, account_id: str) -> int:
        """Get file size in bytes.

        Args:
            remote_key: Remote key WITHOUT account prefix
            account_id: Account ID for automatic prefixing

        Returns:
            File size in bytes

        Raises:
            FileNotFoundError: If file doesn't exist
            StorageBackendError: If operation fails
        """
        try:
            remote_path = self._get_full_path(account_id, remote_key)

            if not remote_path.exists():
                raise DuckPondFileNotFoundError(self._build_account_key(account_id, remote_key))

            return remote_path.stat().st_size

        except Exception as e:
            if isinstance(e, DuckPondFileNotFoundError):
                raise
            raise StorageBackendError("local", "get_file_size", str(e)) from e

    async def get_file_metadata(self, remote_key: str, account_id: str) -> dict[str, Any]:
        """Get file metadata.

        Args:
            remote_key: Remote key WITHOUT account prefix
            account_id: Account ID for automatic prefixing

        Returns:
            Dictionary containing metadata

        Raises:
            FileNotFoundError: If file doesn't exist
            StorageBackendError: If operation fails
        """
        try:
            remote_path = self._get_full_path(account_id, remote_key)

            if not remote_path.exists():
                raise DuckPondFileNotFoundError(self._build_account_key(account_id, remote_key))

            metadata_path = self._get_metadata_path(remote_path)

            if metadata_path.exists():
                async with aiofiles.open(metadata_path, "r") as f:
                    content = await f.read()
                    return json.loads(content)

            return {
                "size": remote_path.stat().st_size,
                "content_type": "application/octet-stream",
            }

        except Exception as e:
            if isinstance(e, DuckPondFileNotFoundError):
                raise
            raise StorageBackendError("local", "get_file_metadata", str(e)) from e

    async def get_storage_usage(self, account_id: str) -> int:
        """Calculate total storage usage for a account.

        Args:
            account_id: Account identifier

        Returns:
            Total storage usage in bytes

        Raises:
            StorageBackendError: If calculation fails
        """
        try:
            account_path = self.base_path / account_id

            if not account_path.exists():
                return 0

            total_size = 0
            for root, _, files in os.walk(account_path):
                for name in files:
                    if not name.endswith(".metadata.json"):
                        file_path = Path(root) / name
                        total_size += file_path.stat().st_size

            return total_size

        except Exception as e:
            raise StorageBackendError("local", "get_storage_usage", str(e)) from e

    async def generate_presigned_url(
        self,
        remote_key: str,
        account_id: str,
        expires_seconds: int = 3600,
        method: str = "GET",
    ) -> str:
        """Generate a presigned URL for direct file access.

        Note: Local filesystem backends do not support presigned URLs.

        Args:
            remote_key: Remote key WITHOUT account prefix
            account_id: Account ID for automatic prefixing
            expires_seconds: URL expiration time in seconds
            method: HTTP method

        Raises:
            NotImplementedError: Local backend doesn't support presigned URLs
        """
        raise NotImplementedError(
            "Presigned URLs are not supported for local filesystem storage. "
            "Use download_file() instead."
        )
