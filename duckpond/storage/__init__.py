"""Storage backend system for DuckPond.

This module provides storage abstraction with support for:
- Local filesystem storage
- S3-compatible cloud storage (future)
- Account isolation through automatic key prefixing
- Async operations for consistent API

Usage:
    backend = get_storage_backend("local", {"path": "/data/duckpond"})
    await backend.upload_file(Path("local.csv"), "data/file.csv", "account-123")
"""

from pathlib import Path

from duckpond.storage.backend import MockStorageBackend, StorageBackend

__all__ = [
    "StorageBackend",
    "MockStorageBackend",
    "get_storage_backend",
]


def get_storage_backend(
    backend_type: str,
    config: dict[str, str] | None = None,
) -> StorageBackend:
    """Create a storage backend instance.

    Args:
        backend_type: Type of backend ("local", "s3", "mock")
        config: Backend-specific configuration dictionary

    Returns:
        Configured StorageBackend instance

    Raises:
        ValueError: If backend_type is unknown or config is invalid

    Examples:
        Local filesystem backend:
        >>> backend = get_storage_backend("local", {"path": "/data/duckpond"})

        S3 backend:
        >>> backend = get_storage_backend("s3", {
        ...     "bucket": "my-bucket",
        ...     "region": "us-east-1",
        ...     "endpoint_url": "https://s3.amazonaws.com"
        ... })

        Mock backend (for testing):
        >>> backend = get_storage_backend("mock")
    """
    config = config or {}

    if backend_type == "local":
        from duckpond.storage.local_backend import LocalBackend

        path = config.get("path", "~/.duckpond/data")
        return LocalBackend(Path(path))

    elif backend_type == "s3":
        from duckpond.storage.s3_backend import S3Backend

        print(f"config: {config}")
        if "bucket" not in config:
            raise ValueError("S3 backend requires 'bucket' in config")

        return S3Backend(
            bucket=config["bucket"],
            region=config.get("region", "us-east-1"),
            endpoint_url=config.get("endpoint_url"),
            aws_access_key_id=config.get("aws_access_key_id"),
            aws_secret_access_key=config.get("aws_secret_access_key"),
        )

    elif backend_type == "mock":
        return MockStorageBackend()

    else:
        raise ValueError(
            f"Unknown storage backend: {backend_type}. Supported backends: local, s3, mock"
        )
