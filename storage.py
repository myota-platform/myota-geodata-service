"""S3-compatible storage adapter for geodata source objects."""
from __future__ import annotations

import hashlib
import os
import urllib.request
from pathlib import Path


class ObjectStore:
    """Store imported source bytes in SeaweedFS or another S3-compatible service."""

    def __init__(self) -> None:
        self.endpoint = os.environ.get("MYOTA_OBJECT_STORAGE_ENDPOINT", "http://seaweedfs:8333")
        self.access_key = os.environ.get("MYOTA_OBJECT_STORAGE_ACCESS_KEY", "myota-s3")
        self.secret_key = os.environ.get("MYOTA_OBJECT_STORAGE_SECRET_KEY", "myota-s3-dev-only")
        self.region = os.environ.get("MYOTA_OBJECT_STORAGE_REGION", "us-east-1")
        self.addressing_style = os.environ.get("MYOTA_OBJECT_STORAGE_ADDRESSING_STYLE", "path")
        local_root = os.environ.get("MYOTA_OBJECT_STORAGE_LOCAL_DIR", "")
        self.local_root = Path(local_root) if local_root else None
        self._client = None

    def _s3(self):
        if self.local_root or self._client is not None:
            return self._client
        try:
            import boto3
            from botocore.client import Config
        except ImportError:
            return None
        self._client = boto3.client(
            "s3",
            endpoint_url=self.endpoint,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            region_name=self.region,
            config=Config(signature_version="s3v4", s3={"addressing_style": self.addressing_style}),
        )
        return self._client

    @staticmethod
    def scan_content(content: bytes, filename: str = "upload") -> dict[str, object]:
        max_bytes = int(os.environ.get("MYOTA_UPLOAD_MAX_BYTES", str(25 * 1024 * 1024)))
        if len(content) > max_bytes:
            raise ValueError(f"{filename} exceeds the configured upload limit")
        if b"X5O!P%@AP[4\\PZX54(P^)7CC)7}$EICAR-STANDARD-ANTIVIRUS-TEST-FILE!$H+H*" in content:
            raise ValueError("malware scan rejected the upload")
        scanner_url = os.environ.get("MYOTA_CLAMAV_URL", "").strip()
        if scanner_url:
            request = urllib.request.Request(scanner_url, data=content, method="POST", headers={
                "Content-Type": "application/octet-stream", "X-Upload-Name": filename})
            try:
                with urllib.request.urlopen(request, timeout=10) as response:
                    if response.status >= 300:
                        raise ValueError("malware scanner rejected the upload")
            except Exception as exc:
                raise ValueError("malware scanner is unavailable; upload was not stored") from exc
        return {"status": "CLEAN", "sha256": hashlib.sha256(content).hexdigest(), "size": len(content)}

    def _local_path(self, bucket: str, object_key: str) -> Path:
        if not self.local_root:
            raise RuntimeError("local object storage is not configured")
        path = self.local_root / bucket / object_key
        path.parent.mkdir(parents=True, exist_ok=True)
        return path

    @staticmethod
    def _ensure_bucket(client: object, bucket: str) -> None:
        from botocore.exceptions import ClientError
        try:
            client.head_bucket(Bucket=bucket)
            return
        except ClientError as exc:
            error = exc.response.get("Error", {})
            status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            code = str(error.get("Code", ""))
            if status != 404 and code not in {"404", "NoSuchBucket", "NotFound"}:
                raise
        client.create_bucket(Bucket=bucket)

    def put(self, bucket: str, object_key: str, content: bytes, content_type: str) -> dict[str, object]:
        checksum = hashlib.sha256(content).hexdigest()
        if self.local_root:
            self._local_path(bucket, object_key).write_bytes(content)
        else:
            client = self._s3()
            if not client:
                raise RuntimeError("boto3 is not installed")
            self._ensure_bucket(client, bucket)
            client.put_object(Bucket=bucket, Key=object_key, Body=content, ContentType=content_type)
        return {"sha256": checksum, "size": len(content), "storedAt": object_key}
