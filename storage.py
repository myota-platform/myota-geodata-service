"""S3-compatible storage adapter for geodata source objects."""

from __future__ import annotations

import hashlib
import os
import shutil
import urllib.request
from pathlib import Path


class ObjectStore:
    """Store imported source bytes in SeaweedFS or another S3-compatible service."""

    def __init__(self) -> None:
        self.endpoint = os.environ.get(
            "MYOTA_OBJECT_STORAGE_ENDPOINT", "http://seaweedfs:8333"
        )
        self.access_key = os.environ.get(
            "MYOTA_OBJECT_STORAGE_ACCESS_KEY", "myota-s3"
        )
        self.secret_key = os.environ.get(
            "MYOTA_OBJECT_STORAGE_SECRET_KEY", "myota-s3-dev-only"
        )
        self.region = os.environ.get(
            "MYOTA_OBJECT_STORAGE_REGION", "us-east-1"
        )
        self.addressing_style = os.environ.get(
            "MYOTA_OBJECT_STORAGE_ADDRESSING_STYLE", "path"
        )
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
            config=Config(
                signature_version="s3v4",
                s3={"addressing_style": self.addressing_style},
            ),
        )
        return self._client

    @staticmethod
    def scan_content(
        content: bytes, filename: str = "upload"
    ) -> dict[str, object]:
        max_bytes = int(
            os.environ.get("MYOTA_UPLOAD_MAX_BYTES", str(1024 * 1024 * 1024))
        )
        if len(content) > max_bytes:
            raise ValueError(f"{filename} exceeds the configured upload limit")
        if (
            b"X5O!P%@AP[4\\PZX54(P^)7CC)7}$EICAR-STANDARD-ANTIVIRUS-TEST-FILE!$H+H*"
            in content
        ):
            raise ValueError("malware scan rejected the upload")
        scanner_url = os.environ.get("MYOTA_CLAMAV_URL", "").strip()
        if scanner_url:
            request = urllib.request.Request(
                scanner_url,
                data=content,
                method="POST",
                headers={
                    "Content-Type": "application/octet-stream",
                    "X-Upload-Name": filename,
                },
            )
            try:
                with urllib.request.urlopen(request, timeout=10) as response:
                    if response.status >= 300:
                        raise ValueError("malware scanner rejected the upload")
            except Exception as exc:
                raise ValueError(
                    "malware scanner is unavailable; upload was not stored"
                ) from exc
        return {
            "status": "CLEAN",
            "sha256": hashlib.sha256(content).hexdigest(),
            "size": len(content),
        }

    @staticmethod
    def scan_path(
        path: str | Path, filename: str = "upload"
    ) -> dict[str, object]:
        """Scan a spooled upload incrementally, without loading it into RAM."""
        max_bytes = int(
            os.environ.get("MYOTA_UPLOAD_MAX_BYTES", str(1024 * 1024 * 1024))
        )
        size = Path(path).stat().st_size
        if size > max_bytes:
            raise ValueError(f"{filename} exceeds the configured upload limit")
        marker = b"X5O!P%@AP[4\\PZX54(P^)7CC)7}$EICAR-STANDARD-ANTIVIRUS-TEST-FILE!$H+H*"
        digest = hashlib.sha256()
        found = False
        previous = b""
        with Path(path).open("rb") as source:
            while chunk := source.read(8 * 1024 * 1024):
                digest.update(chunk)
                found = found or marker in previous + chunk
                previous = (previous + chunk)[-len(marker) :]
        if found:
            raise ValueError("malware scan rejected the upload")
        return {"status": "CLEAN", "sha256": digest.hexdigest(), "size": size}

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
            status = exc.response.get("ResponseMetadata", {}).get(
                "HTTPStatusCode"
            )
            code = str(error.get("Code", ""))
            if status != 404 and code not in {
                "404",
                "NoSuchBucket",
                "NotFound",
            }:
                raise
        client.create_bucket(Bucket=bucket)

    def put(
        self, bucket: str, object_key: str, content: bytes, content_type: str
    ) -> dict[str, object]:
        checksum = hashlib.sha256(content).hexdigest()
        if self.local_root:
            self._local_path(bucket, object_key).write_bytes(content)
        else:
            client = self._s3()
            if not client:
                raise RuntimeError("boto3 is not installed")
            self._ensure_bucket(client, bucket)
            client.put_object(
                Bucket=bucket,
                Key=object_key,
                Body=content,
                ContentType=content_type,
            )
        return {
            "sha256": checksum,
            "size": len(content),
            "storedAt": object_key,
        }

    def put_file(
        self,
        bucket: str,
        object_key: str,
        path: str | Path,
        content_type: str,
        sha256: str | None = None,
        size: int | None = None,
    ) -> dict[str, object]:
        """Upload a file using the S3 multipart-capable client path."""
        source = Path(path)
        size = source.stat().st_size if size is None else size
        if sha256 is None:
            digest = hashlib.sha256()
            with source.open("rb") as stream:
                while chunk := stream.read(8 * 1024 * 1024):
                    digest.update(chunk)
            sha256 = digest.hexdigest()
        if self.local_root:
            shutil.copyfile(source, self._local_path(bucket, object_key))
        else:
            client = self._s3()
            if not client:
                raise RuntimeError("boto3 is not installed")
            self._ensure_bucket(client, bucket)
            # SeaweedFS accepts a streaming PUT reliably for large source
            # objects. Avoid boto3's multipart-finalization path here; it can
            # leave a browser request pending without creating the object.
            with source.open("rb") as stream:
                client.put_object(
                    Bucket=bucket,
                    Key=object_key,
                    Body=stream,
                    ContentLength=size,
                    ContentType=content_type,
                )
        return {"sha256": sha256, "size": size, "storedAt": object_key}

    def create_multipart(
        self, bucket: str, object_key: str, content_type: str
    ) -> str:
        """Start a resumable S3 multipart upload and return its opaque ID."""
        client = self._s3()
        if not client:
            raise RuntimeError(
                "resumable uploads require an S3-compatible object store"
            )
        self._ensure_bucket(client, bucket)
        result = client.create_multipart_upload(
            Bucket=bucket, Key=object_key, ContentType=content_type
        )
        return str(result["UploadId"])

    def upload_part(
        self,
        bucket: str,
        object_key: str,
        upload_id: str,
        part_number: int,
        path: str | Path,
    ) -> str:
        """Upload one bounded request part, returning the S3 ETag."""
        client = self._s3()
        if not client:
            raise RuntimeError("S3-compatible object storage is unavailable")
        with Path(path).open("rb") as stream:
            result = client.upload_part(
                Bucket=bucket,
                Key=object_key,
                UploadId=upload_id,
                PartNumber=part_number,
                Body=stream,
                ContentLength=Path(path).stat().st_size,
            )
        return str(result["ETag"])

    def complete_multipart(
        self,
        bucket: str,
        object_key: str,
        upload_id: str,
        parts: list[dict[str, object]],
    ) -> dict[str, object]:
        """Atomically publish the assembled source object."""
        client = self._s3()
        if not client:
            raise RuntimeError("S3-compatible object storage is unavailable")
        result = client.complete_multipart_upload(
            Bucket=bucket,
            Key=object_key,
            UploadId=upload_id,
            MultipartUpload={
                "Parts": [
                    {
                        "PartNumber": int(part["partNumber"]),
                        "ETag": str(part["etag"]),
                    }
                    for part in parts
                ]
            },
        )
        return {"etag": result.get("ETag"), "storedAt": object_key}

    def abort_multipart(
        self, bucket: str, object_key: str, upload_id: str
    ) -> None:
        client = self._s3()
        if client:
            client.abort_multipart_upload(
                Bucket=bucket, Key=object_key, UploadId=upload_id
            )

    def scan_object(
        self, bucket: str, object_key: str, filename: str
    ) -> dict[str, object]:
        """Hash and malware-check a completed object with bounded memory."""
        if self.local_root:
            path = self._local_path(bucket, object_key)
            return self.scan_path(path, filename)
        client = self._s3()
        if not client:
            raise RuntimeError("S3-compatible object storage is unavailable")
        head = client.head_object(Bucket=bucket, Key=object_key)
        size = int(head["ContentLength"])
        maximum = int(os.environ.get("MYOTA_UPLOAD_MAX_BYTES", str(1024**3)))
        if size > maximum:
            raise ValueError(f"{filename} exceeds the configured upload limit")
        marker = b"X5O!P%@AP[4\\PZX54(P^)7CC)7}$EICAR-STANDARD-ANTIVIRUS-TEST-FILE!$H+H*"
        digest = hashlib.sha256()
        found = False
        previous = b""
        response = client.get_object(Bucket=bucket, Key=object_key)
        try:
            while chunk := response["Body"].read(8 * 1024 * 1024):
                digest.update(chunk)
                found = found or marker in previous + chunk
                previous = (previous + chunk)[-len(marker) :]
        finally:
            response["Body"].close()
        if found:
            raise ValueError("malware scan rejected the upload")

        scanner_url = os.environ.get("MYOTA_CLAMAV_URL", "").strip()
        if scanner_url:
            response = client.get_object(Bucket=bucket, Key=object_key)
            source = response["Body"]
            request = urllib.request.Request(
                scanner_url,
                data=source,
                method="POST",
                headers={
                    "Content-Type": "application/octet-stream",
                    "Content-Length": str(size),
                    "X-Upload-Name": filename,
                },
            )
            try:
                with urllib.request.urlopen(request, timeout=120) as result:
                    if result.status >= 300:
                        raise ValueError("malware scanner rejected the upload")
            except Exception as exc:
                raise RuntimeError(
                    "malware scanner is unavailable; import was not queued"
                ) from exc
            finally:
                source.close()
        return {"status": "CLEAN", "sha256": digest.hexdigest(), "size": size}

    def get(self, bucket: str, object_key: str) -> bytes | None:
        """Read a durable import source for restart recovery."""
        if self.local_root:
            path = self._local_path(bucket, object_key)
            return path.read_bytes() if path.exists() else None
        client = self._s3()
        if not client:
            return None
        try:
            response = client.get_object(Bucket=bucket, Key=object_key)
            return response["Body"].read()
        except Exception:
            return None

    def delete(self, bucket: str, object_key: str) -> None:
        """Delete one object; missing objects are treated as already cleaned up."""
        if self.local_root:
            root = self.local_root.resolve()
            if (
                not bucket
                or "/" in bucket
                or "\\" in bucket
                or bucket in {".", ".."}
            ):
                raise ValueError("bucket must be a single safe path component")
            key = Path(object_key)
            if key.is_absolute() or ".." in key.parts:
                raise ValueError(
                    "object key must be a relative path without parent traversal"
                )
            bucket_root = (root / bucket).resolve()
            if not bucket_root.is_relative_to(root):
                raise ValueError(
                    "bucket escapes the local object storage root"
                )
            path = (bucket_root / key).resolve()
            if not path.is_relative_to(bucket_root):
                raise ValueError("object key escapes its bucket")
            path.unlink(missing_ok=True)
            parent = path.parent
            while parent != bucket_root:
                try:
                    parent.rmdir()
                except OSError:
                    break
                parent = parent.parent
            return
        client = self._s3()
        if not client:
            raise RuntimeError("boto3 is not installed")
        client.delete_object(Bucket=bucket, Key=object_key)
