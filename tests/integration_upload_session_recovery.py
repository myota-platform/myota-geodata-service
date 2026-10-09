"""Failure-injection check for resumable uploads using an isolated SeaweedFS."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import subprocess
import sys
import time
import unittest
import urllib.error
import urllib.request
import uuid
from typing import Any

import boto3
import psycopg
from botocore.client import Config

API_URL = os.environ.get("UPLOAD_RECOVERY_API_URL", "http://127.0.0.1:18023")
DATABASE_URL = os.environ["GEO_DATABASE_URL"]
SEAWEED_CONTAINER = os.environ.get(
    "SEAWEEDFS_TEST_CONTAINER", "myota-seaweedfs-phase2"
)
BUCKET = "myota-geodata-imports"
OWNER = "phase2-upload-recovery-owner"
SIGNING_KEY = "phase2-isolated-test-key"
os.environ.setdefault("MYOTA_AUTH_SIGNING_KEY", SIGNING_KEY)


def _sign_token(subject: str) -> str:
    header = {"alg": "HS256", "typ": "JWT", "tokenType": "access"}
    claims: dict[str, Any] = {
        "sub": subject,
        "scp": ["geodata.import", "*"],
        "exp": int(time.time()) + 3600,
    }
    if subject == OWNER:
        claims["roles"] = [{"role": "GLOBAL_OPERATOR"}]

    def encode(value: dict[str, Any]) -> str:
        return (
            base64.urlsafe_b64encode(
                json.dumps(value, separators=(",", ":")).encode()
            )
            .rstrip(b"=")
            .decode()
        )

    unsigned = f"{encode(header)}.{encode(claims)}"
    signature = hmac.new(
        SIGNING_KEY.encode(), unsigned.encode(), hashlib.sha256
    ).digest()
    encoded_signature = (
        base64.urlsafe_b64encode(signature).rstrip(b"=").decode()
    )
    return f"{unsigned}.{encoded_signature}"


AUTH_TOKEN = _sign_token(OWNER)
OTHER_TOKEN = _sign_token("phase2-upload-recovery-other-user")


def _request(
    method: str,
    path: str,
    *,
    token: str = AUTH_TOKEN,
    body: bytes | dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
) -> tuple[int, dict[str, Any] | bytes]:
    request_headers = {"Authorization": f"Bearer {token}"}
    request_headers.update(headers or {})
    data = body
    if isinstance(body, dict):
        data = json.dumps(body).encode()
        request_headers["Content-Type"] = "application/json"
    request = urllib.request.Request(
        f"{API_URL}{path}",
        data=data,
        headers=request_headers,
        method=method,
    )
    try:
        response = urllib.request.urlopen(request, timeout=30)
    except urllib.error.HTTPError as error:
        response = error
    with response:
        payload = response.read()
        if "application/json" in response.headers.get("Content-Type", ""):
            return response.status, json.loads(payload)
        return response.status, payload


class UploadSessionRecovery(unittest.TestCase):
    api_process: subprocess.Popen[bytes] | None = None

    def start_api(self) -> None:
        environment = os.environ.copy()
        environment.update(
            {
                "GEODATA_HTTP_PORT": API_URL.rsplit(":", 1)[1],
                "MYOTA_REQUIRE_DURABILITY": "1",
                "MYOTA_MAX_BODY_BYTES": str(32 * 1024 * 1024),
                "MYOTA_OBJECT_STORAGE_ENDPOINT": "http://127.0.0.1:8333",
                "MYOTA_OBJECT_STORAGE_ACCESS_KEY": "phase2-test-access",
                "MYOTA_OBJECT_STORAGE_SECRET_KEY": "phase2-test-secret",
                "MYOTA_OBJECT_STORAGE_REGION": "us-east-1",
                "MYOTA_AUTH_SIGNING_KEY": "phase2-isolated-test-key",
                "MYOTA_JETSTREAM_METRICS_STREAMS": "",
                "NATS_URL": "nats://127.0.0.1:1",
            }
        )
        self.api_process = subprocess.Popen(
            [sys.executable, "run_geodata.py"],
            env=environment,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        for _ in range(60):
            if self.api_process.poll() is not None:
                self.fail("Geodata API exited before becoming healthy")
            try:
                status, _ = _request("GET", "/healthz", token="")
                if status == 200:
                    return
            except (OSError, urllib.error.URLError):
                time.sleep(1)
        self.fail("Geodata API did not become healthy within 60 seconds")

    def stop_api(self) -> None:
        if self.api_process is None:
            return
        self.api_process.terminate()
        try:
            self.api_process.wait(timeout=15)
        except subprocess.TimeoutExpired:
            self.api_process.kill()
            self.api_process.wait(timeout=10)
        self.api_process = None

    def setUp(self) -> None:
        self.start_api()

    def tearDown(self) -> None:
        self.stop_api()

    def test_owner_resumes_parts_across_api_and_seaweedfs_restarts(
        self,
    ) -> None:
        first_part = b"a" * (5 * 1024 * 1024)
        last_part = b"b" * (1024 * 1024 + 137)
        complete_bytes = first_part + last_part
        expected_digest = hashlib.sha256(complete_bytes).hexdigest()
        idempotency_key = str(uuid.uuid4())
        status, created = _request(
            "POST",
            "/v1/geodata/import-uploads",
            body={
                "adapter": "MANUAL",
                "source": {"name": "isolated restart-recovery fixture"},
                "filename": "phase2-recovery.geojson",
                "expectedSize": len(complete_bytes),
                "sha256": expected_digest,
                "entityTypes": ["PHASE2_TEST"],
                "format": "GEOJSON",
            },
            headers={"Idempotency-Key": idempotency_key},
        )
        self.assertEqual(status, 201, created)
        upload_id = created["uploadId"]

        status, part = _request(
            "POST",
            f"/v1/geodata/import-uploads/{upload_id}/parts/1",
            body=first_part,
            headers={
                "Content-Type": "application/octet-stream",
                "X-Part-SHA256": hashlib.sha256(first_part).hexdigest(),
            },
        )
        self.assertEqual(status, 200, part)
        self.assertEqual(part["sizeBytes"], len(first_part))

        # API process termination must not lose the database session or the
        # already-acknowledged multipart part.
        self.stop_api()
        self.start_api()
        status, progress = _request(
            "GET", f"/v1/geodata/import-uploads/{upload_id}"
        )
        self.assertEqual(status, 200, progress)
        self.assertEqual(
            progress["parts"],
            [
                {
                    "partNumber": 1,
                    "sizeBytes": len(first_part),
                    "sha256": hashlib.sha256(first_part).hexdigest(),
                }
            ],
        )

        foreign_resume, _ = _request(
            "POST",
            f"/v1/geodata/import-uploads/{upload_id}/parts/1",
            token=OTHER_TOKEN,
            body=first_part,
            headers={
                "Content-Type": "application/octet-stream",
                "X-Part-SHA256": hashlib.sha256(first_part).hexdigest(),
            },
        )
        self.assertGreaterEqual(foreign_resume, 400)
        foreign_abort, _ = _request(
            "DELETE",
            f"/v1/geodata/import-uploads/{upload_id}",
            token=OTHER_TOKEN,
        )
        self.assertGreaterEqual(foreign_abort, 400)

        # Replaying an acknowledged part is idempotent and retains one row.
        status, replay = _request(
            "POST",
            f"/v1/geodata/import-uploads/{upload_id}/parts/1",
            body=first_part,
            headers={
                "Content-Type": "application/octet-stream",
                "X-Part-SHA256": hashlib.sha256(first_part).hexdigest(),
            },
        )
        self.assertEqual(status, 200, replay)
        self.assertEqual(replay["sha256"], part["sha256"])

        status, _ = _request(
            "POST",
            f"/v1/geodata/import-uploads/{upload_id}/parts/2",
            body=last_part,
            headers={
                "Content-Type": "application/octet-stream",
                "X-Part-SHA256": hashlib.sha256(last_part).hexdigest(),
            },
        )
        self.assertEqual(status, 200)

        # Restart the exact, digest-pinned SeaweedFS container. Its test-only
        # named volume preserves filer/master state; production is untouched.
        subprocess.run(
            ["docker", "restart", SEAWEED_CONTAINER],
            check=True,
            capture_output=True,
            text=True,
            timeout=60,
        )
        self._wait_for_object_store()
        status, progress = _request(
            "GET", f"/v1/geodata/import-uploads/{upload_id}"
        )
        self.assertEqual(status, 200, progress)
        self.assertEqual([p["partNumber"] for p in progress["parts"]], [1, 2])

        status, completed = _request(
            "POST", f"/v1/geodata/import-uploads/{upload_id}/complete", body={}
        )
        self.assertEqual(status, 202, completed)
        run_id = completed["importRun"]["id"]
        self.assertEqual(
            completed["importRun"]["source"]["sha256"], expected_digest
        )

        # A lost completion response may be retried without duplicate work.
        status, retry = _request(
            "POST", f"/v1/geodata/import-uploads/{upload_id}/complete", body={}
        )
        self.assertEqual(status, 202, retry)
        self.assertEqual(retry["importRun"]["id"], run_id)

        other_status, _ = _request(
            "GET",
            f"/v1/geodata/import-uploads/{upload_id}",
            token=OTHER_TOKEN,
        )
        self.assertGreaterEqual(other_status, 400)

        storage = boto3.client(
            "s3",
            endpoint_url="http://127.0.0.1:8333",
            aws_access_key_id="phase2-test-access",
            aws_secret_access_key="phase2-test-secret",
            region_name="us-east-1",
            config=Config(
                signature_version="s3v4",
                s3={"addressing_style": "path"},
            ),
        )
        session = storage.get_object(
            Bucket=BUCKET,
            Key=f"geodata-imports/{upload_id}-phase2-recovery.geojson",
        )
        self.assertEqual(
            hashlib.sha256(session["Body"].read()).hexdigest(), expected_digest
        )

        with psycopg.connect(DATABASE_URL) as connection:
            count = connection.execute(
                "SELECT count(*) FROM outbox_event "
                "WHERE aggregate_type='import_run' AND aggregate_id=%s",
                (run_id,),
            ).fetchone()[0]
            self.assertEqual(count, 1)
            row = connection.execute(
                "SELECT status, import_run_id FROM geodata_upload_session WHERE id=%s",
                (upload_id,),
            ).fetchone()
            self.assertEqual(row, ("COMPLETED", uuid.UUID(run_id)))
            metadata = connection.execute(
                "SELECT source_metadata->'source'->>'sha256' "
                "FROM import_run WHERE id=%s",
                (run_id,),
            ).fetchone()
            self.assertEqual(metadata[0], expected_digest)

        # Abort a second incomplete transfer and confirm it is not left active.
        status, abandoned = _request(
            "POST",
            "/v1/geodata/import-uploads",
            body={
                "adapter": "MANUAL",
                "source": {"name": "isolated abort fixture"},
                "filename": "phase2-abort.geojson",
                "expectedSize": 1,
                "entityTypes": ["PHASE2_TEST"],
            },
            headers={"Idempotency-Key": str(uuid.uuid4())},
        )
        self.assertEqual(status, 201, abandoned)
        status, aborted = _request(
            "DELETE",
            f"/v1/geodata/import-uploads/{abandoned['uploadId']}",
        )
        self.assertEqual(status, 200, aborted)
        self.assertEqual(aborted["status"], "ABORTED")
        active_uploads = storage.list_multipart_uploads(
            Bucket=BUCKET,
            Prefix=f"geodata-imports/{abandoned['uploadId']}",
        ).get("Uploads", [])
        self.assertFalse(
            any(
                upload["Key"].startswith(
                    f"geodata-imports/{abandoned['uploadId']}"
                )
                for upload in active_uploads
            )
        )

    @staticmethod
    def _wait_for_object_store() -> None:
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            try:
                with urllib.request.urlopen(
                    "http://127.0.0.1:8333/status", timeout=2
                ):
                    return
            except (OSError, urllib.error.URLError):
                time.sleep(1)
        raise AssertionError("SeaweedFS did not recover after its restart")


if __name__ == "__main__":
    unittest.main(verbosity=2)
