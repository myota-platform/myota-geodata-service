-- Process-independent upload sessions backed by S3 multipart storage.
CREATE TABLE IF NOT EXISTS geodata_upload_session (
  id uuid PRIMARY KEY,
  owner_subject text NOT NULL,
  idempotency_key text NOT NULL,
  filename text NOT NULL,
  metadata jsonb NOT NULL,
  bucket text NOT NULL,
  object_key text NOT NULL UNIQUE,
  multipart_upload_id text NOT NULL,
  expected_size bigint NOT NULL CHECK (expected_size > 0),
  expected_sha256 text,
  status text NOT NULL CHECK (status IN ('UPLOADING', 'COMPLETING', 'COMPLETED', 'ABORTED', 'EXPIRED', 'FAILED')),
  import_run_id uuid REFERENCES import_run(id) ON DELETE SET NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  expires_at timestamptz NOT NULL,
  completed_at timestamptz
);

CREATE INDEX IF NOT EXISTS geodata_upload_session_expiry_idx
  ON geodata_upload_session (expires_at)
  WHERE status IN ('UPLOADING', 'COMPLETING');

CREATE INDEX IF NOT EXISTS geodata_upload_session_owner_idx
  ON geodata_upload_session (owner_subject, created_at DESC);

CREATE UNIQUE INDEX IF NOT EXISTS geodata_upload_session_idempotency_idx
  ON geodata_upload_session (owner_subject, idempotency_key);

CREATE TABLE IF NOT EXISTS geodata_upload_part (
  upload_session_id uuid NOT NULL REFERENCES geodata_upload_session(id) ON DELETE CASCADE,
  part_number integer NOT NULL CHECK (part_number BETWEEN 1 AND 10000),
  size_bytes bigint NOT NULL CHECK (size_bytes > 0),
  sha256 text NOT NULL,
  etag text NOT NULL,
  uploaded_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (upload_session_id, part_number)
);

COMMENT ON TABLE geodata_upload_session IS
  'Restart-safe browser upload sessions. Object bytes live in S3-compatible storage, never a shared API pod volume.';
