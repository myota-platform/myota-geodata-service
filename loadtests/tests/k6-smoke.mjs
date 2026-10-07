// Real k6 transport against an ephemeral local protocol fixture, never production.
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { createHash, randomUUID } from 'node:crypto';
import { mkdtempSync, rmSync } from 'node:fs';
import { createServer } from 'node:http';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';

const sha256 = value => createHash('sha256').update(value).digest('hex');
const uploads = new Map();
let partsReceived = 0, cleanups = 0;
const server = createServer(async (request, response) => {
  const reply = (status, payload) => {
    response.writeHead(status, { 'Content-Type': 'application/json' });
    response.end(JSON.stringify(payload));
  };
  try {
    const chunks = [];
    for await (const chunk of request) chunks.push(chunk);
    const body = Buffer.concat(chunks);
    const path = request.url;
    if (path === '/v1/identity/auth/login') {
      reply(200, { accessToken: 'local-smoke-token' });
      return;
    }
    assert.equal(request.headers.authorization, 'Bearer local-smoke-token');
    if (path.startsWith('/v1/geodata/load-test-runs/')) {
      assert.equal(request.method, 'DELETE');
      const tag = decodeURIComponent(path.split('/').at(-1));
      assert.equal(JSON.parse(body).confirmation, `DELETE LOAD TEST DATA ${tag}`);
      let removed = 0;
      for (const [id, upload] of uploads) {
        if (upload.metadata.source.loadTestRunId !== tag) continue;
        assert.equal(upload.status, 'COMPLETED');
        uploads.delete(id);
        removed++;
      }
      cleanups++;
      reply(200, { cleaned: true, importsDeleted: removed, uploadSessionsDeleted: removed });
      return;
    }
    if (path === '/v1/geodata/import-uploads') {
      assert.equal(request.method, 'POST');
      const metadata = JSON.parse(body);
      assert.ok(metadata.source.loadTestRunId.startsWith('lt-'));
      assert.ok(metadata.expectedSize > 5 * 1024 * 1024);
      assert.ok(request.headers['idempotency-key']);
      const uploadId = randomUUID();
      uploads.set(uploadId, { metadata, parts: [], status: 'UPLOADING' });
      reply(201, { uploadId, partSizeBytes: 5 * 1024 * 1024 });
      return;
    }
    const match = path.match(/^\/v1\/geodata\/import-uploads\/([^/]+)\/(parts\/(\d+)|complete)$/);
    assert.ok(match, `Unexpected endpoint ${path}`);
    const upload = uploads.get(match[1]);
    assert.ok(upload);
    if (match[2] === 'complete') {
      const source = Buffer.concat(upload.parts);
      assert.equal(source.length, upload.metadata.expectedSize);
      assert.equal(sha256(source), upload.metadata.sha256);
      assert.equal(JSON.parse(source).features.length, 4500);
      upload.status = 'COMPLETED';
      reply(202, { status: 'COMPLETED', importRun: { id: randomUUID() } });
    } else {
      assert.equal(request.headers['content-type'], 'application/octet-stream');
      assert.equal(sha256(body), request.headers['x-part-sha256']);
      assert.ok(body.length <= 5 * 1024 * 1024);
      upload.parts[Number(match[3]) - 1] = body;
      partsReceived++;
      reply(200, { sizeBytes: body.length, sha256: sha256(body) });
    }
  } catch (error) {
    console.error(error.message);
    reply(400, { detail: error.message });
  }
});

await new Promise((resolve, reject) => {
  server.once('error', reject);
  server.listen(0, '127.0.0.1', resolve);
});
const configDirectory = mkdtempSync(join(tmpdir(), 'myota-k6-smoke-'));
try {
  const child = spawn('k6', ['run', '--quiet', fileURLToPath(new URL('../geodata-workloads.js', import.meta.url))], {
    stdio: 'inherit',
    env: {
      PATH: process.env.PATH,
      XDG_CONFIG_HOME: configDirectory, K6_NO_USAGE_REPORT: 'true',
      MYOTA_ENV: 'development', MYOTA_LOAD_TEST_ALLOW_NONPROD: 'YES',
      MYOTA_BASE_URL: `http://127.0.0.1:${server.address().port}`,
      MYOTA_LOAD_TEST_EMAIL: 'smoke@example.test', MYOTA_LOAD_TEST_PASSWORD: 'local-fixture-only',
      MYOTA_LOAD_TEST_PROFILE: 'large-upload', MYOTA_LOAD_TEST_VUS: '1',
      MYOTA_LOAD_TEST_DURATION: '30s', MYOTA_LOAD_TEST_FEATURES: '4500',
      MYOTA_LOAD_TEST_PADDING_BYTES: '1500',
    },
  });
  const timeout = setTimeout(() => child.kill('SIGTERM'), 80000);
  try {
    const code = await new Promise((resolve, reject) => {
      child.once('error', reject);
      child.once('exit', resolve);
    });
    assert.equal(code, 0, 'Actual k6 upload smoke must pass its thresholds');
    assert.equal(partsReceived, 2);
    assert.equal(cleanups, 1);
    assert.equal(uploads.size, 0, 'Teardown must remove every fixture upload');
    console.log('Local k6 transport smoke passed; no fixtures remain.');
  } finally {
    clearTimeout(timeout);
  }
} finally {
  await new Promise(resolve => server.close(resolve));
  rmSync(configDirectory, { recursive: true, force: true });
}
