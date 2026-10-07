import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { test } from 'node:test';
import { createContext, SourceTextModule, SyntheticModule } from 'node:vm';

const source = readFileSync(new URL('../geodata-workloads.js', import.meta.url), 'utf8');
const sha256 = value => createHash('sha256').update(value).digest('hex');
const response = (status, payload) => ({
  status,
  body: typeof payload === 'string' ? payload : JSON.stringify(payload),
  json(selector) {
    const value = typeof payload === 'string' ? JSON.parse(payload) : payload;
    return selector ? selector.split('.').reduce((result, key) => result?.[key], value) : value;
  },
});

async function harness(overrides = {}, behavior = {}) {
  const requests = [], checks = [], logs = [];
  const environment = {
    MYOTA_ENV: 'development', MYOTA_LOAD_TEST_ALLOW_NONPROD: 'YES',
    MYOTA_BASE_URL: 'http://localhost:8090', MYOTA_LOAD_TEST_PROFILE: 'large-upload',
    MYOTA_LOAD_TEST_FEATURES: '2', MYOTA_LOAD_TEST_PADDING_BYTES: '0',
    MYOTA_LOAD_TEST_EMAIL: 'test-admin@example.test', MYOTA_LOAD_TEST_PASSWORD: 'not-a-real-password',
    ...overrides,
  };
  const http = {
    expectedStatuses: (...statuses) => statuses,
  };
  for (const method of ['post', 'get', 'del', 'patch']) {
    http[method] = (url, body, params) => {
      if (method === 'get') { params = body; body = null; }
      const request = { method, url, body, params };
      requests.push(request);
      if (url.endsWith('/v1/identity/auth/login')) return response(200, { accessToken: 'test-token' });
      if (url.includes('/load-test-runs/')) return response(200, { cleaned: true });
      if (method === 'post' && url.endsWith('/import-uploads')) {
        return behavior.create || response(201, { uploadId: 'test-upload', partSizeBytes: 5 * 1024 * 1024 });
      }
      if (method === 'post' && url.includes('/parts/')) {
        return behavior.part || response(200, { sizeBytes: body.length, sha256: sha256(body) });
      }
      if (method === 'post' && url.endsWith('/complete')) {
        return behavior.complete || response(202, { importRun: { id: 'test-import' } });
      }
      if (method === 'get') return behavior.progress || response(200, { status: 'UPLOADING' });
      if (method === 'del') return response(200, { status: 'ABORTED' });
      throw new Error(`Unexpected request: ${method} ${url}`);
    };
  }
  const context = createContext({
    __ENV: environment, __VU: 1, __ITER: 0,
    console: { log: value => logs.push(value) },
  });
  const modules = {
    'k6/http': new SyntheticModule(['default'], function () { this.setExport('default', http); }, { context }),
    'k6/crypto': new SyntheticModule(['default'], function () { this.setExport('default', { sha256 }); }, { context }),
    k6: new SyntheticModule(['check', 'sleep'], function () {
      this.setExport('check', (value, predicates) => {
        let passed = true;
        for (const [label, predicate] of Object.entries(predicates)) {
          const success = predicate(value);
          checks.push({ label, success });
          passed = success && passed;
        }
        return passed;
      });
      this.setExport('sleep', () => {});
    }, { context }),
  };
  const module = new SourceTextModule(source, { context });
  await module.link(specifier => modules[specifier]);
  await module.evaluate();
  return {
    requests, checks, logs, options: module.namespace.options,
    run: () => module.namespace.default({ token: 'test-token', runId: 'lt-upload-regression' }),
    setup: () => module.namespace.setup(),
    teardown: () => module.namespace.teardown({ token: 'test-token', runId: 'lt-upload-regression' }),
  };
}

test('large-upload executes exactly one real upload per VU, not idle iterations', async () => {
  const h = await harness();
  assert.equal(h.options.scenarios.geodata_workload.executor, 'per-vu-iterations');
  assert.equal(h.options.scenarios.geodata_workload.iterations, 1);
});

test('session, binary parts and completion follow the canonical contract', async () => {
  const h = await harness({ MYOTA_LOAD_TEST_FEATURES: '4500', MYOTA_LOAD_TEST_PADDING_BYTES: '1500' });
  h.run();
  const created = h.requests[0];
  const metadata = JSON.parse(created.body);
  const parts = h.requests.filter(r => r.url.includes('/parts/'));
  assert.equal(parts.length, 2);
  assert.equal(created.params.headers['Idempotency-Key'], 'lt-upload-regression-upload-1');
  assert.equal(metadata.source.loadTestRunId, 'lt-upload-regression');
  assert.equal(metadata.sha256, sha256(parts.map(r => r.body).join('')));
  assert.equal(metadata.expectedSize, parts.reduce((total, r) => total + Buffer.byteLength(r.body), 0));
  for (const [index, part] of parts.entries()) {
    assert.ok(part.url.endsWith(`/parts/${index + 1}`));
    assert.equal(part.params.headers['Content-Type'], 'application/octet-stream');
    assert.equal(part.params.headers['X-Part-SHA256'], sha256(part.body));
    assert.ok(part.body.length <= 5 * 1024 * 1024);
  }
  assert.ok(h.requests.at(-1).url.endsWith('/complete'));
  assert.equal(h.requests.at(-1).body, '{}');
  assert.ok(h.checks.every(c => c.success));
  assert.ok(h.requests.every(r => !r.url.includes('/imports/upload')));
});

test('a small final part is valid and completion reads nested importRun.id', async () => {
  const h = await harness(); h.run();
  assert.equal(h.requests.filter(r => r.url.includes('/parts/')).length, 1);
  assert.ok(h.checks.find(c => c.label === 'large file upload accepted').success);
  assert.equal(h.requests.filter(r => r.method === 'del').length, 0);
});

test('client parts remain bounded at 16 MiB even if the server advertises more', async () => {
  const h = await harness({ MYOTA_LOAD_TEST_FEATURES: '5000', MYOTA_LOAD_TEST_PADDING_BYTES: '4096' }, {
    create: response(201, { uploadId: 'test-upload', partSizeBytes: 64 * 1024 * 1024 }),
  });
  h.run();
  const parts = h.requests.filter(r => r.url.includes('/parts/'));
  assert.equal(parts.length, 2);
  assert.ok(parts.every(r => r.body.length <= 16 * 1024 * 1024));
});

test('session failure reports a redacted problem and does not transfer bytes', async () => {
  const h = await harness({}, { create: response(400, { detail: 'not-a-real-password test-admin@example.test rejected', requestId: 'request-1' }) });
  h.run();
  assert.equal(h.requests.length, 1);
  assert.equal(h.checks[0].success, false);
  assert.ok(h.logs[0].includes('HTTP 400'));
  assert.ok(h.logs[0].includes('request-1'));
  assert.ok(!h.logs[0].includes('not-a-real-password'));
  assert.ok(!h.logs[0].includes('test-admin@example.test'));
});

test('invalid server part size aborts the session without writing parts', async () => {
  const h = await harness({}, { create: response(201, { uploadId: 'test-upload', partSizeBytes: 1 }) });
  h.run();
  assert.equal(h.requests.filter(r => r.url.includes('/parts/')).length, 0);
  assert.equal(h.requests.at(-1).method, 'del');
  assert.ok(h.checks.some(c => !c.success));
});

test('a failed part is reported, aborts its session and never completes it', async () => {
  const h = await harness({}, { part: response(503, { detail: 'storage unavailable', correlationId: 'correlation-1' }) });
  h.run();
  assert.ok(h.logs.some(line => line.includes('HTTP 503') && line.includes('correlation-1')));
  assert.equal(h.requests.at(-1).method, 'del');
  assert.equal(h.requests.filter(r => r.url.endsWith('/complete')).length, 0);
});

test('a 200 part response with a wrong checksum still fails and aborts', async () => {
  const h = await harness({}, { part: response(200, { sizeBytes: 1, sha256: 'incorrect' }) });
  h.run();
  assert.ok(h.checks.some(c => c.label === 'upload part stored with checksum' && !c.success));
  assert.equal(h.requests.at(-1).method, 'del');
});

test('completion rejection reconciles state then aborts an incomplete session', async () => {
  const h = await harness({}, { complete: response(400, { detail: 'object verification failed' }) });
  h.run();
  assert.deepEqual(h.requests.slice(-2).map(r => r.method), ['get', 'del']);
  assert.ok(h.logs.some(line => line.includes('object verification failed')));
  assert.ok(h.checks.some(c => c.label === 'large file upload accepted' && !c.success));
});

test('lost completion response recovers a completed session without aborting it', async () => {
  const h = await harness({}, {
    complete: response(503, { detail: 'response lost' }),
    progress: response(200, { status: 'COMPLETED', importRunId: 'test-import' }),
  });
  h.run();
  assert.ok(h.checks.find(c => c.label === 'large file upload accepted').success);
  assert.equal(h.requests.at(-1).method, 'get');
  assert.equal(h.requests.filter(r => r.method === 'del').length, 0);
});

test('malformed completion JSON is reconciled instead of throwing before cleanup', async () => {
  const h = await harness({}, { complete: response(202, 'invalid-json') });
  h.run();
  assert.equal(h.requests.at(-1).method, 'del');
  assert.ok(h.checks.some(c => c.label === 'large file upload accepted' && !c.success));
});

test('production safeguards and report-only failure thresholds remain enabled', async () => {
  const h = await harness({
    MYOTA_ENV: 'production', MYOTA_BASE_URL: 'https://api.myota.top',
    MYOTA_LOAD_TEST_ALLOW_PRODUCTION: 'YES', MYOTA_LOAD_TEST_PRODUCTION_HOSTS: 'api.myota.top',
  });
  assert.equal(h.options.thresholds.http_req_failed[0], 'rate<0.05');
  assert.equal(h.options.tags.target, 'production');
  const data = h.setup();
  assert.ok(h.requests.some(r => r.method === 'del' && r.url.includes('/load-test-runs/')));
  assert.ok(data.runId.startsWith('lt-'));
});

test('production still rejects unacknowledged targets and excessive upload VUs', async () => {
  await assert.rejects(harness({ MYOTA_ENV: 'production', MYOTA_BASE_URL: 'https://api.myota.top' }), /acknowledgement/);
  await assert.rejects(harness({ MYOTA_LOAD_TEST_VUS: '9' }), /safe cap/);
});

test('teardown deletes only its exact tagged run with the required confirmation', async () => {
  const h = await harness(); h.teardown();
  assert.equal(h.requests.length, 1);
  assert.ok(h.requests[0].url.endsWith('/load-test-runs/lt-upload-regression'));
  assert.equal(JSON.parse(h.requests[0].body).confirmation, 'DELETE LOAD TEST DATA lt-upload-regression');
});
