import http from 'k6/http';
import crypto from 'k6/crypto';
import { check, sleep } from 'k6';

const BASE_URL = (__ENV.MYOTA_BASE_URL || 'https://api.myota.top').replace(/\/$/, '');
const PROFILE = __ENV.MYOTA_LOAD_TEST_PROFILE || 'preprocessing';
const ENVIRONMENT = (__ENV.MYOTA_ENV || '').toLowerCase();
const ALLOW_PRODUCTION = __ENV.MYOTA_LOAD_TEST_ALLOW_PRODUCTION === 'YES';
const MAX_VUS = 50;
const VUS = Number(__ENV.MYOTA_LOAD_TEST_VUS || (PROFILE === 'large-upload' ? 3 : 8));
const DURATION = __ENV.MYOTA_LOAD_TEST_DURATION || '2m';
const PROFILE_FEATURE_CAP = PROFILE === 'large-upload' ? 5000 :
  PROFILE === 'simultaneous-edits' ? 1 : PROFILE === 'queue-backlog' ? 50 : PROFILE === 'promotion' ? 40 : 100;
const FEATURES = Number(__ENV.MYOTA_LOAD_TEST_FEATURES ||
  (PROFILE === 'large-upload' ? 2500 : PROFILE_FEATURE_CAP));
const PADDING_BYTES = Number(__ENV.MYOTA_LOAD_TEST_PADDING_BYTES || 1024);
const IMPORTS_PER_VU_CAP = PROFILE === 'queue-backlog' ? 30 : PROFILE === 'promotion' ? 2 : 5;
const IMPORTS_PER_VU = Number(__ENV.MYOTA_LOAD_TEST_IMPORTS_PER_VU || IMPORTS_PER_VU_CAP);
const VALID_PROFILES = new Set(['large-upload', 'simultaneous-edits', 'preprocessing', 'promotion', 'queue-backlog', 'cleanup-only']);
const hostname = BASE_URL.replace(/^https?:\/\//, '').split('/')[0].split(':')[0].toLowerCase();
const LOCAL_HOSTS = new Set(['localhost', '127.0.0.1', 'gateway', 'myota-gateway', 'host.docker.internal']);
const ALLOWED_NONPROD_HOSTS = new Set((__ENV.MYOTA_LOAD_TEST_ALLOWED_HOSTS || '').split(',').map((host) => host.trim().toLowerCase()).filter(Boolean));
const ALLOWED_PRODUCTION_HOSTS = new Set((__ENV.MYOTA_LOAD_TEST_PRODUCTION_HOSTS || '').split(',').map((host) => host.trim().toLowerCase()).filter(Boolean));
const PRODUCTION_HOST = hostname === 'myota.top' || hostname === 'api.myota.top' || hostname === 'admin.myota.top' ||
  hostname === 'spainip.es' || hostname.endsWith('.spainip.es') || hostname.includes('production') ||
  hostname.startsWith('prod-') || hostname.startsWith('prod.');
const SAFE_HOST = !PRODUCTION_HOST && (LOCAL_HOSTS.has(hostname) || hostname.endsWith('.test') ||
  hostname.endsWith('.local') || ALLOWED_NONPROD_HOSTS.has(hostname));
const PRODUCTION_TARGET = ENVIRONMENT === 'production' && PRODUCTION_HOST &&
  ALLOW_PRODUCTION && ALLOWED_PRODUCTION_HOSTS.has(hostname);

function durationSeconds(value) {
  const amount = Number(value.slice(0, -1));
  return value.endsWith('m') ? amount * 60 : amount;
}

if (!VALID_PROFILES.has(PROFILE)) throw new Error(`Unknown workload profile: ${PROFILE}`);
if (!(SAFE_HOST && ['development', 'test', 'staging'].includes(ENVIRONMENT) && __ENV.MYOTA_LOAD_TEST_ALLOW_NONPROD === 'YES') && !PRODUCTION_TARGET) {
  throw new Error('Set the matching explicit non-production or production acknowledgement and exact host allowlist before running write workloads.');
}
if (!Number.isInteger(VUS) || VUS < 1 || VUS > MAX_VUS) {
  throw new Error(`MYOTA_LOAD_TEST_VUS must be between 1 and ${MAX_VUS}`);
}
if (!/^[1-9][0-9]*(s|m)$/.test(DURATION) || durationSeconds(DURATION) > 600) {
  throw new Error('MYOTA_LOAD_TEST_DURATION must be 1s-10m');
}
if (!Number.isInteger(FEATURES) || FEATURES < 1 || FEATURES > PROFILE_FEATURE_CAP) {
  throw new Error(`MYOTA_LOAD_TEST_FEATURES exceeds the safe cap for ${PROFILE}`);
}
if (PROFILE === 'promotion' && FEATURES % 40 !== 0) {
  throw new Error('The promotion profile requires a feature count divisible by 40 for an exact 2.5%/2.5% split.');
}
if (!Number.isInteger(IMPORTS_PER_VU) || IMPORTS_PER_VU < 1 || IMPORTS_PER_VU > IMPORTS_PER_VU_CAP) {
  throw new Error(`MYOTA_LOAD_TEST_IMPORTS_PER_VU must be between 1 and ${IMPORTS_PER_VU_CAP} for ${PROFILE}`);
}
if (!Number.isInteger(PADDING_BYTES) || PADDING_BYTES < 0 || PADDING_BYTES > 4096) {
  throw new Error('MYOTA_LOAD_TEST_PADDING_BYTES must be between 0 and 4096 bytes per feature');
}
if (PROFILE === 'queue-backlog' && Number(__ENV.MYOTA_LOAD_TEST_ITERATIONS_PER_SECOND || 1) > 1) {
  throw new Error('queue-backlog is capped at one import request per second');
}

const thresholds = PROFILE === 'cleanup-only' ? {
  http_req_failed: ['rate<0.05'],
} : {
  // Production thresholds are report-only until teardown has had a chance to
  // finish the full workload and clean its fixtures. Aborting mid-run leaves
  // accepted imports active, which the cleanup endpoint correctly refuses to
  // delete. Non-production may stop early to shorten feedback cycles.
  http_req_failed: [PRODUCTION_TARGET ? 'rate<0.05' : {
    threshold: 'rate<0.05', abortOnFail: true, delayAbortEval: '30s',
  }],
  checks: ['rate>0.90'],
};
if (PRODUCTION_TARGET && PROFILE !== 'cleanup-only') {
  // Keep API/control-plane latency distinct from bulk transfer and fixture
  // cleanup. Upload-part time scales with bytes; the load-test file itself
  // uses 16 MiB maximum parts, so allow up to 60 seconds per part while still
  // surfacing its own p95 in the k6 summary.
  thresholds['http_req_duration{request_class:control}'] = ['p(95)<2000'];
  if (PROFILE === 'large-upload') {
    thresholds['http_req_duration{request_class:bulk_transfer}'] = [
      'p(95)<60000',
    ];
  }
}
const cleanupExpectedStatuses = http.expectedStatuses(200, 400, 401);
// Import POST returns 202 before the durable run projection is visible. A
// short-lived 404 while polling that accepted run is eventual-consistency
// signaling, not an API failure; later 4xx/5xx responses remain failures.
const preprocessingPollExpectedStatuses = http.expectedStatuses(200, 404);

export const options = {
  setupTimeout: PROFILE === 'cleanup-only' ? '6m' : '60s',
  teardownTimeout: '6m',
  scenarios: {
    geodata_workload: PROFILE === 'cleanup-only' ? {
      executor: 'shared-iterations',
      vus: 1,
      iterations: 1,
      maxDuration: '30s',
    } : PROFILE === 'large-upload' ? {
      executor: 'per-vu-iterations',
      vus: VUS,
      iterations: 1,
      maxDuration: DURATION,
      gracefulStop: '30s',
    } : PROFILE === 'queue-backlog' ? {
      executor: 'constant-arrival-rate',
      rate: Number(__ENV.MYOTA_LOAD_TEST_ITERATIONS_PER_SECOND || 1),
      timeUnit: '1s',
      duration: DURATION,
      preAllocatedVUs: VUS,
      maxVUs: MAX_VUS,
      gracefulStop: '30s',
    } : {
      executor: 'constant-vus',
      vus: VUS,
      duration: DURATION,
      gracefulStop: '30s',
    },
  },
  thresholds,
  tags: { test_suite: 'geodata_workloads', workload_profile: PROFILE, target: PRODUCTION_TARGET ? 'production' : 'nonproduction' },
};

function auth(token, contentType = 'application/json', operation = 'api') {
  const requestClass = operation === 'upload-part' ? 'bulk_transfer' :
    operation === 'fixture-cleanup' ? 'cleanup' : 'control';
  const headers = {
    Accept: 'application/json', Authorization: `Bearer ${token}`,
    'User-Agent': `MyOTA-Geodata-LoadTest/1.0 (+https://myota.org; ${PRODUCTION_TARGET ? 'production' : 'non-production'}; run=${__ENV.MYOTA_LOAD_TEST_RUN_ID || 'generated'})`,
  };
  headers['Content-Type'] = contentType;
  return {
    headers,
    tags: {
      service: 'geodata',
      workload_profile: PROFILE,
      operation,
      request_class: requestClass,
    },
  };
}

function feature(index, runId, suffix = '', paddingBytes = 0) {
  const longitude = -5.99 + ((index % 80) * 0.0006);
  const latitude = 37.38 + ((Math.floor(index / 80) % 80) * 0.0006);
  return {
    type: 'Feature',
    id: `${runId}-${index}${suffix}`,
    properties: {
      name: `Load test ${runId} feature ${index}${suffix}`,
      sourceRef: `${runId}:${index}${suffix}`,
      ...(paddingBytes ? { testPayloadPadding: 'x'.repeat(paddingBytes) } : {}),
    },
    geometry: { type: 'Polygon', coordinates: [[
      [longitude, latitude], [longitude + 0.00025, latitude],
      [longitude + 0.00025, latitude + 0.00025], [longitude, latitude + 0.00025],
      [longitude, latitude],
    ]] },
  };
}

function dataset(count, runId, suffix = '', paddingBytes = 0) {
  return JSON.stringify({ type: 'FeatureCollection', features: Array.from({ length: count }, (_, i) => feature(i, runId, suffix, paddingBytes)) });
}

function responseField(response, selector) {
  try { return response.json(selector); } catch (_) { return null; }
}

function uploadCheck(response, label, predicate) {
  const accepted = predicate(response);
  check(response, { [label]: () => accepted });
  if (!accepted) {
    const details = loginErrorDetails(response, __ENV.MYOTA_LOAD_TEST_EMAIL, __ENV.MYOTA_LOAD_TEST_PASSWORD);
    console.log(`${label} failed (HTTP ${response.status}); API response: ${details}`);
  }
  return accepted;
}

function uploadDataset(token, runId) {
  // Generated fixtures contain only ASCII, so string offsets and byte offsets
  // coincide. Do not use this slicing strategy for arbitrary uploaded files.
  const source = dataset(FEATURES, runId, `:${__VU}`, PADDING_BYTES);
  const metadata = {
    adapter: 'MANUAL', format: 'GEOJSON', filename: `loadtest-${runId}-${__VU}.geojson`,
    entityTypes: ['MUNICIPAL_PARK'], expectedSize: source.length,
    sha256: crypto.sha256(source, 'hex'),
    source: { name: `MyOTA load test ${runId}`, license: 'CC0', attribution: 'Synthetic fixture', loadTestRunId: runId },
  };
  const params = auth(token, 'application/json', 'upload-session-create');
  params.headers['Idempotency-Key'] = `${runId}-upload-${__VU}`;
  const created = http.post(`${BASE_URL}/v1/geodata/import-uploads`, JSON.stringify(metadata), params);
  const uploadId = responseField(created, 'uploadId');
  if (!uploadCheck(created, 'resumable upload session created', (r) => r.status === 201 && typeof uploadId === 'string' && uploadId.length > 0)) return;
  console.log(`Resumable upload ${uploadId}; run=${runId}; bytes=${source.length}`);

  const endpoint = `${BASE_URL}/v1/geodata/import-uploads/${encodeURIComponent(uploadId)}`;
  let completed = false;
  try {
    const advertisedPartSize = Number(responseField(created, 'partSizeBytes'));
    if (!Number.isSafeInteger(advertisedPartSize) || advertisedPartSize < 5 * 1024 * 1024) {
      check(false, { 'upload part size is valid': () => false });
      console.log('Upload session returned an invalid partSizeBytes; aborting its multipart upload.');
      return;
    }
    const partSize = Math.min(advertisedPartSize, 16 * 1024 * 1024);
    for (let offset = 0, partNumber = 1; offset < source.length; offset += partSize, partNumber += 1) {
      const part = source.slice(offset, offset + partSize);
      const partParams = auth(token, 'application/octet-stream', 'upload-part');
      partParams.headers['X-Part-SHA256'] = crypto.sha256(part, 'hex');
      const response = http.post(`${endpoint}/parts/${partNumber}`, part, partParams);
      if (!uploadCheck(response, 'upload part stored with checksum', (r) => r.status === 200
        && Number(responseField(r, 'sizeBytes')) === part.length
        && responseField(r, 'sha256') === partParams.headers['X-Part-SHA256'])) return;
    }

    const response = http.post(`${endpoint}/complete`, '{}', auth(token, 'application/json', 'upload-complete'));
    let importId = responseField(response, 'importRun.id');
    completed = response.status === 202 && Boolean(importId);
    // Completion can succeed despite a lost response. Reconcile the durable
    // session before attempting to abort an already accepted source object.
    if (!completed) {
      const progress = http.get(endpoint, auth(token, 'application/json', 'upload-session-status'));
      importId = responseField(progress, 'importRunId');
      completed = progress.status === 200 && responseField(progress, 'status') === 'COMPLETED' && Boolean(importId);
    }
    uploadCheck(response, 'large file upload accepted', () => completed);
  } finally {
    if (!completed) {
      const aborted = http.del(endpoint, null, auth(token, 'application/json', 'upload-session-abort'));
      uploadCheck(aborted, 'incomplete upload aborted', (r) => r.status === 200 && responseField(r, 'status') === 'ABORTED');
    }
  }
}

function requestImport(token, runId, count, requestSequence) {
  const suffix = `:${requestSequence}`;
  const body = {
    adapter: 'MANUAL', format: 'GEOJSON', entityType: 'MUNICIPAL_PARK', entityTypes: ['MUNICIPAL_PARK'],
    source: { name: `MyOTA load test ${runId}`, license: 'CC0', attribution: 'Synthetic load-test fixture', loadTestRunId: runId },
    features: Array.from({ length: count }, (_, i) => feature(i, runId, `${suffix}:${i}`)),
  };
  const response = http.post(`${BASE_URL}/v1/geodata/imports`, JSON.stringify(body), auth(token, 'application/json', 'import-submit'));
  let importId = null;
  if (response.status === 202) {
    try { importId = response.json('id'); } catch (_) { /* Log the malformed response below. */ }
  }
  const accepted = response.status === 202 && Boolean(importId);
  if (!accepted) {
    const details = loginErrorDetails(response, __ENV.MYOTA_LOAD_TEST_EMAIL, __ENV.MYOTA_LOAD_TEST_PASSWORD);
    console.log(`Import submission ${requestSequence} for run ${runId} was not accepted (HTTP ${response.status}); API response: ${details}`);
  }
  check(response, { 'import was accepted for preprocessing': () => accepted });
  return accepted ? importId : null;
}

function waitForPreprocessing(token, runId, importId, timeoutSeconds = 120) {
  const params = auth(token, 'application/json', 'import-status-poll');
  params.responseCallback = preprocessingPollExpectedStatuses;
  const deadline = Date.now() + timeoutSeconds * 1000;
  let lastStatus = 0;
  let lastDetails = 'no response body';
  while (Date.now() < deadline) {
    const response = http.get(`${BASE_URL}/v1/geodata/imports/${importId}`, params);
    lastStatus = response.status;
    if (response.status === 200) {
      const status = String(response.json('status') || '').toUpperCase();
      if (status.startsWith('PREPROCESSED') || status === 'FAILED') return status;
    } else {
      lastDetails = loginErrorDetails(response, __ENV.MYOTA_LOAD_TEST_EMAIL, __ENV.MYOTA_LOAD_TEST_PASSWORD);
    }
    sleep(2);
  }
  throw new Error(`Timed out waiting for preprocessing for load-test run ${runId}; last status response was HTTP ${lastStatus}: ${lastDetails}`);
}

function loginErrorDetails(response, email, password) {
  const safeFields = ['type', 'title', 'status', 'code', 'detail', 'requestId', 'correlationId'];
  try {
    const payload = response.json();
    if (payload && typeof payload === 'object' && !Array.isArray(payload)) {
      const safePayload = {};
      for (const key of safeFields) {
        const value = payload[key];
        if (typeof value === 'string' || typeof value === 'number') {
          let safeValue = String(value);
          if (email) safeValue = safeValue.split(email).join('[redacted email]');
          if (password) safeValue = safeValue.split(password).join('[redacted password]');
          safePayload[key] = safeValue;
        }
      }
      if (Object.keys(safePayload).length) return JSON.stringify(safePayload);
    }
  } catch (_) {
    // Fall back to a bounded, credential-redacted text snippet for non-JSON errors.
  }

  let body = String(response.body || '').trim();
  if (email) body = body.split(email).join('[redacted email]');
  if (password) body = body.split(password).join('[redacted password]');
  body = body
    .replace(/Bearer\s+[^\s"']+/gi, 'Bearer [redacted]')
    .replace(/("(?:password|accessToken|refreshToken|token|secret|authorization)"\s*:\s*")[^"]*(")/gi, '$1[redacted]$2');
  if (!body) return 'response body was empty';
  return body.length > 1200 ? `${body.slice(0, 1200)}… [truncated]` : body;
}

function login() {
  const email = __ENV.MYOTA_LOAD_TEST_EMAIL;
  const password = __ENV.MYOTA_LOAD_TEST_PASSWORD;
  if (!email || !password) throw new Error('Set MYOTA_LOAD_TEST_EMAIL and MYOTA_LOAD_TEST_PASSWORD for the dedicated target-environment admin.');
  const response = http.post(`${BASE_URL}/v1/identity/auth/login`, JSON.stringify({ email, password }), {
    headers: { Accept: 'application/json', 'Content-Type': 'application/json', 'User-Agent': 'MyOTA-Geodata-LoadTest/1.0' },
    tags: { service: 'identity', workload_profile: PROFILE, operation: 'login', request_class: 'control' },
  });
  if (response.status !== 200 && response.status !== 201) {
    const details = loginErrorDetails(response, email, password);
    throw new Error(`Load-test admin login failed (${response.status}); API response: ${details}`);
  }
  return response.json('accessToken');
}

function cleanupRun(token, runId) {
  let currentToken = token;
  const url = `${BASE_URL}/v1/geodata/load-test-runs/${encodeURIComponent(runId)}`;
  const body = JSON.stringify({ confirmation: `DELETE LOAD TEST DATA ${runId}` });
  let lastStatus = 0;
  let lastDetails = 'no response body';
  for (let attempt = 0; attempt < 30; attempt += 1) {
    const params = auth(currentToken, 'application/json', 'fixture-cleanup');
    params.responseCallback = cleanupExpectedStatuses;
    const response = http.del(url, body, params);
    lastStatus = response.status;
    if (response.status === 200) {
      console.log(`Removed load-test fixture run ${runId}: ${JSON.stringify(response.json())}`);
      return;
    }
    lastDetails = loginErrorDetails(response, __ENV.MYOTA_LOAD_TEST_EMAIL, __ENV.MYOTA_LOAD_TEST_PASSWORD);
    if (response.status === 401) {
      currentToken = login();
      continue;
    }
    let pending = false;
    if (response.status === 400) {
      try {
        const detail = String(response.json('detail') || '');
        pending = detail.includes('load-test runs must finish or fail') ||
          detail.includes('promotion queues must finish') ||
          detail.includes('load-test uploads must finish or be aborted');
      } catch (_) {
        pending = false;
      }
    }
    if (pending) {
      console.log(`Cleanup for run ${runId} is waiting for active processing (attempt ${attempt + 1}/30): ${lastDetails}`);
    }
    if (!pending) break;
    sleep(10);
  }
  throw new Error(`Automatic cleanup failed for ${runId} (HTTP ${lastStatus}); API response: ${lastDetails}`);
}

export function setup() {
  const token = login();
  const runId = __ENV.MYOTA_LOAD_TEST_RUN_ID || `lt-${Date.now()}-${Math.floor(Math.random() * 1e9)}`;
  if (!/^lt-[A-Za-z0-9._-]{1,77}$/.test(runId)) throw new Error('MYOTA_LOAD_TEST_RUN_ID must start with lt- and use at most 80 letters, digits, dot, underscore, or hyphen.');
  console.log(`Workload ${PROFILE}; target=${PRODUCTION_TARGET ? 'production' : 'nonproduction'}; run=${runId}; uploads=resumable-v1`);
  // Fail before any production writes unless the dedicated cleanup endpoint,
  // its production acknowledgement, and the caller's global-administrator role work.
  if (PRODUCTION_TARGET || PROFILE === 'cleanup-only') cleanupRun(token, runId);
  if (PROFILE === 'cleanup-only') return { token, runId, cleanupOnly: true };
  try {
    let seedEntityId = null;
    if (PROFILE === 'simultaneous-edits') {
      const importId = requestImport(token, runId, 1, 'setup-seed');
      if (!importId || waitForPreprocessing(token, runId, importId) === 'FAILED') throw new Error('Could not create the shared edit fixture.');
      const page = http.get(`${BASE_URL}/v1/geodata/imports/${importId}/candidates?page=1&pageSize=10`, auth(token));
      const pageItems = page.json('items') || [];
      const candidateId = pageItems.length ? pageItems[0].id : null;
      if (!candidateId) throw new Error('Edit fixture candidate was not staged.');
      http.post(`${BASE_URL}/v1/geodata/imports/${importId}/candidates/validate`, JSON.stringify({
        candidateIds: [candidateId], reviewerId: 'load-test', validationStatus: 'VALID', note: 'Synthetic load-test fixture',
      }), auth(token));
      http.post(`${BASE_URL}/v1/geodata/imports/${importId}/process`, JSON.stringify({
        candidateIds: [candidateId], targetStatus: 'CANDIDATE', processorId: 'load-test', note: 'Synthetic load-test fixture',
      }), auth(token));
      const deadline = Date.now() + 120000;
      while (Date.now() < deadline) {
        const detail = http.get(`${BASE_URL}/v1/geodata/imports/${importId}`, auth(token));
        const stats = detail.json('stats') || {};
        if (Number(stats.created || 0) + Number(stats.updated || 0) > 0) break;
        sleep(2);
      }
      const listed = http.get(`${BASE_URL}/v1/geodata/entities?page=1&pageSize=100`, auth(token));
      const fixture = (listed.json('items') || []).find((entity) => entity.name && entity.name.includes(runId));
      seedEntityId = fixture && fixture.id;
      if (!seedEntityId) throw new Error('Promotion did not produce the shared edit fixture.');
    }
    return { token, runId, seedEntityId };
  } catch (error) {
    cleanupRun(token, runId);
    throw error;
  }
}

export default function (data) {
  if (data.cleanupOnly) return;

  if (PROFILE === 'large-upload') {
    uploadDataset(data.token, data.runId);
    return;
  }

  if (PROFILE === 'simultaneous-edits') {
    const response = http.patch(`${BASE_URL}/v1/geodata/entities/${encodeURIComponent(data.seedEntityId)}`, JSON.stringify({
      editorId: 'load-test', name: `Load test ${data.runId} edit ${__VU}-${__ITER}`,
      note: 'Concurrent edit workload against a test-owned fixture',
    }), auth(data.token));
    check(response, { 'concurrent entity edit succeeded': (r) => r.status === 200 });
    sleep(1);
    return;
  }

  if (__ITER >= IMPORTS_PER_VU) { sleep(1); return; }

  const importId = requestImport(data.token, data.runId, FEATURES, `${__VU}:${__ITER}`);
  if (!importId) return;
  if (PROFILE === 'preprocessing' || PROFILE === 'queue-backlog') { sleep(1); return; }
  const status = waitForPreprocessing(data.token, data.runId, importId);
  if (status === 'FAILED') {
    check(false, { 'import preprocessing completed': () => false });
    return;
  }
  const candidatesResponse = http.get(`${BASE_URL}/v1/geodata/imports/${importId}/candidates?page=1&pageSize=100`, auth(data.token));
  const candidateIds = (candidatesResponse.json('items') || []).map((candidate) => candidate.id);
  check(candidatesResponse, { 'preprocessed candidates are available': () => candidateIds.length > 0 });
  if (!candidateIds.length) return;
  if (PROFILE === 'promotion' && candidateIds.length !== FEATURES) {
    throw new Error(`Promotion import staged ${candidateIds.length} records; expected ${FEATURES} for the exact 5% split.`);
  }
  const candidatePromotionIds = PROFILE === 'promotion' ? candidateIds.slice(0, FEATURES / 40) : candidateIds;
  const approvedPromotionIds = PROFILE === 'promotion'
    ? candidateIds.slice(FEATURES / 40, FEATURES / 20)
    : [];
  const rejectedIds = PROFILE === 'promotion'
    ? candidateIds.slice(FEATURES / 20)
    : [];
  const validIds = [...candidatePromotionIds, ...approvedPromotionIds];
  const validation = http.post(`${BASE_URL}/v1/geodata/imports/${importId}/candidates/validate`, JSON.stringify({
    candidateIds: validIds, reviewerId: 'load-test', validationStatus: 'VALID', note: 'Synthetic load-test promotion profile',
  }), auth(data.token));
  check(validation, { 'candidate validation succeeded': (r) => r.status === 200 });
  if (PROFILE === 'promotion') {
    const rejection = http.post(`${BASE_URL}/v1/geodata/imports/${importId}/candidates/validate`, JSON.stringify({
      candidateIds: rejectedIds, reviewerId: 'load-test', validationStatus: 'REJECTED', note: 'Excluded from the bounded 5% promotion sample.',
    }), auth(data.token));
    check(rejection, { 'unselected records were rejected': (r) => r.status === 200 });
    for (const [selectedIds, targetStatus] of [
      [candidatePromotionIds, 'CANDIDATE'],
      [approvedPromotionIds, 'APPROVED'],
    ]) {
      const promotion = http.post(`${BASE_URL}/v1/geodata/imports/${importId}/process`, JSON.stringify({
        candidateIds: selectedIds, targetStatus, processorId: 'load-test', note: 'Synthetic load-test promotion profile',
      }), auth(data.token));
      check(promotion, { [`${targetStatus.toLowerCase()} promotion queued`]: (r) => r.status === 202 || r.status === 200 });
    }
  } else {
    const promotion = http.post(`${BASE_URL}/v1/geodata/imports/${importId}/process`, JSON.stringify({
      candidateIds, targetStatus: 'CANDIDATE', processorId: 'load-test', note: 'Synthetic load-test promotion profile',
    }), auth(data.token));
    check(promotion, { 'promotion queue accepted': (r) => r.status === 202 || r.status === 200 });
  }
  sleep(1);
}

export function teardown(data) {
  if (data.cleanupOnly) return;
  cleanupRun(data.token, data.runId);
}
