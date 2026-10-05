import http from 'k6/http';
import { check, sleep } from 'k6';

const BASE_URL = (__ENV.MYOTA_BASE_URL || 'https://api.myota.top').replace(/\/$/, '');
const VUS = Number(__ENV.MYOTA_LOAD_TEST_VUS || 2);
const DURATION = __ENV.MYOTA_LOAD_TEST_DURATION || '60s';
const PRODUCTION_HOSTS = new Set(['api.myota.top']);
const hostname = BASE_URL.replace(/^https?:\/\//, '').split('/')[0].split(':')[0].toLowerCase();
const isProduction = PRODUCTION_HOSTS.has(hostname);

if (!Number.isInteger(VUS) || VUS < 1 || VUS > 4) {
  throw new Error('MYOTA_LOAD_TEST_VUS must be an integer from 1 to 4');
}
if (!/^[1-9][0-9]*(s|m)$/.test(DURATION) || durationSeconds(DURATION) > 300) {
  throw new Error('MYOTA_LOAD_TEST_DURATION must be 1s-300s or 1m-5m');
}
if (isProduction && __ENV.MYOTA_ALLOW_PRODUCTION !== 'YES') {
  throw new Error('Production target requires MYOTA_ALLOW_PRODUCTION=YES. This script is read-only and capped at 4 VUs / 5 minutes.');
}

function durationSeconds(value) {
  const amount = Number(value.slice(0, -1));
  return value.endsWith('m') ? amount * 60 : amount;
}

export const options = {
  scenarios: {
    geodata_read_baseline: {
      executor: 'constant-vus',
      vus: VUS,
      duration: DURATION,
      gracefulStop: '5s',
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.05'],
    checks: ['rate>0.95'],
  },
  tags: { test_suite: 'geodata_read_baseline', target: isProduction ? 'production' : 'nonproduction' },
};

const params = {
  headers: {
    Accept: 'application/json',
    'User-Agent': 'MyOTA-Geodata-Baseline/1.0 (+https://myota.org; read-only)',
  },
  tags: { service: 'geodata' },
};

function pointBounds(entity) {
  const geometry = entity && entity.geometry;
  if (!geometry || !geometry.coordinates) return null;
  const points = [];
  const visit = (value) => {
    if (!Array.isArray(value)) return;
    if (value.length >= 2 && Number.isFinite(value[0]) && Number.isFinite(value[1])) {
      points.push([value[0], value[1]]);
      return;
    }
    for (const child of value) visit(child);
  };
  visit(geometry.coordinates);
  if (!points.length) return null;
  const longitudes = points.map((point) => point[0]);
  const latitudes = points.map((point) => point[1]);
  const west = longitudes.reduce((a, b) => Math.min(a, b), 180);
  const east = longitudes.reduce((a, b) => Math.max(a, b), -180);
  const south = latitudes.reduce((a, b) => Math.min(a, b), 90);
  const north = latitudes.reduce((a, b) => Math.max(a, b), -90);
  const pad = 0.02;
  return [
    Math.max(-180, west - pad),
    Math.max(-90, south - pad),
    Math.min(180, east + pad),
    Math.min(90, north + pad),
  ];
}

export function setup() {
  const response = http.get(`${BASE_URL}/v1/geodata/entities?page=1&pageSize=10`, params);
  const valid = check(response, {
    'entity catalogue setup responds 200': (r) => r.status === 200,
    'entity catalogue setup returns JSON': (r) => Boolean(r.json('items')),
  });
  if (!valid) throw new Error(`Cannot build read-only test dataset: GET entity catalogue returned ${response.status}`);

  const entities = response.json('items') || [];
  const samples = entities
    .filter((entity) => entity && entity.id)
    .map((entity) => ({ id: entity.id, bounds: pointBounds(entity) }))
    .filter((entity) => entity.bounds);
  if (!samples.length) {
    throw new Error('Production has no catalogue entities to sample; refusing to run a misleading baseline.');
  }
  return {
    entityIds: samples.map((entity) => entity.id),
    bounds: samples.map((entity) => entity.bounds),
  };
}

export default function (dataset) {
  const health = http.get(`${BASE_URL}/healthz`, params);
  check(health, { 'gateway health is available': (r) => r.status === 200 });
  sleep(0.5);

  const page = 1 + (__ITER % 10);
  const catalogue = http.get(`${BASE_URL}/v1/geodata/entities?page=${page}&pageSize=25`, params);
  check(catalogue, { 'entity catalogue read succeeds': (r) => r.status === 200 });
  sleep(0.5);

  const index = (__VU + __ITER) % dataset.bounds.length;
  const [minLon, minLat, maxLon, maxLat] = dataset.bounds[index];
  const mapQuery = `bbox=${encodeURIComponent(`${minLon},${minLat},${maxLon},${maxLat}`)}&page=1&pageSize=50`;
  const map = http.get(`${BASE_URL}/v1/geodata/entities?${mapQuery}`, params);
  check(map, { 'bounded map catalogue read succeeds': (r) => r.status === 200 });
  sleep(0.5);

  const entityId = dataset.entityIds[(__VU + __ITER) % dataset.entityIds.length];
  const detail = http.get(`${BASE_URL}/v1/geodata/entities/${encodeURIComponent(entityId)}`, params);
  check(detail, { 'sample entity detail read succeeds': (r) => r.status === 200 });

  // Pace every request: at most about 2 requests/sec per VU, with 4 VUs max.
  sleep(0.5);
}

export function teardown(dataset) {
  // The setup dataset is held only in k6 memory and contains sampled public IDs.
  // No entities, imports, files, or other application records are created.
  console.log(`Read-only baseline complete; sampled ${dataset.entityIds.length} entities. No application data was created.`);
}
