import http from 'k6/http';
import { check } from 'k6';

// ---- Config (override with: k6 run -e READ_VUS=200 -e DURATION_S=30 kv_loadtest.js)
const BASE = __ENV.BASE_URL || 'http://127.0.0.1:7878';
const STORE = __ENV.STORE || 'k6_bench';
const PRELOAD = Number(__ENV.PRELOAD || 2000); // keys written before the test starts
const D = Number(__ENV.DURATION_S || 20);      // seconds per phase
const GAP = 5;                                 // seconds between phases
const VALUE = 'v'.repeat(64);                  // 64-byte values

const WRITE_VUS = Number(__ENV.WRITE_VUS || 32);
const READ_VUS = Number(__ENV.READ_VUS || 100);
const MIXED_VUS = Number(__ENV.MIXED_VUS || 64);
const PAGE_VUS = Number(__ENV.PAGE_VUS || 16);

const phase = (exec, vus, index) => ({
  executor: 'constant-vus',
  exec,
  vus,
  duration: `${D}s`,
  startTime: `${index * (D + GAP)}s`,
});

export const options = {
  scenarios: {
    writes: phase('writeOnly', WRITE_VUS, 0),
    reads: phase('readOnly', READ_VUS, 1),
    mixed: phase('mixed', MIXED_VUS, 2), // 80% reads / 20% writes
    pages: phase('pagesOnly', PAGE_VUS, 3), // GET /pages/{store}/{offset}/100
  },
  thresholds: {
    checks: ['rate>0.99'],
    http_req_failed: ['rate<0.01'],
    // The always-true thresholds below only exist so k6 prints a per-phase
    // breakdown (latency + request count) in the end-of-test summary.
    'http_req_duration{scenario:writes}': ['max>=0'],
    'http_req_duration{scenario:reads}': ['max>=0'],
    'http_req_duration{scenario:mixed}': ['max>=0'],
    'http_req_duration{scenario:pages}': ['max>=0'],
    'http_reqs{scenario:writes}': ['count>=0'],
    'http_reqs{scenario:reads}': ['count>=0'],
    'http_reqs{scenario:mixed}': ['count>=0'],
    'http_reqs{scenario:pages}': ['count>=0'],
  },
  summaryTrendStats: ['avg', 'med', 'p(90)', 'p(95)', 'p(99)', 'max'],
};

// Every handler returns HTTP 200, even on failure, so the real result is
// the operation_status field in the JSON body.
function validOp(r) {
  if (r.status !== 200) return false;
  try {
    return r.json('operation_status') === true;
  } catch (_) {
    return false;
  }
}

export function setup() {
  const created = http.get(`${BASE}/create_kv/${STORE}`);
  check(created, { 'store created': (r) => r.status === 200 });

  // Preload so reads and pages hit real data. A plain object body is sent
  // as application/x-www-form-urlencoded, which is what /write expects.
  for (let i = 0; i < PRELOAD; i += 50) {
    const batch = [];
    for (let j = i; j < Math.min(i + 50, PRELOAD); j++) {
      batch.push([
        'POST',
        `${BASE}/write`,
        { key: `pre-${j}`, value: VALUE, kv_store_name: STORE },
      ]);
    }
    http.batch(batch);
  }
}

export function writeOnly() {
  const r = http.post(`${BASE}/write`, {
    key: `w-${__VU}-${__ITER}`,
    value: VALUE,
    kv_store_name: STORE,
  });
  check(r, { 'write ok': validOp });
}

export function readOnly() {
  const key = `pre-${Math.floor(Math.random() * PRELOAD)}`;
  const r = http.get(`${BASE}/${STORE}/read/${key}`);
  check(r, { 'read ok': validOp });
}

export function mixed() {
  if (Math.random() < 0.8) {
    readOnly();
  } else {
    writeOnly();
  }
}

export function pagesOnly() {
  const pages = Math.max(1, Math.floor(PRELOAD / 100));
  const offset = Math.floor(Math.random() * pages) * 100;
  const r = http.get(`${BASE}/pages/${STORE}/${offset}/100`);
  check(r, { 'page ok': validOp });
}