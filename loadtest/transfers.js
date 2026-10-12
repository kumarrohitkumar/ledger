// k6 run loadtest/transfers.js   (BASE_URL defaults to http://localhost:8080)
//
// 200 accounts, transfers in random directions at a constant arrival rate, and
// 10% of requests retried with the same Idempotency-Key as a client would after
// a timeout. At the end the ledger must reconcile cleanly.
import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter } from 'k6/metrics';

const BASE = __ENV.BASE_URL || 'http://localhost:8080';
const RATE = Number(__ENV.RATE || 500);
const json = { headers: { 'Content-Type': 'application/json' } };
const retries = new Counter('idempotent_retries');

export const options = {
  scenarios: {
    transfers: { executor: 'constant-arrival-rate', rate: RATE, timeUnit: '1s', duration: __ENV.DURATION || '60s', preAllocatedVUs: 100, maxVUs: 400 },
  },
  thresholds: {
    'http_req_failed{expected_response:true}': ['rate<0.001'],
    'http_req_duration{name:transfer}': ['p(95)<100'],
    checks: ['rate>0.999'],
  },
};

export function setup() {
  const bank = http.post(`${BASE}/v1/accounts`, JSON.stringify({ name: 'bank', currency: 'INR', allow_negative: true }), json).json();
  const accounts = [];
  for (let i = 0; i < 200; i++) {
    const a = http.post(`${BASE}/v1/accounts`, JSON.stringify({ name: `acct-${i}`, currency: 'INR' }), json).json();
    http.post(`${BASE}/v1/transfers`, JSON.stringify({ from: bank.id, to: a.id, amount: 1_000_000 }),
      { headers: { ...json.headers, 'Idempotency-Key': `fund-${a.id}` } });
    accounts.push(a.id);
  }
  return { accounts, run: Date.now() };
}

export default function (data) {
  const n = data.accounts.length;
  const from = data.accounts[Math.floor(Math.random() * n)];
  let to = data.accounts[Math.floor(Math.random() * n)];
  if (to === from) to = data.accounts[(data.accounts.indexOf(from) + 1) % n];
  const key = `${data.run}-${__VU}-${__ITER}`;
  const body = JSON.stringify({ from, to, amount: 1 + Math.floor(Math.random() * 5000) });
  const params = { headers: { ...json.headers, 'Idempotency-Key': key }, tags: { name: 'transfer' },
                   responseCallback: http.expectedStatuses(200, 201, 422) };

  const res = http.post(`${BASE}/v1/transfers`, body, params);
  check(res, { 'transfer accepted or refused cleanly': (r) => [201, 422].includes(r.status) });

  if (Math.random() < 0.1) {
    const again = http.post(`${BASE}/v1/transfers`, body, params);
    retries.add(1);
    check(again, {
      'retry is a replay': (r) => r.status === 422 || (r.status === 200 && r.headers['Idempotent-Replayed'] === 'true'),
      'retry returns the same transfer': (r) => r.status === 422 || r.json('id') === res.json('id'),
    });
  }
}

export function teardown() {
  // Let the relay and consumer catch up, then check the invariants.
  for (let i = 0; i < 30; i++) {
    const r = http.get(`${BASE}/v1/reconcile`).json();
    if (r.ok && r.report.unpublished_events === 0 && r.report.unprocessed_events === 0) {
      console.log(`reconcile OK: ${JSON.stringify(r.report)}`);
      return;
    }
    sleep(1);
  }
  const final = http.get(`${BASE}/v1/reconcile`).json();
  console.log(`reconcile after load: ${JSON.stringify(final)}`);
  check(final, { 'ledger reconciles after load': (r) => r.ok === true });
}
