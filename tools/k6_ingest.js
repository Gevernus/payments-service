// Intake load for the payments API: a constant arrival rate of POST /api/v1/payments with
// unique Idempotency-Keys, from a generator that needs far less CPU than the Python tool.
//   RATE=200 DURATION=45s RUN=200 k6 run --summary-export=out.json tools/k6_ingest.js
import http from 'k6/http';
import { check } from 'k6';
import { Counter } from 'k6/metrics';

// Metrics must be declared in the init context, not inside the VU function.
const accepted = new Counter('status_202');
const unavailable = new Counter('status_503');
const other = new Counter('status_other');

export const options = {
  scenarios: {
    intake: {
      executor: 'constant-arrival-rate',
      rate: Number(__ENV.RATE || 100),
      timeUnit: '1s',
      duration: __ENV.DURATION || '30s',
      preAllocatedVUs: 200,
      maxVUs: 3000,
    },
  },
  summaryTrendStats: ['med', 'p(95)', 'p(99)', 'max'],
};

const body = JSON.stringify({
  amount: '10.00',
  currency: 'RUB',
  description: 'k6',
  metadata: {},
  webhook_url: 'http://webhook-receiver:9000/hooks/ok',
});

export default function () {
  const headers = {
    'Content-Type': 'application/json',
    'X-API-Key': __ENV.API_KEY || 'dev-only-api-key-not-for-production-use-0000',
    'Idempotency-Key': `k6-${__ENV.RUN || 'x'}-${__VU}-${__ITER}`,
  };
  const res = http.post(`${__ENV.API || 'http://127.0.0.1:8000'}/api/v1/payments`, body, { headers });
  check(res, { accepted: (r) => r.status === 202 });
  if (res.status === 202) accepted.add(1);
  else if (res.status === 503) unavailable.add(1);
  else other.add(1);
}
