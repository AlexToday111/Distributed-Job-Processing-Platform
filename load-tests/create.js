import http from 'k6/http';
import { check } from 'k6';
export const options = {
  vus: Number(__ENV.VUS || 50), duration: __ENV.DURATION || '30s',
  thresholds: { http_req_failed: ['rate<0.01'], http_req_duration: ['p(95)<1000'] },
};
export default function () {
  const response = http.post(`${__ENV.API_URL || 'http://api:8080'}/api/v1/jobs`, JSON.stringify({
    type: 'sleep', payload: { durationMs: Number(__ENV.SLEEP_MS || 0) },
    queue: __ENV.QUEUE || 'load', maxAttempts: 3,
  }), { headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${__ENV.API_KEY || 'local-demo-api-key'}` } });
  check(response, { 'created': (r) => r.status === 201 });
}
