import http from 'k6/http';
import { check, sleep } from 'k6';
import { Trend, Rate } from 'k6/metrics';
const endToEnd = new Trend('job_end_to_end_seconds');
const completed = new Rate('job_completed');
export const options = {
  vus: Number(__ENV.VUS || 16), iterations: Number(__ENV.JOBS || 1000),
  thresholds: { job_completed: ['rate>0.99'] },
};
const base = __ENV.API_URL || 'http://api:8080';
const headers = { 'Content-Type': 'application/json', Authorization: `Bearer ${__ENV.API_KEY || 'local-demo-api-key'}` };
export default function () {
  const start = Date.now();
  const created = http.post(`${base}/api/v1/jobs`, JSON.stringify({ type: 'sleep', payload: {durationMs: Number(__ENV.SLEEP_MS || 100)} }), { headers });
  if (!check(created, {created: r => r.status === 201})) { completed.add(false); return; }
  const id = created.json('id');
  for (let n = 0; n < 600; n++) {
    const res = http.get(`${base}/api/v1/jobs/${id}`, { headers });
    if (res.status === 200 && res.json('status') === 'SUCCEEDED') { completed.add(true); endToEnd.add((Date.now()-start)/1000); return; }
    sleep(0.1);
  }
  completed.add(false);
}
