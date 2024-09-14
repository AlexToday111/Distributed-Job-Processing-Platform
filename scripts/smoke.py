"""Проверка контрактов всего стека. Запускайте после docker compose up --build --wait."""
import json
import os
import time
import urllib.request
import uuid

BASE = os.getenv("API_URL", "http://localhost:8080")
TOKEN = os.getenv("API_KEY", "local-demo-api-key")

def request(method, path, body=None, headers=None):
    h = {"Authorization": "Bearer " + TOKEN, "Content-Type": "application/json"}
    h.update(headers or {})
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(BASE + path, data=data, headers=h, method=method)
    with urllib.request.urlopen(req, timeout=10) as res:
        return json.load(res)

def wait(job_id, states, timeout=40):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        job = request("GET", "/api/v1/jobs/" + job_id)
        if job["status"] in states:
            return job
        time.sleep(0.2)
    raise AssertionError(f"job {job_id} did not reach {states}: {job}")

def main():
    assert request("GET", "/health/ready")["status"] == "ready"
    key = str(uuid.uuid4())
    body = {"type": "sleep", "payload": {"durationMs": 10}}
    first = request("POST", "/api/v1/jobs", body, {"Idempotency-Key": key})
    duplicate = request("POST", "/api/v1/jobs", body, {"Idempotency-Key": key})
    assert first["id"] == duplicate["id"]
    assert wait(first["id"], {"SUCCEEDED"})["attempts"] == 1
    attempts = request("GET", f'/api/v1/jobs/{first["id"]}/attempts')
    assert len(attempts) == 1 and attempts[0]["status"] == "SUCCEEDED"
    delayed = request("POST", "/api/v1/jobs", {**body, "delaySeconds": 2})
    assert delayed["status"] == "SCHEDULED"
    wait(delayed["id"], {"SUCCEEDED"})
    failing = request("POST", "/api/v1/jobs", {"type": "unstable", "payload": {"failureProbability": 1}, "maxAttempts": 2})
    assert wait(failing["id"], {"DEAD_LETTERED"})["attempts"] == 2
    history = request("GET", f'/api/v1/jobs/{failing["id"]}/attempts')
    assert len(history) == 2 and all(a["status"] == "FAILED" for a in history)
    dlq = request("GET", "/api/v1/dead-letter")
    assert any(j["id"] == failing["id"] for j in dlq["jobs"])
    request("POST", f'/api/v1/jobs/{failing["id"]}/requeue')
    wait(failing["id"], {"DEAD_LETTERED"})
    assert len(request("GET", f'/api/v1/jobs/{failing["id"]}/attempts')) == 4
    running = request("POST", "/api/v1/jobs", {"type": "sleep", "payload": {"durationMs": 10000}})
    wait(running["id"], {"RUNNING"})
    request("POST", f'/api/v1/jobs/{running["id"]}/cancel')
    assert wait(running["id"], {"CANCELLED"})["status"] == "CANCELLED"
    time.sleep(3.5)
    assert request("GET", f'/api/v1/jobs/{running["id"]}')["status"] == "CANCELLED"
    print(json.dumps({"status": "PASS", "scenarios": ["idempotency", "execution", "attempts", "delay", "retry", "DLQ", "requeue", "running cancellation"], "trace_job": first["id"]}))

if __name__ == "__main__":
    main()
