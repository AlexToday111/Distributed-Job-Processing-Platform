"""Восстановить задание после SIGKILL исполнителя и затем запустить исполнителя снова."""
import json
import subprocess
from smoke import request, wait

job = request("POST", "/api/v1/jobs", {"type": "sleep", "payload": {"durationMs": 8000}, "maxAttempts": 3})
running = wait(job["id"], {"RUNNING"})
worker = running["workerId"]
container = subprocess.check_output(["docker", "compose", "ps", "-q", worker], text=True).strip()
try:
    subprocess.run(["docker", "update", "--restart=no", container], check=True)
    subprocess.run(["docker", "compose", "kill", "-s", "SIGKILL", worker], check=True)
    result = wait(job["id"], {"SUCCEEDED"}, timeout=60)
    history = request("GET", f'/api/v1/jobs/{job["id"]}/attempts')
    assert result["attempts"] >= 2
    assert history[0]["status"] == "EXPIRED" and history[-1]["status"] == "SUCCEEDED"
    assert result["workerId"] != worker
    print(json.dumps({"status": "PASS", "job": result["id"], "killed_worker": worker, "recovered_worker": result["workerId"], "attempts": history}))
finally:
    subprocess.run(["docker", "update", "--restart=unless-stopped", container], check=True)
    subprocess.run(["docker", "compose", "start", worker], check=True)
