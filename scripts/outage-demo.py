"""Проверить отказы планировщика и базы данных в локальной демонстрации и восстановить оба сервиса."""
import json
import subprocess
import time
import urllib.error
import urllib.request
from smoke import request, wait, BASE

def compose(*args):
    subprocess.run(["docker", "compose", *args], check=True)

def status(url):
    try:
        with urllib.request.urlopen(url, timeout=5) as res:
            return res.status
    except urllib.error.HTTPError as error:
        return error.code

job = request("POST", "/api/v1/jobs", {"type":"sleep","payload":{"durationMs":4000}})
wait(job["id"], {"RUNNING"})
try:
    compose("stop", "scheduler")
    time.sleep(5)
    # Обработчик может завершиться, но во время отказа его результат остаётся неподтверждённым.
    assert request("GET", "/api/v1/jobs/"+job["id"])["status"] == "RUNNING"
finally:
    compose("start", "scheduler")
result = wait(job["id"], {"SUCCEEDED"})
assert result["attempts"] == 1

try:
    compose("stop", "postgres")
    assert status(BASE+"/health/live") == 200
    assert status(BASE+"/health/ready") == 503
    try:
        request("POST", "/api/v1/jobs", {"type":"sleep","payload":{"durationMs":0}})
        raise AssertionError("create reported success without persistence")
    except urllib.error.HTTPError as error:
        assert error.code == 500
finally:
    compose("start", "postgres")
deadline = time.monotonic()+30
while status(BASE+"/health/ready") != 200:
    assert time.monotonic() < deadline
    time.sleep(0.2)
assert request("GET", "/api/v1/jobs/"+job["id"])["status"] == "SUCCEEDED"
print(json.dumps({"status":"PASS","scheduler_outage":"result retained, one attempt","database_outage":"readiness 503, no false create success, data retained"}))
