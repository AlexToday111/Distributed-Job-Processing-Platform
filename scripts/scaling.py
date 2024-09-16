"""Контролируемый локальный эксперимент с 1/2/4/8 исполнителями; затем восстанавливает исполнителей Compose.

Запускайте из корня репозитория при доступном Docker CLI. Использует отдельную
случайную очередь, создаёт временные контейнеры исполнителей и сохраняет замеры в JSON.
"""
import concurrent.futures
import datetime
import json
import os
import pathlib
import subprocess
import time
import uuid
from smoke import request

JOBS = int(os.getenv("SCALING_JOBS", "400"))
TEMP_WORKERS = []
RESULTS = []
QUEUE = "scaling-" + uuid.uuid4().hex[:8]

def docker(*args):
    return subprocess.check_output(["docker", *args], text=True).strip()

def sql(query):
    return docker("compose", "exec", "-T", "postgres", "psql", "-U", "jobs", "-d", "jobs", "-At", "-c", query)

def create_one(_):
    return request("POST", "/api/v1/jobs", {"queue":QUEUE,"type": "sleep", "payload": {"durationMs": 100}, "timeoutSeconds": 10})["id"]

try:
    image = docker("compose", "images", "-q", "worker-1")
    docker("compose", "stop", "worker-1", "worker-2")
    for count in [1, 2, 4, 8]:
        for n in range(len(TEMP_WORKERS), count):
            name = f"job-platform-scaling-{n+1}"
            docker("run", "-d", "--name", name, "--network", "job-platform_default",
                   "-e", f"WORKER_ID=scaling-{n+1}", "-e", "SCHEDULER_ADDR=scheduler:9090",
                   "-e", f"WORKER_QUEUES={QUEUE}",
                   "-e", "WORKER_TOKEN=local-demo-worker-token", "-e", "WORKER_CONCURRENCY=4",
                   "-e", "WORKER_QUEUE_SIZE=4", "-e", "LEASE_DURATION=15s",
                   "-e", "WORKER_HEARTBEAT_INTERVAL=3s", "-e", "WORKER_HEARTBEAT_TIMEOUT=12s",
                   "-e", "OTEL_EXPORTER_OTLP_ENDPOINT=http://jaeger:4318", image, "worker")
            TEMP_WORKERS.append(name)
        time.sleep(1)
        since = datetime.datetime.now(datetime.timezone.utc).isoformat()
        started = time.monotonic()
        with concurrent.futures.ThreadPoolExecutor(max_workers=16) as pool:
            ids = list(pool.map(create_one, range(JOBS)))
        while time.monotonic() - started < 120:
            done = int(sql(f"SELECT count(*) FROM jobs WHERE queue='{QUEUE}' AND created_at>='{since}' AND status='SUCCEEDED'"))
            if done == JOBS:
                break
            time.sleep(0.2)
        else:
            raise RuntimeError("batch did not finish")
        elapsed = time.monotonic() - started
        data = json.loads(sql(f"SELECT json_build_object('p95_queue_wait_seconds',percentile_cont(0.95) WITHIN GROUP(ORDER BY extract(epoch FROM started_at-scheduled_at)),'database_completion_span_seconds',extract(epoch FROM max(completed_at)-min(created_at))) FROM jobs WHERE queue='{QUEUE}' AND created_at>='{since}'"))
        row = {"workers":count,"concurrency":4,"queue_size":4,"jobs":JOBS,"duration_ms":100,"elapsed_seconds":elapsed,"jobs_per_second":JOBS/elapsed, **data}
        RESULTS.append(row)
        print(json.dumps(row), flush=True)
    pathlib.Path("docs/scaling-results.json").write_text(json.dumps({"recorded_at":datetime.datetime.now(datetime.timezone.utc).isoformat(),"results":RESULTS},indent=2))
finally:
    for name in TEMP_WORKERS:
        docker("rm", "-f", name)
    docker("compose", "start", "worker-1", "worker-2")
