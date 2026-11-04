#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Runs inside `breeze run`: an Airflow with demo triggers of every kind for the Triggers page (see README.md).
# Fake triggers only, no external services. Stays up until Ctrl-C or until $OUT/STOP appears.
#
#   --listen-on-container-network  bind the API server to every interface of the container, so another
#                                  container on Breeze's network can reach it (see README.md). Default: 127.0.0.1.
set -u
HERE=$(cd "$(dirname "$0")" && pwd)
API_HOST=127.0.0.1
[ "${1:-}" = "--listen-on-container-network" ] && API_HOST=0.0.0.0

export AIRFLOW__CORE__LOAD_EXAMPLES=False AIRFLOW__CORE__DAGS_FOLDER=$HERE/dags
export AIRFLOW__CORE__EXECUTOR=LocalExecutor AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=False
export AIRFLOW__DAG_PROCESSOR__REFRESH_INTERVAL=5 AIRFLOW__DAG_PROCESSOR__MIN_FILE_PROCESS_INTERVAL=10
export AIRFLOW__LOGGING__COLORED_CONSOLE_LOG=False PYTHONPATH=$HERE/mods
export AIRFLOW__CORE__AUTH_MANAGER=airflow.api_fastapi.auth.managers.simple.simple_auth_manager.SimpleAuthManager
export AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_ALL_ADMINS=False AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS=admin:admin
echo '{"admin": "admin"}' > /tmp/triggers_ui_preview_passwords.json
export AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE=/tmp/triggers_ui_preview_passwords.json
# The demo triggers raise botocore errors to look like real queue watchers; nothing calls AWS.
export AWS_DEFAULT_REGION=us-east-1 AWS_EC2_METADATA_DISABLED=true
OUT=${DEMO_OUT:-/files/triggers-ui-preview}
rm -rf "$OUT"; mkdir -p "$OUT/logs"
export AIRFLOW__LOGGING__BASE_LOG_FOLDER=$OUT/logs
# Flag files that drive the fake queues (see mods/demo_triggers.py).
rm -rf /tmp/demo; mkdir -p /tmp/demo
# Two simulated triggerer hosts, each with its own log server on its own loopback address.
grep -q triggerer-0 /etc/hosts || printf '127.0.0.2 triggerer-0\n127.0.0.3 triggerer-1\n' >> /etc/hosts

Q() { python "$HERE/q.py" "$@"; }
N() { Q "$1" 2>/dev/null | tr -dc 0-9; }
say() { echo "[$(date -u +%H:%M:%S)] $*"; }
PIDS=()
bg() { local log=$1; shift; "$@" > "$log" 2>&1 & PIDS+=($!); }
start_triggerer() {  # start_triggerer <host> <ip> [triggerer args]
  local host=$1 ip=$2; shift 2
  mkdir -p "$OUT/$host"
  bg "$OUT/logsrv-$host.log" env DEMO_HOSTNAME="$host" AIRFLOW__CORE__HOSTNAME_CALLABLE=demo_host.get_hostname \
    AIRFLOW__LOGGING__BASE_LOG_FOLDER="$OUT/$host" python "$HERE/logsrv.py" "$ip"
  env DEMO_HOSTNAME="$host" AIRFLOW__CORE__HOSTNAME_CALLABLE=demo_host.get_hostname \
    AIRFLOW__LOGGING__BASE_LOG_FOLDER="$OUT/$host" airflow triggerer --skip-serve-logs "$@" > "$OUT/$host.log" 2>&1 &
  local pid=$!; PIDS+=("$pid"); echo "$pid" > "$OUT/$host.pid"
}
kill9_tree() { for c in $(ps -o pid= --ppid "$1"); do kill -9 "$c" 2>/dev/null; done; kill -9 "$1" 2>/dev/null; }
teardown() {
  say "stopping"
  for p in "${PIDS[@]}"; do kill -TERM "$p" 2>/dev/null; done
  sleep 10
  for p in "${PIDS[@]}"; do kill -9 "$p" 2>/dev/null; done
}
trap 'teardown; exit 0' INT TERM

# The Airflow UI and the SimpleAuthManager login page.
for UI in /opt/airflow/airflow-core/src/airflow/ui /opt/airflow/airflow-core/src/airflow/api_fastapi/auth/managers/simple/ui; do
  if [ ! -f $UI/dist/index.html ]; then
    say "building $UI (once; or run 'breeze compile-ui-assets' beforehand)"
    (cd $UI && pnpm install --frozen-lockfile --config.confirmModulesPurge=false && pnpm build) >> "$OUT/ui-build.log" 2>&1 \
      || { echo "UI BUILD FAILED"; tail -30 "$OUT/ui-build.log"; exit 1; }
  fi
done

airflow db migrate > "$OUT/migrate.log" 2>&1 || { echo "MIGRATE FAILED"; tail -30 "$OUT/migrate.log"; exit 1; }
touch /tmp/demo/payments_queue.expired /tmp/demo/inventory_queue.down
bg "$OUT/api-server.log" airflow api-server --host $API_HOST --port 8080
bg "$OUT/dag-processor.log" airflow dag-processor
bg "$OUT/scheduler.log" airflow scheduler

WQ="select count(distinct trigger_id) from asset_watcher where trigger_id is not null"
for _ in $(seq 1 100); do [ "$(N "$WQ")" -ge 4 ] 2>/dev/null && break; sleep 3; done
say "asset watcher triggers: $(N "$WQ")"
for d in example_deferred_task example_deadline_callback; do
  airflow dags unpause $d > /dev/null 2>&1; airflow dags trigger $d > /dev/null 2>&1
done
# 4 watcher triggers, 4 deferred tasks and 1 deadline callback.
for _ in $(seq 1 80); do [ "$(N 'select count(*) from trigger')" -ge 9 ] 2>/dev/null && break; sleep 3; done
say "triggers before the first triggerer starts: $(N 'select count(*) from trigger')"
start_triggerer triggerer-0 127.0.0.2 --capacity 9

# Orders arrive every 20 s, clickstream every 45 s.
( while true; do sleep 20; touch /tmp/demo/orders_queue.fire; done ) & PIDS+=($!)
( while true; do sleep 45; touch /tmp/demo/clickstream_queue.fire; done ) & PIDS+=($!)

# Inventory: let it fail three times, then let it recover (its endpoint comes back and messages arrive).
IQ="select coalesce(t.consecutive_failures, 0) from trigger t join asset_watcher w on w.trigger_id=t.id where w.name='inventory_queue'"
for _ in $(seq 1 100); do [ "$(N "$IQ")" -ge 3 ] 2>/dev/null && break; sleep 3; done
say "inventory failures: $(N "$IQ"); restoring its endpoint"
rm -f /tmp/demo/inventory_queue.down
for _ in $(seq 1 60); do [ "$(N "$IQ")" = 0 ] && break; touch /tmp/demo/inventory_queue.fire; sleep 3; done
say "inventory failures after recovery: $(N "$IQ")"

# A trigger nobody can take (triggerer-0 is full): pending, then on triggerer-1, then orphaned by a kill -9.
airflow dags unpause example_partner_export > /dev/null 2>&1; airflow dags trigger example_partner_export > /dev/null 2>&1
PQ="select count(*) from trigger t join task_instance ti on ti.trigger_id=t.id where ti.dag_id='example_partner_export'"
for _ in $(seq 1 60); do [ "$(N "$PQ")" -ge 1 ] 2>/dev/null && break; sleep 3; done
say "partner export trigger created; pending for 30 s"
sleep 30
start_triggerer triggerer-1 127.0.0.3
AQ="select count(*) from trigger t join job j on j.id=t.triggerer_id where j.hostname='triggerer-1'"
for _ in $(seq 1 40); do [ "$(N "$AQ")" -ge 1 ] 2>/dev/null && break; sleep 3; done
say "triggerer-1 holds $(N "$AQ") trigger(s); letting it log for 60 s"
sleep 60
say "kill -9 triggerer-1"; kill9_tree "$(cat "$OUT/triggerer-1.pid")"
sleep 45
say "triggers (id, failures, triggerer): $(Q "select t.id, t.consecutive_failures, j.hostname from trigger t left join job j on j.id=t.triggerer_id order by t.id" | tr '\n' ' ')"
for _ in $(seq 1 30); do
  [ "$(curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:8080/api/v2/monitor/health)" = 200 ] && break; sleep 3
done
touch "$OUT/READY"
say "READY: UI at http://<api server>:8080/triggers, log in as admin / admin. Ctrl-C to stop."
while [ ! -f "$OUT/STOP" ]; do sleep 5; done
say "STOP seen"
teardown
echo DONE
