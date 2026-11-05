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
#
# AIP-117 demo steps against `breeze start-airflow`. Run from the host: ./dev/aip117_demo/demo.sh <step>
set -euo pipefail

API="${AIRFLOW_API:-http://localhost:28080}"

token() {
    curl -sf -X POST "${API}/auth/token" -H 'Content-Type: application/json' \
        -d '{"username": "admin", "password": "admin"}' |
        python3 -c 'import json, sys; print(json.load(sys.stdin)["access_token"])'
}

call() {
    local method=$1 path=$2 body=${3:-}
    echo "> ${method} ${path} ${body}" >&2
    curl -s -X "${method}" "${API}/api/v2${path}" \
        -H "Authorization: Bearer ${TOKEN}" -H 'Content-Type: application/json' \
        ${body:+-d "${body}"} -w '\n< HTTP %{http_code}\n'
}

pool() {
    call POST /pools "{\"name\": \"$1\", \"slots\": $2, \"allow_overrides\": $3}" >/dev/null || true
}

TOKEN=${AIRFLOW_TOKEN:-$(token)}

case "${1:-}" in
setup)
    pool wh_small 2 false
    pool wh_large 16 false
    pool etl_pool 2 false
    pool backfill_pool 2 false
    pool heavy_pool 16 false
    pool etl_overflow_pool 16 true
    for dag in aip117_regional_sync aip117_daily_load aip117_reprocess_orders; do
        call PATCH "/dags/${dag}" '{"is_paused": false}' >/dev/null
    done
    call GET /pools
    ;;
trigger-small | trigger-large)
    size=${1#trigger-}
    call POST /dags/aip117_regional_sync/dagRuns \
        "{\"logical_date\": null, \"conf\": {\"warehouse_pool\": \"wh_${size}\"}}"
    ;;
trigger-bad-conf)
    # Not in the param's enum: rejected like any invalid conf.
    call POST /dags/aip117_regional_sync/dagRuns \
        '{"logical_date": null, "conf": {"warehouse_pool": "default_pool"}}'
    ;;
backfill)
    call POST /backfills \
        "{\"dag_id\": \"aip117_daily_load\", \"from_date\": \"$(date -u -v-6d +%Y-%m-%dT00:00:00Z 2>/dev/null || date -u -d '-6 days' +%Y-%m-%dT00:00:00Z)\", \"to_date\": \"$(date -u -v-1d +%Y-%m-%dT00:00:00Z 2>/dev/null || date -u -d '-1 day' +%Y-%m-%dT00:00:00Z)\", \"max_active_runs\": 6}"
    ;;
override)
    # heavy_pool exists but is not nominated, so this fails with the same error as an unknown pool.
    call POST /dags/aip117_reprocess_orders/dagRuns \
        '{"logical_date": null, "overrides": {"tasks": {"extract": {"pool": "heavy_pool", "pool_slots": 4}}}}'
    ;;
override-unknown)
    call POST /dags/aip117_reprocess_orders/dagRuns \
        '{"logical_date": null, "overrides": {"tasks": {"extract": {"pool": "no_such_pool"}}}}'
    ;;
nominate)
    call PATCH /pools/heavy_pool '{"name": "heavy_pool", "slots": 16, "include_deferred": false, "allow_overrides": true}'
    ;;
override-group)
    call POST /dags/aip117_reprocess_orders/dagRuns \
        '{"logical_date": null, "overrides": {"tasks": {"extract": {"pool": "heavy_pool", "pool_slots": 4}, "transform": {"pool": "heavy_pool"}}}}'
    ;;
remap)
    call POST /dags/aip117_reprocess_orders/dagRuns \
        '{"logical_date": null, "overrides": {"pools": {"etl_pool": "etl_overflow_pool"}}}'
    ;;
tis)
    # Effective values per task instance of the latest run of a Dag.
    dag=${2:-aip117_reprocess_orders}
    run=$(call GET "/dags/${dag}/dagRuns?order_by=-run_after&limit=1" 2>/dev/null |
        python3 -c 'import json, sys; runs = json.loads(sys.stdin.read().rsplit("\n<", 1)[0])["dag_runs"]; print(runs[0]["dag_run_id"] if runs else "")')
    [ -n "${run}" ] || { echo "no runs of ${dag}"; exit 0; }
    echo "run: ${run}"
    call GET "/dags/${dag}/dagRuns/${run}/taskInstances?limit=100" 2>/dev/null |
        python3 -c '
import json, sys
tis = json.loads(sys.stdin.read().rsplit("\n<", 1)[0])["task_instances"]
for ti in sorted(tis, key=lambda t: t["task_id"]):
    print("{task_id:28} pool={pool:18} slots={pool_slots} weight={priority_weight} {state}".format(**ti))
'
    ;;
pools-by-run)
    # Which pools each run of a Dag used; run IDs start with the run type.
    dag=${2:-aip117_daily_load}
    call GET "/dags/${dag}/dagRuns/~/taskInstances?limit=200" 2>/dev/null |
        python3 -c '
import collections, json, sys
tis = json.loads(sys.stdin.read().rsplit("\n<", 1)[0])["task_instances"]
by_run = collections.defaultdict(set)
for ti in tis:
    by_run[ti["dag_run_id"]].add(ti["pool"])
for run, pools in sorted(by_run.items()):
    print("{:58} {}".format(run, ", ".join(sorted(pools))))
'
    ;;
*)
    echo "usage: $0 setup|trigger-small|trigger-large|trigger-bad-conf|backfill|override|override-unknown|nominate|override-group|remap|tis [dag_id]|pools-by-run [dag_id]" >&2
    exit 1
    ;;
esac
