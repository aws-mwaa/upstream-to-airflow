<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
 -->
<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of contents**

- [Triggers UI preview](#triggers-ui-preview)
  - [Run it](#run-it)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Triggers UI preview

A local Airflow with fake triggers of every kind, to try the Triggers page (Browse > Triggers), a trigger's
page with its Logs tab, and the watcher status on an asset's page. No external services: the triggers only
pretend to watch queues and jobs.

What you get, about 5 minutes after start:

- asset watchers: `orders_queue` (healthy, fires every 20 s), `payments_queue` (failing: expired credentials),
  `inventory_queue` (failed three times, then recovered), and `clickstream_queue`, one trigger shared by the
  watchers of two assets;
- deferred task triggers (`example_deferred_task`, one of them mapped) and a deadline callback trigger
  (`example_deadline_callback`);
- `example_partner_export`'s trigger: pending while the only triggerer is full, then picked up by a second
  triggerer, then orphaned when that triggerer is killed.

## Run it

From the repository root:

```bash
breeze run --backend postgres bash dev/triggers-ui-preview/demo.sh --listen-on-container-network
```

It builds the UI if it was not built yet (or run `breeze compile-ui-assets` first), prints `READY` when the
data is in place, and stops on Ctrl-C. Log in as **admin / admin** (SimpleAuthManager).

`breeze run` publishes no port. To open the UI in your browser, publish it on your machine's loopback only,
from a second terminal:

```bash
C=$(docker ps --format '{{.Names}}' | grep -- '-airflow-run-' | head -1)
docker run --rm -p 127.0.0.1:28080:8080 --network "$(docker inspect -f '{{range $k, $v := .NetworkSettings.Networks}}{{$k}}{{end}}' "$C")" \
  alpine/socat TCP-LISTEN:8080,fork,reuseaddr "TCP:$C:8080"
```

and open http://127.0.0.1:28080/triggers.
