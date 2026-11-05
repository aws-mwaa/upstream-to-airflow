/*!
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
import { ChakraProvider, defaultSystem } from "@chakra-ui/react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import "@testing-library/jest-dom/vitest";
import { render, screen } from "@testing-library/react";
import { http, HttpResponse } from "msw";
import { createMemoryRouter, RouterProvider } from "react-router-dom";
import { beforeAll, beforeEach, describe, expect, it } from "vitest";

import type { TriggerDetailResponse } from "openapi/requests/types.gen";

import { TimezoneProvider } from "src/context/timezone";
import { server } from "src/mocks/server";

import { Logs } from "./Logs";

const baseTrigger: TriggerDetailResponse = {
  asset_watchers: [],
  callback: null,
  classpath: "airflow.providers.standard.triggers.temporal.DateTimeTrigger",
  consecutive_failures: 0,
  created_date: "2026-10-01T00:00:00Z",
  dag_id: null,
  first_failed_at: null,
  id: 0,
  kind: "task",
  last_error: null,
  last_event_at: null,
  last_failed_at: null,
  queue: null,
  status: "running",
  task_instance: null,
  team_name: null,
  triggerer_hostname: "triggerer-1",
};

// A deferred task's, a deadline callback's and a failing asset watcher's trigger.
const triggers: Array<TriggerDetailResponse> = [
  {
    ...baseTrigger,
    dag_id: "deferring_dag",
    id: 1,
    task_instance: {
      dag_id: "deferring_dag",
      dag_run_id: "manual_run",
      map_index: -1,
      state: "deferred",
      task_id: "wait",
      try_number: 1,
    },
  },
  {
    ...baseTrigger,
    callback: {
      dag_id: "deadline_dag",
      dag_run_id: "scheduled_run",
      id: "0199a8e0-0000-7000-8000-000000000001",
      state: "pending",
    },
    classpath: "airflow.triggers.callback.CallbackTrigger",
    dag_id: "deadline_dag",
    id: 2,
    kind: "callback",
    status: "pending",
  },
  {
    ...baseTrigger,
    asset_watchers: [
      {
        asset_id: 1,
        asset_name: "asset_with_dependencies",
        asset_uri: "s3://bucket/asset",
        scheduled_dag_ids: [],
        watcher_name: "my_watcher",
      },
    ],
    classpath: "airflow.providers.common.messaging.triggers.msg_queue.MessageQueueTrigger",
    consecutive_failures: 3,
    id: 3,
    kind: "asset_watcher",
    last_error: "ConnectionError: broker.example.com:9092 is unreachable",
    status: "failing",
  },
];

const logLines = [
  { event: "::group::Log message source details" },
  { event: "/root/airflow/logs/triggers/3/7/trigger.log" },
  { event: "::endgroup::" },
  { event: "Trigger 3 starting", level: "info", timestamp: "2026-10-01T00:01:00Z" },
  {
    event: "ConnectionError: broker.example.com:9092 is unreachable",
    level: "error",
    timestamp: "2026-10-01T00:01:01Z",
  },
];

// No retries, so a 403 renders at once instead of after the default backoff.
const renderAt = (path: string) =>
  render(
    <ChakraProvider value={defaultSystem}>
      <QueryClientProvider client={new QueryClient({ defaultOptions: { queries: { retry: false } } })}>
        <TimezoneProvider>
          <RouterProvider
            router={createMemoryRouter([{ element: <Logs />, path: "/triggers/:triggerId/logs" }], {
              initialEntries: [path],
            })}
          />
        </TimezoneProvider>
      </QueryClientProvider>
    </ChakraProvider>,
  );

beforeAll(() => {
  // The log viewer is virtualized and renders no lines in a zero-sized container.
  Object.defineProperty(HTMLElement.prototype, "offsetHeight", { value: 20 });
  Object.defineProperty(HTMLElement.prototype, "clientHeight", { value: 20 });
  Object.defineProperty(HTMLElement.prototype, "offsetWidth", { value: 800 });
});

beforeEach(() => {
  server.use(
    http.get("/api/v2/triggers/:triggerId", ({ params }) => {
      const trigger = triggers.find(({ id }) => String(id) === params.triggerId);

      return trigger === undefined
        ? HttpResponse.json({ detail: "Not Found" }, { status: 404 })
        : HttpResponse.json(trigger);
    }),
    http.get("/api/v2/triggers/:triggerId/logs", () => HttpResponse.json({ content: logLines })),
  );
});

// Tests render i18n keys: the msw handlers serve empty translation bundles.
describe("Trigger Logs tab", () => {
  it("renders the trigger's own log lines", async () => {
    renderAt("/triggers/3/logs");

    expect(await screen.findByText(/Trigger 3 starting/u)).toBeInTheDocument();
    expect(screen.getByText(/broker\.example\.com:9092 is unreachable/u)).toBeInTheDocument();
  });

  it.each([
    {
      link: "/dags/deferring_dag/runs/manual_run/tasks/wait",
      text: "logs.trigger.ownerLog.task",
      triggerId: 1,
    },
    {
      link: "/dags/deadline_dag/runs/scheduled_run/callbacks",
      text: "logs.trigger.ownerLog.callback",
      triggerId: 2,
    },
  ])("links a $text trigger's lines to its owner's full log", async ({ link, text, triggerId }) => {
    renderAt(`/triggers/${triggerId}/logs`);

    expect(await screen.findByText(text)).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "logs.trigger.openOwnerLog" })).toHaveAttribute("href", link);
  });

  it("has no owner log to link for a watcher", async () => {
    renderAt("/triggers/3/logs");

    expect(await screen.findByText(/Trigger 3 starting/u)).toBeInTheDocument();
    expect(screen.queryByTestId("trigger-owner-log")).not.toBeInTheDocument();
  });

  it("explains a 403", async () => {
    server.use(
      http.get("/api/v2/triggers/:triggerId/logs", () =>
        HttpResponse.json({ detail: "Reading a trigger's logs needs task log access" }, { status: 403 }),
      ),
    );

    renderAt("/triggers/3/logs");

    expect(await screen.findByText("logs.trigger.forbidden")).toBeInTheDocument();
  });
});
