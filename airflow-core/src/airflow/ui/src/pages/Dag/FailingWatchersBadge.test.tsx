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
import "@testing-library/jest-dom/vitest";
import { render, screen, waitFor } from "@testing-library/react";
import { http, HttpResponse } from "msw";
import { describe, expect, it } from "vitest";

import type { TriggerDetailResponse } from "openapi/requests/types.gen";

import { server } from "src/mocks/server";
import { Wrapper } from "src/utils/Wrapper";

import { FailingWatchersBadge } from "./FailingWatchersBadge";

// A failing watcher's trigger, with one watcher per list of the Dags scheduled on its asset.
const trigger = (id: number, ...scheduledDagIds: Array<Array<string>>) =>
  ({
    asset_watchers: scheduledDagIds.map((dagIds, index) => ({
      asset_id: index,
      asset_name: `asset_${index}`,
      asset_uri: `s3://bucket/asset_${index}`,
      scheduled_dag_ids: dagIds,
      watcher_name: `watcher_${id}_${index}`,
    })),
    id,
    status: "failing",
  }) as unknown as TriggerDetailResponse;

const serveTriggers = (triggers: Array<TriggerDetailResponse>) => {
  const requested: Array<string> = [];

  server.use(
    http.get("/api/v2/triggers", ({ request }) => {
      requested.push(new URL(request.url).search);

      return HttpResponse.json({ total_entries: triggers.length, triggers });
    }),
  );

  return requested;
};

// Tests render i18n keys: the msw handlers serve empty translation bundles.
describe("FailingWatchersBadge", () => {
  it("links to the trigger of a failing watcher on an asset the Dag is scheduled on", async () => {
    const requested = serveTriggers([
      trigger(7, ["my_dag"]),
      // A watcher on an asset the Dag only produces: it does not hold the Dag up.
      trigger(9, ["other_dag"]),
    ]);

    render(<FailingWatchersBadge dagId="my_dag" />, { wrapper: Wrapper });

    expect(await screen.findByTestId("failing-watchers-badge")).toHaveTextContent("failingWatchers");
    expect(screen.getByRole("link")).toHaveAttribute("href", "/triggers/7");
    expect(requested).toHaveLength(1);
    expect(Object.fromEntries(new URLSearchParams(requested[0]))).toEqual({
      kind: "asset_watcher",
      limit: "100",
      scheduled_dag_id: "my_dag",
      status: "failing",
    });
  });

  it("links to the failing watchers in the Triggers list when there are several", async () => {
    serveTriggers([trigger(7, ["my_dag"]), trigger(9, ["my_dag", "other_dag"])]);

    render(<FailingWatchersBadge dagId="my_dag" />, { wrapper: Wrapper });

    expect(await screen.findByRole("link")).toHaveAttribute(
      "href",
      "/triggers?kind=asset_watcher&status=failing",
    );
  });

  it("shows nothing while no failing watcher runs the Dag", async () => {
    const requested = serveTriggers([trigger(9, ["other_dag"], [])]);

    render(<FailingWatchersBadge dagId="my_dag" />, { wrapper: Wrapper });

    await waitFor(() => expect(requested).toHaveLength(1));
    expect(screen.queryByTestId("failing-watchers-badge")).not.toBeInTheDocument();
  });
});
