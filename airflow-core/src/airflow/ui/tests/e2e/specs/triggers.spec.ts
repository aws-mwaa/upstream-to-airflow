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
import { expect, test } from "tests/e2e/fixtures";
import { baseUrl } from "tests/e2e/utils/shared";

const KINDS = [
  { label: "Task", value: "task" },
  { label: "Callback", value: "callback" },
  { label: "Asset Watcher", value: "asset_watcher" },
];

test.describe("Triggers page", () => {
  test.setTimeout(120_000);

  test.beforeAll(async ({ authenticatedRequest }) => {
    const response = await authenticatedRequest.get(`${baseUrl}/api/v2/triggers?limit=1`);

    test.skip(response.status() === 404, "This Airflow has no GET /api/v2/triggers");
  });

  test("lists triggers of every kind", async ({ triggersPage }) => {
    for (const kind of KINDS) {
      await triggersPage.navigate(`?kind=${kind.value}`);
      // Retried: the table shows placeholder rows with empty cells while it loads.
      await expect(async () => {
        const kinds = await triggersPage.columnTexts("Kind");

        expect(kinds.length).toBeGreaterThan(0);
        expect(new Set(kinds)).toEqual(new Set([kind.label]));
      }).toPass({ timeout: 30_000 });
    }
  });

  test("filters to failing watchers and reads one's error", async ({ page, triggersPage }) => {
    await triggersPage.navigate();
    await triggersPage.addFilter("Kind", "Asset Watcher");
    await triggersPage.addFilter("Status", "Failing");

    await expect(page).toHaveURL(/kind=asset_watcher/u);
    await expect(page).toHaveURL(/status=failing/u);
    await expect(async () => {
      expect(new Set(await triggersPage.columnTexts("Kind"))).toEqual(new Set(["Asset Watcher"]));
      expect(new Set(await triggersPage.columnTexts("Status"))).toEqual(new Set(["Failing"]));
    }).toPass({ timeout: 30_000 });

    await triggersPage.tableRows.first().getByRole("link").first().click();
    await expect(page).toHaveURL(/\/triggers\/\d+$/u);
    await expect(page.getByTestId("trigger-last-error")).toHaveText(/\S/u, { timeout: 30_000 });
  });
});
