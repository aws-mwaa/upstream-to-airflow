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
import type { DagRunSchedulingOverrides, TaskSchedulingOverride } from "openapi/requests/types.gen";

// Form inputs keep strings so that an empty field means "keep the task's own value".
export type TaskOverrideRow = {
  pool: string;
  poolSlots: string;
  priorityWeight: string;
  queue: string;
  target: string;
};

export type PoolRemapRow = {
  from: string;
  to: string;
};

export const emptyTaskOverrideRow: TaskOverrideRow = {
  pool: "",
  poolSlots: "",
  priorityWeight: "",
  queue: "",
  target: "",
};

export const emptyPoolRemapRow: PoolRemapRow = { from: "", to: "" };

const toTaskEntry = (row: TaskOverrideRow): TaskSchedulingOverride => ({
  ...(row.pool === "" ? {} : { pool: row.pool }),
  ...(row.poolSlots === "" ? {} : { pool_slots: Number(row.poolSlots) }),
  ...(row.priorityWeight === "" ? {} : { priority_weight: Number(row.priorityWeight) }),
  ...(row.queue === "" ? {} : { queue: row.queue }),
});

export const toOverridesBody = (
  rows: Array<TaskOverrideRow> = [],
  remaps: Array<PoolRemapRow> = [],
): DagRunSchedulingOverrides | undefined => {
  const tasks = Object.fromEntries(
    rows
      .filter((row) => row.target !== "")
      .map((row) => [row.target, toTaskEntry(row)] as const)
      .filter(([, entry]) => Object.keys(entry).length > 0),
  );
  const pools = Object.fromEntries(
    remaps.filter((remap) => remap.from !== "" && remap.to !== "").map((remap) => [remap.from, remap.to]),
  );

  if (Object.keys(tasks).length === 0 && Object.keys(pools).length === 0) {
    return undefined;
  }

  return {
    ...(Object.keys(pools).length > 0 ? { pools } : {}),
    ...(Object.keys(tasks).length > 0 ? { tasks } : {}),
  };
};
