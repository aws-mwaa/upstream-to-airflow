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
import { useTranslation } from "react-i18next";

import { useTriggerServiceGetTriggers } from "openapi/queries";

import { RouterLink } from "src/system-components";

import { StateBadge } from "src/components/StateBadge";

/** Reports failing watchers on the assets the Dag is scheduled on, which keep it from running. */
export const FailingWatchersBadge = ({ dagId }: { readonly dagId: string }) => {
  const { t: translate } = useTranslation("dag");
  // Narrowed to this Dag on the server, so the page limit is per Dag.
  const { data } = useTriggerServiceGetTriggers({
    kind: ["asset_watcher"],
    limit: 100,
    scheduledDagId: dagId,
    status: ["failing"],
  });

  // Scheduled on the asset directly or by an asset name or URI reference; not producing or consuming it in a task.
  const failing = (data?.triggers ?? []).flatMap((trigger) =>
    trigger.asset_watchers
      .filter((watcher) => watcher.scheduled_dag_ids.includes(dagId))
      .map(() => trigger.id),
  );
  const triggerIds = [...new Set(failing)];

  if (failing.length === 0) {
    return undefined;
  }

  return (
    <RouterLink
      to={
        triggerIds.length === 1 ? `/triggers/${triggerIds[0]}` : "/triggers?kind=asset_watcher&status=failing"
      }
    >
      <StateBadge cursor="pointer" data-testid="failing-watchers-badge" state="failed">
        {translate("failingWatchers", { count: failing.length })}
      </StateBadge>
    </RouterLink>
  );
};
