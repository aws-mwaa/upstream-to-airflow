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
import { Box } from "@chakra-ui/react";
import { useParams } from "react-router-dom";

import { LogViewer, useLogDisplayOptions } from "src/pages/TaskInstance/Logs/LogViewer";

import { useCallbackLogs } from "src/queries/useLogs";

// Shown under a callback's row in the Callbacks tab.
export const CallbackLogs = ({ callbackId }: { readonly callbackId: string }) => {
  const { dagId = "", runId = "" } = useParams();
  const display = useLogDisplayOptions();

  const { error, fetchedData, isLoading, parsedData } = useCallbackLogs({
    callbackId,
    dagId,
    dagRunId: runId,
    logLevelFilters: display.logLevelFilters,
    showLogLevel: display.showLogLevel,
    showSource: display.showSource,
    showTimestamp: display.showTimestamp,
    sourceFilters: display.sourceFilters,
  });

  return (
    <Box display="flex" flexDirection="column" maxH="60vh" p={2}>
      <LogViewer
        display={display}
        downloadName={`logs_${dagId}_${runId}_callback_${callbackId}.txt`}
        error={error}
        fetchedData={fetchedData}
        isLoading={isLoading}
        parsedData={parsedData}
        title={callbackId}
      />
    </Box>
  );
};
