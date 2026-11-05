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
import { useMemo } from "react";

import { Button, createListCollection, Field, HStack, Input, Stack, Text } from "@chakra-ui/react";
import { Controller, useFieldArray, type Control } from "react-hook-form";
import { useTranslation } from "react-i18next";
import { FiPlus, FiTrash2 } from "react-icons/fi";

import { useGridServiceGetDagStructure, usePoolServiceGetPools } from "openapi/queries";
import type { GridNodeResponse } from "openapi/requests/types.gen";

import { Accordion, IconButton, Select } from "src/system-components";

import { useConfig } from "src/queries/useConfig";

import { emptyPoolRemapRow, emptyTaskOverrideRow } from "./overridesBody";
import type { DagRunTriggerParams } from "./types";

type Option = { label: string; value: string };

const flattenNodes = (nodes: Array<GridNodeResponse>, groupSuffix: string): Array<Option> =>
  nodes.flatMap((node) =>
    node.children
      ? [
          { label: `${node.id} (${groupSuffix})`, value: node.id },
          ...flattenNodes(node.children, groupSuffix),
        ]
      : [{ label: node.id, value: node.id }],
  );

type OptionSelectProps = {
  readonly items: Array<Option>;
  readonly label: string;
  readonly onChange: (value: string) => void;
  readonly placeholder: string;
  readonly testId: string;
  readonly value: string | undefined;
};

const OptionSelect = ({ items, label, onChange, placeholder, testId, value }: OptionSelectProps) => {
  const collection = useMemo(() => createListCollection({ items }), [items]);

  return (
    <Field.Root flex={2}>
      <Field.Label fontSize="sm">{label}</Field.Label>
      <Select.Root
        collection={collection}
        data-testid={testId}
        onValueChange={(details) => onChange(details.value[0] ?? "")}
        size="sm"
        value={value === undefined || value === "" ? [] : [value]}
      >
        <Select.Trigger>
          <Select.ValueText placeholder={placeholder} />
        </Select.Trigger>
        <Select.Content maxH="240px" overflowY="auto">
          {collection.items.map((item) => (
            <Select.Item item={item} key={item.value}>
              {item.label}
            </Select.Item>
          ))}
        </Select.Content>
      </Select.Root>
    </Field.Root>
  );
};

type SchedulingOverridesProps = {
  readonly control: Control<DagRunTriggerParams>;
  readonly dagId: string;
  readonly open: boolean;
};

const SchedulingOverrides = ({ control, dagId, open }: SchedulingOverridesProps) => {
  const { t: translate } = useTranslation("components");
  const queues = (useConfig("override_target_queues") as Array<string> | undefined) ?? [];
  const { data: poolData } = usePoolServiceGetPools({ limit: 100 }, undefined, { enabled: open });
  const { data: structure } = useGridServiceGetDagStructure({ dagId, limit: 1 }, undefined, {
    enabled: open,
  });
  const taskRows = useFieldArray({ control, name: "overrides" });
  const remapRows = useFieldArray({ control, name: "poolRemaps" });

  const pools = poolData?.pools ?? [];
  const nominatedPools = pools
    .filter((pool) => pool.allow_overrides)
    .map((pool) => ({ label: pool.name, value: pool.name }));
  const allPools = pools.map((pool) => ({ label: pool.name, value: pool.name }));
  const queueOptions = queues.map((queue) => ({ label: queue, value: queue }));
  const targets = flattenNodes(structure ?? [], translate("triggerDag.overrides.group"));

  // Nothing can be overridden until an administrator nominates a pool or lists a queue.
  if (nominatedPools.length === 0 && queueOptions.length === 0) {
    return undefined;
  }

  const keep = translate("triggerDag.overrides.keep");

  return (
    <Accordion.Root collapsible overflow="visible" size="lg" variant="enclosed">
      <Accordion.Item value="schedulingOverrides">
        <Accordion.ItemTrigger cursor="button">
          <Text>{translate("triggerDag.overrides.title")}</Text>
        </Accordion.ItemTrigger>
        <Accordion.ItemContent>
          <Stack data-testid="scheduling-overrides" gap={3} pb={2}>
            <Text color="fg.muted" fontSize="sm">
              {translate("triggerDag.overrides.help")}
            </Text>
            {taskRows.fields.map((row, index) => (
              <HStack alignItems="end" gap={2} key={row.id}>
                <Controller
                  control={control}
                  name={`overrides.${index}.target`}
                  render={({ field }) => (
                    <OptionSelect
                      items={targets}
                      label={translate("triggerDag.overrides.task")}
                      onChange={field.onChange}
                      placeholder={translate("triggerDag.overrides.task")}
                      testId={`override-target-${index}`}
                      value={field.value}
                    />
                  )}
                />
                <Controller
                  control={control}
                  name={`overrides.${index}.pool`}
                  render={({ field }) => (
                    <OptionSelect
                      items={nominatedPools}
                      label={translate("triggerDag.overrides.pool")}
                      onChange={field.onChange}
                      placeholder={keep}
                      testId={`override-pool-${index}`}
                      value={field.value}
                    />
                  )}
                />
                <Controller
                  control={control}
                  name={`overrides.${index}.poolSlots`}
                  render={({ field }) => (
                    <Field.Root flex={1}>
                      <Field.Label fontSize="sm">{translate("triggerDag.overrides.poolSlots")}</Field.Label>
                      <Input {...field} min={1} placeholder={keep} size="sm" type="number" />
                    </Field.Root>
                  )}
                />
                <Controller
                  control={control}
                  name={`overrides.${index}.priorityWeight`}
                  render={({ field }) => (
                    <Field.Root flex={1}>
                      <Field.Label fontSize="sm">
                        {translate("triggerDag.overrides.priorityWeight")}
                      </Field.Label>
                      <Input {...field} placeholder={keep} size="sm" type="number" />
                    </Field.Root>
                  )}
                />
                {queueOptions.length > 0 ? (
                  <Controller
                    control={control}
                    name={`overrides.${index}.queue`}
                    render={({ field }) => (
                      <OptionSelect
                        items={queueOptions}
                        label={translate("triggerDag.overrides.queue")}
                        onChange={field.onChange}
                        placeholder={keep}
                        testId={`override-queue-${index}`}
                        value={field.value}
                      />
                    )}
                  />
                ) : undefined}
                <IconButton
                  label={translate("triggerDag.overrides.remove")}
                  onClick={() => taskRows.remove(index)}
                  size="sm"
                  variant="ghost"
                >
                  <FiTrash2 />
                </IconButton>
              </HStack>
            ))}
            {remapRows.fields.map((row, index) => (
              <HStack alignItems="end" gap={2} key={row.id}>
                <Controller
                  control={control}
                  name={`poolRemaps.${index}.from`}
                  render={({ field }) => (
                    <OptionSelect
                      items={allPools}
                      label={translate("triggerDag.overrides.remapFrom")}
                      onChange={field.onChange}
                      placeholder={translate("triggerDag.overrides.pool")}
                      testId={`remap-from-${index}`}
                      value={field.value}
                    />
                  )}
                />
                <Controller
                  control={control}
                  name={`poolRemaps.${index}.to`}
                  render={({ field }) => (
                    <OptionSelect
                      items={nominatedPools}
                      label={translate("triggerDag.overrides.remapTo")}
                      onChange={field.onChange}
                      placeholder={translate("triggerDag.overrides.pool")}
                      testId={`remap-to-${index}`}
                      value={field.value}
                    />
                  )}
                />
                <IconButton
                  label={translate("triggerDag.overrides.remove")}
                  onClick={() => remapRows.remove(index)}
                  size="sm"
                  variant="ghost"
                >
                  <FiTrash2 />
                </IconButton>
              </HStack>
            ))}
            <HStack>
              <Button
                data-testid="add-task-override"
                onClick={() => taskRows.append({ ...emptyTaskOverrideRow })}
                size="sm"
                variant="outline"
              >
                <FiPlus /> {translate("triggerDag.overrides.addTask")}
              </Button>
              {nominatedPools.length > 0 ? (
                <Button
                  data-testid="add-pool-remap"
                  onClick={() => remapRows.append({ ...emptyPoolRemapRow })}
                  size="sm"
                  variant="outline"
                >
                  <FiPlus /> {translate("triggerDag.overrides.addRemap")}
                </Button>
              ) : undefined}
            </HStack>
          </Stack>
        </Accordion.ItemContent>
      </Accordion.Item>
    </Accordion.Root>
  );
};

export default SchedulingOverrides;
