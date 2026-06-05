/**
 * The comprehensive trace-tree workflow: a continue-as-new state machine that
 * touches every Temporal boundary the plugin instruments — each raw and wrapped
 * in a user `traceable` — so the test (test-comprehensive-tree.ts) can assert the
 * exact emitted run hierarchy.
 *
 * Every instrumented call in the body and in every handler runs sequentially.
 * The isolate's workflow-body `traceable` parent resolution falls back to the
 * workflow run under concurrency, so a `Promise.all` over instrumented calls
 * would make the tree nondeterministic.
 *
 * @module
 */

import { getCurrentRunTree, traceable } from 'langsmith/traceable';
import {
  condition,
  continueAsNew,
  createNexusServiceClient,
  defineQuery,
  defineSignal,
  defineUpdate,
  executeChild,
  proxyActivities,
  proxyLocalActivities,
  setHandler,
  startChild,
  workflowInfo,
} from '@temporalio/workflow';

import type * as activities from '../activities/comprehensive';
import { comprehensiveNexusService } from '../stubs/nexus';

/** Nexus endpoint the test creates before starting the workflow. */
export const COMPREHENSIVE_NEXUS_ENDPOINT = 'comprehensive-nexus-endpoint';

const { comprehensiveActivity, notifyReady } = proxyActivities<typeof activities>({
  startToCloseTimeout: '1 minute',
  retry: { maximumAttempts: 1 },
});

const { comprehensiveLocalActivity } = proxyLocalActivities<typeof activities>({
  startToCloseTimeout: '1 minute',
  retry: { maximumAttempts: 1 },
});

const workflowInnerCall = traceable(async (input: string): Promise<string> => `wf:${input}`, {
  name: 'workflow_inner_call',
});
const signalInnerCall = traceable(async (input: string): Promise<string> => `sig:${input}`, {
  name: 'signal_inner_call',
});
const updateInnerCall = traceable(async (input: string): Promise<string> => `upd:${input}`, {
  name: 'update_inner_call',
});

/**
 * Emit a nested user run under the current run, synchronously. Query handlers and
 * update validators must be synchronous, so they cannot use `traceable` (which
 * returns a Promise); this mirrors how a user would hand-instrument a sync
 * handler. A no-op when there is no current run to nest under.
 */
function syncInnerRun(name: string): void {
  const parent = getCurrentRunTree(true);
  if (!parent) {
    return;
  }
  const child = parent.createChild({ name });
  void child.postRun();
  void child.end({});
  void child.patchRun();
}

export const comprehensiveSignal = defineSignal<[string]>('signal');
export const completeSignal = defineSignal('complete');
export const comprehensiveQuery = defineQuery<string, [string]>('query');
export const comprehensiveUpdate = defineUpdate<string, [string]>('update');

/** Child workflow, used raw and wrapped. */
export async function ComprehensiveChildWorkflow(input: string): Promise<string> {
  return comprehensiveActivity(input);
}

/** Signal-child target: waits for a signal, then returns its payload. */
export async function ComprehensiveReceiverWorkflow(): Promise<string> {
  let value = '';
  setHandler(comprehensiveSignal, (v: string) => {
    value = v;
  });
  await condition(() => value !== '');
  return value;
}

/** Register every inbound handler; each body emits a nested user run. */
function registerHandlers(state: { lastSignal: string; done: boolean }): void {
  setHandler(comprehensiveSignal, async (value: string) => {
    state.lastSignal = value;
    await signalInnerCall(value);
  });
  setHandler(completeSignal, () => {
    state.done = true;
  });
  setHandler(comprehensiveQuery, (value: string) => {
    syncInnerRun('query_inner_call');
    return `qry:${value}`;
  });
  setHandler(comprehensiveUpdate, async (value: string) => updateInnerCall(value), {
    validator: (_value: string) => {
      syncInnerRun('validator_inner_call');
    },
  });
}

export async function ComprehensiveWorkflow(iteration: number): Promise<string> {
  const state = { lastSignal: '', done: false };
  registerHandlers(state);

  if (iteration === 0) {
    await comprehensiveActivity('a');
    await traceable(async () => comprehensiveActivity('b'), { name: 'user_wrap_activity' })();

    await comprehensiveLocalActivity('c');

    await executeChild(ComprehensiveChildWorkflow, {
      args: ['d'],
      workflowId: `${workflowInfo().workflowId}-child-raw`,
    });
    await traceable(
      async () =>
        executeChild(ComprehensiveChildWorkflow, {
          args: ['e'],
          workflowId: `${workflowInfo().workflowId}-child-wrapped`,
        }),
      { name: 'user_wrap_child' }
    )();

    const receiver = await startChild(ComprehensiveReceiverWorkflow, {
      workflowId: `${workflowInfo().workflowId}-signal-child`,
    });
    await receiver.signal(comprehensiveSignal, 'f');
    await receiver.result();

    await createNexusServiceClient({
      service: comprehensiveNexusService,
      endpoint: COMPREHENSIVE_NEXUS_ENDPOINT,
    }).executeOperation(comprehensiveNexusService.operations.greet, { name: 'g' }, {});

    await workflowInnerCall('h');

    // Outbound boundaries done; release the driver to issue the handler calls,
    // then wait for its completion signal before continuing as new.
    await notifyReady();
    await condition(() => state.done);
    await continueAsNew<typeof ComprehensiveWorkflow>(iteration + 1);
  }

  // The continue-as-new successor: returns immediately; its RunWorkflow: shares the trace.
  return state.lastSignal;
}
