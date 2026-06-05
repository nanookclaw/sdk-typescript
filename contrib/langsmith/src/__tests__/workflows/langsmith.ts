/**
 * Workflow fixtures for the plugin test suite.
 *
 * This module is the worker's `workflowsPath`. The plugin injects its workflow
 * interceptor module + deterministic context provider into the bundle, so the
 * `traceable` calls embedded in some bodies below nest under the workflow run
 * without any plugin-specific imports here.
 *
 * @module
 */

import { traceable } from 'langsmith/traceable';
import {
  ApplicationFailure,
  condition,
  continueAsNew,
  defineQuery,
  defineSignal,
  defineUpdate,
  proxyActivities,
  setHandler,
  startChild,
  workflowInfo,
} from '@temporalio/workflow';
import { ApplicationFailureCategory } from '@temporalio/common';

import type * as activities from '../activities/langsmith';

const { simpleActivity, plainActivity, failingActivity, benignFailingActivity } = proxyActivities<typeof activities>({
  startToCloseTimeout: '1 minute',
  retry: { maximumAttempts: 1 },
});

/**
 * A native `traceable` invoked directly inside a *workflow* body (not an
 * activity). Defining it at module scope is exactly how a user writes their own
 * instrumentation; nothing here is plugin-specific.
 */
const workflowInnerCall = traceable(async (input: string): Promise<string> => `wf-llm:${input}`, {
  name: 'workflow_inner_call',
});

/** Single plain activity — the minimal "happy path" used by the basic tree, replay, and side-effect tests. */
export async function SimpleWorkflow(input: string): Promise<string> {
  return simpleActivity(input);
}

/**
 * Calls native `traceable` **directly in the workflow body**. This is the only
 * fixture that exercises the in-isolate LangSmith context provider
 * (`WorkflowContextManager.run`/`stack`): inside the V8 isolate
 * `node:async_hooks` is unavailable, so LangSmith's default provider degrades to
 * a mock whose `getStore()` is always `undefined`. The provider the plugin
 * installs is what lets this `traceable` find its parent run and nest under the
 * workflow run with zero code changes — the headline "works unchanged inside
 * workflows" claim. Proven by test-comprehensive.ts.
 */
export async function WorkflowBodyTraceableWorkflow(input: string): Promise<string> {
  return workflowInnerCall(input);
}

/**
 * No `traceable` anywhere in the workflow or its activity. Used by the
 * kill-switch test, where any captured run would have to come from the plugin
 * itself, so an empty collector proves emission was fully suppressed.
 */
export async function PlainWorkflow(input: string): Promise<string> {
  return plainActivity(input);
}

export const mySignal = defineSignal<[string]>('my_signal');
export const completeSignal = defineSignal('complete');
export const myQuery = defineQuery<string>('my_query');
export const myUpdate = defineUpdate<string, [string]>('my_update');
export const myUnvalidatedUpdate = defineUpdate<string, [string]>('my_unvalidated_update');

/**
 * Exercises every inbound handler surface: a validated update, an unvalidated
 * update, a query, and two signals (one carries data, one releases the wait).
 */
export async function HandlersWorkflow(): Promise<string> {
  let lastSignal = '';
  let done = false;

  setHandler(myQuery, () => lastSignal);
  setHandler(mySignal, (value: string) => {
    lastSignal = value;
  });
  setHandler(completeSignal, () => {
    done = true;
  });
  setHandler(myUpdate, (value: string) => `updated:${value}`, {
    validator: (value: string) => {
      if (value === 'reject') {
        throw new Error('rejected by validator');
      }
    },
  });
  setHandler(myUnvalidatedUpdate, (value: string) => `unvalidated:${value}`);

  await condition(() => done);
  return lastSignal;
}

/**
 * Waits for `complete` so a parent can signal it before it finishes. Used as the
 * child target of {@link SignalChildWorkflow} to exercise the workflow-side
 * `signalWorkflow` outbound interceptor (`SignalChildWorkflow:` marker).
 */
export async function SignalReceiverWorkflow(): Promise<string> {
  let value = '';
  setHandler(mySignal, (v: string) => {
    value = v;
  });
  await condition(() => value !== '');
  return value;
}

/**
 * Starts a child workflow and signals it from inside the workflow body, firing
 * the outbound `signalWorkflow` interceptor with a `child` target so a
 * `SignalChildWorkflow:` marker is emitted.
 */
export async function SignalChildWorkflow(input: string): Promise<string> {
  const child = await startChild(SignalReceiverWorkflow, {
    workflowId: `${workflowInfo().workflowId}-signal-child`,
  });
  await child.signal(mySignal, input);
  return child.result();
}

/** Calls an activity that fails with a non-benign error, to assert error marking on the run. */
export async function ErrorWorkflow(): Promise<void> {
  await failingActivity();
}

/** Calls an activity that fails with a BENIGN-category error, which must NOT mark the run errored. */
export async function BenignWorkflow(): Promise<void> {
  await benignFailingActivity();
}

/**
 * Continues as new once, then returns. Used to assert the successor run
 * continues the same LangSmith trace (the header is propagated through the
 * continue-as-new input).
 */
export async function ContinueAsNewWorkflow(iteration: number): Promise<number> {
  if (iteration === 0) {
    await continueAsNew<typeof ContinueAsNewWorkflow>(iteration + 1);
  }
  return iteration;
}

/**
 * A workflow that fails directly (no activity) with a BENIGN-category failure.
 * Used to assert the *workflow-inbound* error path leaves the `RunWorkflow:` run
 * unmarked — distinct from the activity-inbound benign path covered by
 * {@link BenignWorkflow}. The category (not just the type string) is what
 * `describeError` keys on, so it must be set explicitly.
 */
export async function BenignWorkflowDirect(): Promise<void> {
  throw ApplicationFailure.create({
    message: 'benign-control',
    type: 'BenignStop',
    category: ApplicationFailureCategory.BENIGN,
    nonRetryable: true,
  });
}
