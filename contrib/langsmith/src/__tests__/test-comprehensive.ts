/**
 * Run-hierarchy edge cases that the comprehensive trace-tree test
 * (test-comprehensive-tree.ts) cannot cover, because its workflow always runs
 * under a client-side `user_pipeline` root and its assertion is name-only:
 *  - a workflow started with NO ambient run (two separate roots),
 *  - a root workflow-body `traceable` with no propagated parent (the `crypto`
 *    regression — must not crash, and emits no spurious synthetic root),
 *  - plugin options (projectName / tags / scrubbed metadata) carried onto runs
 *    (a per-field check `dumpTraces` does not render).
 *
 * @module
 */

import test from 'ava';

import * as activities from './activities/langsmith';
import { InMemoryRunCollector, dumpTraces, withTracingWorker } from './helpers';
import * as workflows from './workflows/langsmith';

process.env.LANGSMITH_TRACING = 'true';

const ALL_ACTIVITIES = {
  simpleActivity: activities.simpleActivity,
};

/** Basic single-activity workflow with addTemporalRuns on, started with no ambient — two roots. */
const SIMPLE_TREE = [
  'StartWorkflow:SimpleWorkflow',
  'RunWorkflow:SimpleWorkflow',
  '  StartActivity:simpleActivity',
  '  RunActivity:simpleActivity',
].join('\n');

/** Root workflow-body `traceable`, no propagated parent — just the user run, no synthetic root. */
const WORKFLOW_BODY_ROOT_TREE = ['workflow_inner_call'].join('\n');

test('emits the basic SimpleWorkflow tree with no ambient (two roots)', async (t) => {
  const collector = new InMemoryRunCollector();
  await withTracingWorker({
    collector,
    options: { addTemporalRuns: true },
    activities: ALL_ACTIVITIES,
    body: async ({ client, taskQueue }) => {
      await client.workflow.execute(workflows.SimpleWorkflow, {
        taskQueue,
        workflowId: `simple-${Date.now()}`,
        args: ['hi'],
      });
    },
  });
  t.deepEqual(dumpTraces(collector.records), SIMPLE_TREE);
});

/**
 * A workflow-body `traceable` with addTemporalRuns off and NO client-side
 * `traceable` wrapper, so no parent is propagated in. Without the synthetic
 * anchor root, LangSmith takes its no-parent branch and mints a uuid via
 * `crypto`, which the workflow isolate lacks — crashing the Workflow Task. The
 * synthetic root keeps it on the `createChild` branch and stays invisible, so
 * only the user's `workflow_inner_call` run is emitted.
 */
test('root workflow-body traceable (no propagated parent) does not crash and emits just the user run', async (t) => {
  const collector = new InMemoryRunCollector();
  await withTracingWorker({
    collector,
    options: { addTemporalRuns: false },
    activities: ALL_ACTIVITIES,
    body: async ({ client, taskQueue }) => {
      await client.workflow.execute(workflows.WorkflowBodyTraceableWorkflow, {
        taskQueue,
        workflowId: `wf-body-root-${Date.now()}`,
        args: ['hello'],
      });
    },
  });
  t.deepEqual(dumpTraces(collector.records), WORKFLOW_BODY_ROOT_TREE);
});

test('plugin options are carried onto emitted runs: applies projectName, defaultTags, and (scrubbed) defaultMetadata', async (t) => {
  const collector = new InMemoryRunCollector();
  await withTracingWorker({
    collector,
    options: {
      addTemporalRuns: true,
      projectName: 'my-project',
      defaultTags: ['env:test'],
      // The api_key entry must be scrubbed before it reaches the backend.
      defaultMetadata: { team: 'platform', api_key: 'should-be-removed' },
    },
    activities: ALL_ACTIVITIES,
    body: async ({ client, taskQueue }) => {
      await client.workflow.execute(workflows.SimpleWorkflow, {
        taskQueue,
        workflowId: `options-${Date.now()}`,
        args: ['hi'],
      });
    },
  });

  const runWorkflow = collector.byName('RunWorkflow:SimpleWorkflow');
  t.is(runWorkflow?.project_name, 'my-project');
  t.deepEqual(runWorkflow?.tags, ['env:test']);
  const metadata = runWorkflow?.extra?.metadata as Record<string, unknown> | undefined;
  t.deepEqual(metadata, { team: 'platform' });
  t.false('api_key' in (metadata ?? {}));
});
