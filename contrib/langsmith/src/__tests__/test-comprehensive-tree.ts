/**
 * The comprehensive line-for-line trace-tree end-to-end test.
 *
 * Boots a real local Temporal server with the plugin on the client + worker,
 * drives one continue-as-new `ComprehensiveWorkflow` that touches every
 * instrumented Temporal boundary — activity, local activity, child workflow,
 * signal-to-child, live Nexus, continue-as-new, and the signal / query / update +
 * validator inbound handlers — each both raw and wrapped in a user `traceable`,
 * and asserts the EXACT emitted run hierarchy with `deepEqual` on {@link dumpTraces}.
 *
 * A single live tree catches whole classes of bug a per-primitive subtree test
 * misses: an interceptor that reads the wrong input field renders a run as
 * `Something:undefined`; a missing, extra, or misparented run changes the array.
 * (Replay-safety and exactly-once are covered by the replay/side-effect tests.)
 *
 * @module
 */

import test from 'ava';
import { traceable } from 'langsmith/traceable';

import * as activities from './activities/comprehensive';
import { InMemoryRunCollector, dumpTraces, withTracingWorker } from './helpers';
import { comprehensiveNexusServiceHandler } from './stubs/nexus';
import {
  COMPREHENSIVE_NEXUS_ENDPOINT,
  ComprehensiveWorkflow,
  completeSignal,
  comprehensiveQuery,
  comprehensiveSignal,
  comprehensiveUpdate,
} from './workflows/comprehensive';

process.env.LANGSMITH_TRACING = 'true';
// Keep langsmith callbacks synchronous so a stray run that escapes to the real
// default client fails fast instead of blocking teardown on a background batch.
process.env.LANGCHAIN_CALLBACKS_BACKGROUND = 'false';

const COMPREHENSIVE_ACTIVITIES = {
  comprehensiveActivity: activities.comprehensiveActivity,
  comprehensiveLocalActivity: activities.comprehensiveLocalActivity,
  notifyReady: activities.notifyReady,
};

const WORKFLOWS_PATH = require.resolve('./workflows/comprehensive');

/**
 * Drive one full scenario and return the collected runs. Everything runs inside a
 * client-side `user_pipeline` traceable so it threads under one user root. The
 * workflow runs its outbound boundaries first and then blocks; only once it
 * signals readiness does the driver issue the inbound handler calls (query /
 * signal / update) in a FIXED awaited sequence, so the emitted tree is deterministic.
 */
async function runComprehensive(addTemporalRuns: boolean): Promise<InMemoryRunCollector> {
  const collector = new InMemoryRunCollector();
  await withTracingWorker({
    collector,
    options: { addTemporalRuns },
    activities: COMPREHENSIVE_ACTIVITIES,
    workerOptions: {
      workflowsPath: WORKFLOWS_PATH,
      nexusServices: [comprehensiveNexusServiceHandler],
    },
    body: async ({ client, taskQueue, env }) => {
      await env.createNexusEndpoint(COMPREHENSIVE_NEXUS_ENDPOINT, taskQueue);

      const ready = activities.resetReady();
      const pipeline = traceable(
        async () => {
          const handle = await client.workflow.start(ComprehensiveWorkflow, {
            taskQueue,
            workflowId: `comprehensive-${addTemporalRuns}-${Date.now()}`,
            args: [0],
          });

          // Wait until the workflow has run its outbound boundaries and is blocked
          // waiting for handler calls, so the tree order is deterministic.
          await ready;

          const wrap = { client: collector.asClient(), tracingEnabled: true };

          await handle.query(comprehensiveQuery, 'q1');
          await traceable(async () => handle.query(comprehensiveQuery, 'q2'), { name: 'user_query_wrap', ...wrap })();

          await handle.signal(comprehensiveSignal, 's1');
          await traceable(async () => handle.signal(comprehensiveSignal, 's2'), { name: 'user_signal_wrap', ...wrap })();

          await handle.executeUpdate(comprehensiveUpdate, { args: ['u1'] });
          await traceable(async () => handle.executeUpdate(comprehensiveUpdate, { args: ['u2'] }), {
            name: 'user_update_wrap',
            ...wrap,
          })();

          await handle.signal(completeSignal);
          await handle.result();
        },
        { name: 'user_pipeline', client: collector.asClient(), tracingEnabled: true }
      );
      await pipeline();
    },
  });
  return collector;
}

/** addTemporalRuns: true — Temporal-operation runs interleave with the user `traceable` runs. */
const EXPECTED_TRUE: string[] = [
  'user_pipeline',
  '  StartWorkflow:ComprehensiveWorkflow',
  '  RunWorkflow:ComprehensiveWorkflow',
  '    StartActivity:comprehensiveActivity',
  '    RunActivity:comprehensiveActivity',
  '      comprehensive_activity',
  '        comprehensive_activity_inner',
  '    user_wrap_activity',
  '      StartActivity:comprehensiveActivity',
  '      RunActivity:comprehensiveActivity',
  '        comprehensive_activity',
  '          comprehensive_activity_inner',
  '    StartActivity:comprehensiveLocalActivity',
  '    RunActivity:comprehensiveLocalActivity',
  '    StartChildWorkflow:ComprehensiveChildWorkflow',
  '    RunWorkflow:ComprehensiveChildWorkflow',
  '      StartActivity:comprehensiveActivity',
  '      RunActivity:comprehensiveActivity',
  '        comprehensive_activity',
  '          comprehensive_activity_inner',
  '    user_wrap_child',
  '      StartChildWorkflow:ComprehensiveChildWorkflow',
  '      RunWorkflow:ComprehensiveChildWorkflow',
  '        StartActivity:comprehensiveActivity',
  '        RunActivity:comprehensiveActivity',
  '          comprehensive_activity',
  '            comprehensive_activity_inner',
  '    StartChildWorkflow:ComprehensiveReceiverWorkflow',
  '    SignalChildWorkflow:signal',
  '      HandleSignal:signal',
  '    RunWorkflow:ComprehensiveReceiverWorkflow',
  '    StartNexusOperation:comprehensiveNexusService/greet',
  '    RunStartNexusOperationHandler:comprehensiveNexusService/greet',
  '      nexus_inner_call',
  '    workflow_inner_call',
  '    StartActivity:notifyReady',
  '    RunActivity:notifyReady',
  '    RunWorkflow:ComprehensiveWorkflow',
  '  QueryWorkflow:query',
  '    HandleQuery:query',
  '      query_inner_call',
  '  user_query_wrap',
  '    QueryWorkflow:query',
  '      HandleQuery:query',
  '        query_inner_call',
  '  SignalWorkflow:signal',
  '    HandleSignal:signal',
  '      signal_inner_call',
  '  user_signal_wrap',
  '    SignalWorkflow:signal',
  '      HandleSignal:signal',
  '        signal_inner_call',
  '  StartWorkflowUpdate:update',
  '    ValidateUpdate:update',
  '      validator_inner_call',
  '    HandleUpdate:update',
  '      update_inner_call',
  '  user_update_wrap',
  '    StartWorkflowUpdate:update',
  '      ValidateUpdate:update',
  '        validator_inner_call',
  '      HandleUpdate:update',
  '        update_inner_call',
  '  SignalWorkflow:complete',
  '    HandleSignal:complete',
];

/** addTemporalRuns: false — only the user `traceable` runs, still parented across every boundary. */
const EXPECTED_FALSE: string[] = [
  'user_pipeline',
  '  comprehensive_activity',
  '    comprehensive_activity_inner',
  '  user_wrap_activity',
  '    comprehensive_activity',
  '      comprehensive_activity_inner',
  '  comprehensive_activity',
  '    comprehensive_activity_inner',
  '  user_wrap_child',
  '    comprehensive_activity',
  '      comprehensive_activity_inner',
  '  nexus_inner_call',
  '  workflow_inner_call',
  '  query_inner_call',
  '  user_query_wrap',
  '    query_inner_call',
  '  user_signal_wrap',
  '    signal_inner_call',
  '  signal_inner_call',
  '  validator_inner_call',
  '  update_inner_call',
  '  user_update_wrap',
  '    validator_inner_call',
  '    update_inner_call',
];

test.serial('comprehensive trace tree: addTemporalRuns=true', async (t) => {
  const collector = await runComprehensive(true);
  t.deepEqual(dumpTraces(collector.records).split('\n'), EXPECTED_TRUE);
});

test.serial('comprehensive trace tree: addTemporalRuns=false', async (t) => {
  const collector = await runComprehensive(false);
  t.deepEqual(dumpTraces(collector.records).split('\n'), EXPECTED_FALSE);
});
