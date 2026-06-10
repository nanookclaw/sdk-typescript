/**
 * Read-only handlers must not perturb the Workflow's main PRNG.
 *
 * `handleQuery` and `validateUpdate` run only on the live cached instance and are
 * never replayed. If their LangSmith run-id minting drew from the main PRNG, a
 * post-handler `uuid4()` on the cached instance would diverge from a fresh replay
 * (where the handler never ran). The fixture embeds that draw verbatim in a child
 * workflow's `workflowId`, so any perturbation changes the `workflowId` on the
 * `StartChildWorkflowExecution` command (carried losslessly, no modulo) and the
 * divergence surfaces as a determinism violation when the recorded history is
 * replayed.
 *
 * The treatment run (issue a query AND a validator-rejected update before the
 * release) replays cleanly only when run-id minting is isolated from the main
 * PRNG; the control run (no read-only handlers) replays cleanly regardless.
 *
 * @module
 */

import test, { type ExecutionContext } from 'ava';

import { Worker } from '@temporalio/worker';

import { LangSmithPlugin } from '../index';
import { InMemoryRunCollector, WORKFLOWS_PATH, withTracingWorker } from './helpers';
import * as workflows from './workflows/langsmith';

process.env.LANGSMITH_TRACING = 'true';

async function runAndReplay(t: ExecutionContext, issueReadonlyHandlers: boolean): Promise<void> {
  const collector = new InMemoryRunCollector();
  const options = { addTemporalRuns: true };

  const history = await withTracingWorker({
    collector,
    options,
    activities: {},
    // A cached instance is what surfaces the perturbation: a live cache
    // (any value > 0) retains the read-only handler's effect, whereas
    // maxCachedWorkflows: 0 would evict and replay every task from scratch,
    // discarding it and masking the bug.
    workerOptions: { maxCachedWorkflows: 2 },
    body: async ({ client, taskQueue }) => {
      const handle = await client.workflow.start(workflows.ReadonlyDeterminismWorkflow, {
        taskQueue,
        workflowId: `readonly-determinism-${issueReadonlyHandlers ? 'treatment' : 'control'}-${Date.now()}`,
      });
      if (issueReadonlyHandlers) {
        await handle.query(workflows.readonlyQuery);
        await t.throwsAsync(handle.executeUpdate(workflows.readonlyUpdate, { args: ['x'] }));
      }
      await handle.signal(workflows.releaseSignal);
      await handle.result();
      return handle.fetchHistory();
    },
  });

  // Replay the recorded history with a fresh plugin-enabled replay worker. The
  // read-only handler invocations are not in history, so a clean replay never
  // runs them; if their ids came off the main PRNG, the replayed command stream
  // diverges from history and runReplayHistory throws.
  const plugin = new LangSmithPlugin({ ...options, client: new InMemoryRunCollector().asClient() });
  await Worker.runReplayHistory({ workflowsPath: WORKFLOWS_PATH, plugins: [plugin] }, history);
}

test('read-only handlers do not perturb the main random sequence: control replays cleanly', async (t) => {
  await runAndReplay(t, false);
  t.pass();
});

test('read-only handlers do not perturb the main random sequence: treatment replays cleanly', async (t) => {
  await runAndReplay(t, true);
  t.pass();
});
