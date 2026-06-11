/**
 * Activity-side and Nexus-handler-side LangSmith interceptors. Both run in the
 * real Node worker process and install the reconstructed run via `withRunTree`
 * so a user's unchanged body `traceable` calls nest under it.
 *
 * @module
 */

import { RunTree } from 'langsmith/run_trees';
import { withRunTree } from 'langsmith/traceable';
import type { Payload } from '@temporalio/common';
import type { Context as ActivityContext } from '@temporalio/activity';

import {
  HEADER_KEY,
  decodeContextString,
  isTracingEnabled,
  readContextHeader,
  type LangSmithTraceContext,
} from './propagation';
import {
  RUN_TYPE,
  asOutputs,
  describeError,
  buildRunTree,
  runActivityRunName,
  runCancelNexusHandlerRunName,
  runStartNexusHandlerRunName,
  runTreeFromContext,
} from './run-tree';
import type { EmitterConfig } from './sinks';

type Headers = Record<string, Payload>;

interface ActivityExecuteInput {
  readonly args: unknown[];
  readonly headers: Headers;
}
type ActivityNext = (input: ActivityExecuteInput) => Promise<unknown>;

interface ActivityInboundInterceptor {
  execute?(input: ActivityExecuteInput, next: ActivityNext): Promise<unknown>;
}

/**
 * Reconstruct the propagated parent run from a LangSmith trace context, wired to
 * the plugin-configured client so descendant runs emit to the right place. The
 * returned run is the *parent* — never posted itself — used only to parent
 * the operation run (or directly as ambient when `addTemporalRuns` is off).
 */
function reconstructParentRun(config: EmitterConfig, ctx: LangSmithTraceContext | undefined): RunTree | undefined {
  const parsed = runTreeFromContext(ctx);
  if (!parsed) {
    return undefined;
  }
  return new RunTree({
    name: parsed.name || 'parent',
    run_type: parsed.run_type || RUN_TYPE.CHAIN,
    id: parsed.id,
    trace_id: parsed.trace_id,
    dotted_order: parsed.dotted_order,
    parent_run_id: parsed.parent_run_id,
    project_name: config.projectName ?? parsed.project_name,
    client: config.client,
    // Force-enable so nested body `traceable` runs emit; tracing gate is `isTracingEnabled()`.
    tracingEnabled: true,
  });
}

/**
 * Run `fn` with `op` installed as the active LangSmith run, emitting the run
 * around the call. Shared by activity and Nexus handlers.
 */
async function traceOperation(op: RunTree, fn: () => Promise<unknown>): Promise<unknown> {
  await op.postRun();
  try {
    const result = await withRunTree(op, fn);
    await op.end(asOutputs(result));
    await op.patchRun();
    return result;
  } catch (err) {
    await op.end(undefined, describeError(err));
    await op.patchRun();
    throw err;
  }
}

/**
 * Build the activity-inbound interceptor factory. The worker calls the factory
 * once per activity with that activity's {@link ActivityContext}, from which we
 * read the activity type for the run name.
 */
export function createActivityInboundInterceptor(
  config: EmitterConfig
): (ctx: ActivityContext) => ActivityInboundInterceptor {
  return (ctx: ActivityContext) => ({
    async execute(input: ActivityExecuteInput, next: ActivityNext): Promise<unknown> {
      if (!isTracingEnabled()) {
        return next(input);
      }
      const parent = reconstructParentRun(config, readContextHeader(input.headers));
      if (!config.addTemporalRuns) {
        // Propagation only: nest the user's body `traceable` runs under the
        // reconstructed parent without emitting a Temporal-operation run.
        return parent ? withRunTree(parent, () => next(input)) : next(input);
      }
      const activityType = ctx.info.activityType;
      const run = buildRunTree(config, {
        name: runActivityRunName(activityType),
        runType: RUN_TYPE.TOOL,
        parent,
        inputs: { args: input.args },
      });
      return traceOperation(run, () => next(input));
    },
  });
}

/**
 * The Nexus operation context the SDK passes to a handler interceptor. Both
 * `startOperation` and `cancelOperation` receive `{ ctx, ... }`, where `ctx`
 * carries the service / operation names and the plain-string headers.
 */
interface NexusOperationContext {
  readonly service: string;
  readonly operation: string;
  readonly headers?: Record<string, string>;
}
interface NexusOperationInput {
  readonly ctx: NexusOperationContext;
}
type NexusNext = (input: NexusOperationInput) => Promise<unknown>;

interface NexusInboundInterceptor {
  startOperation?(input: NexusOperationInput, next: NexusNext): Promise<unknown>;
  cancelOperation?(input: NexusOperationInput, next: NexusNext): Promise<unknown>;
}

function nexusContext(input: NexusOperationInput): LangSmithTraceContext | undefined {
  return decodeContextString(input.ctx.headers?.[HEADER_KEY]);
}

/**
 * Build the Nexus-handler interceptor. `startOperation` opens a
 * `RunStartNexusOperationHandler:` run (so workflows the handler starts nest
 * under it); `cancelOperation` opens `RunCancelNexusOperationHandler:`.
 */
export function createNexusInboundInterceptor(config: EmitterConfig): NexusInboundInterceptor {
  const handle =
    (nameOf: (s: string, o: string) => string) =>
    async (input: NexusOperationInput, next: NexusNext): Promise<unknown> => {
      if (!isTracingEnabled()) {
        return next(input);
      }
      const parent = reconstructParentRun(config, nexusContext(input));
      if (!config.addTemporalRuns) {
        return parent ? withRunTree(parent, () => next(input)) : next(input);
      }
      const run = buildRunTree(config, {
        name: nameOf(input.ctx.service, input.ctx.operation),
        runType: RUN_TYPE.CHAIN,
        parent,
      });
      return traceOperation(run, () => next(input));
    };

  return {
    startOperation: handle(runStartNexusHandlerRunName),
    cancelOperation: handle(runCancelNexusHandlerRunName),
  };
}
