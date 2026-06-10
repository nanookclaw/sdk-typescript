/**
 * Client-side LangSmith interceptor. Execution ops (`start`) emit a peer marker
 * and propagate the **ambient** context (the remote run is a sibling); messaging
 * ops (`signal`, `query`, `update`, …) emit the marker and propagate the
 * **marker** context (the remote handler nests under it).
 *
 * @module
 */

import { RunTree } from 'langsmith/run_trees';
import { getCurrentRunTree } from 'langsmith/traceable';
import type { Payload } from '@temporalio/common';

import { isTracingEnabled, withContextHeader } from './propagation';
import {
  RUN_TYPE,
  buildRunTree,
  emitMarkerRun,
  queryWorkflowRunName,
  runHeaders,
  signalWithStartRunName,
  signalWorkflowRunName,
  startUpdateWithStartRunName,
  startWorkflowRunName,
  startWorkflowUpdateRunName,
} from './run-tree';
import type { EmitterConfig } from './sinks';

type Headers = Record<string, Payload>;
type NextFn<I, O> = (input: I) => Promise<O>;

/** Local structural views of the client interceptor inputs (only fields we read). */
interface WithHeaders {
  readonly headers: Headers;
  readonly args?: unknown[];
}
interface StartInput extends WithHeaders {
  readonly workflowType: string;
}
interface SignalInput extends WithHeaders {
  readonly signalName: string;
}
interface SignalWithStartInput extends WithHeaders {
  readonly workflowType: string;
  readonly signalName: string;
}
interface QueryInput extends WithHeaders {
  readonly queryType: string;
}
interface UpdateInput extends WithHeaders {
  readonly updateName?: string;
  readonly name?: string;
}

function updateName(input: UpdateInput): string {
  return input.updateName ?? input.name ?? 'update';
}

/**
 * Build the client-side LangSmith interceptor. Returned shape is structurally
 * the SDK's `WorkflowClientInterceptor`; methods the SDK does not recognize are
 * simply never invoked.
 */
export function createClientInterceptor(config: EmitterConfig): Record<string, unknown> {
  const peerStart = async <O>(input: StartInput, next: NextFn<StartInput, O>, name: string): Promise<O> => {
    if (!isTracingEnabled()) {
      return next(input);
    }
    const ambient = getCurrentRunTree(true);
    if (config.addTemporalRuns) {
      await emitMarkerRun(
        buildRunTree(config, { name, runType: RUN_TYPE.CHAIN, parent: ambient, inputs: { args: input.args ?? [] } })
      );
    }
    const headers = withContextHeader(input.headers, runHeaders(ambient));
    return next({ ...input, headers });
  };

  const parentMessage = async <I extends WithHeaders, O>(input: I, next: NextFn<I, O>, name: string): Promise<O> => {
    if (!isTracingEnabled()) {
      return next(input);
    }
    const ambient = getCurrentRunTree(true);
    let propagate: RunTree | undefined = ambient;
    if (config.addTemporalRuns) {
      const marker = buildRunTree(config, {
        name,
        runType: RUN_TYPE.CHAIN,
        parent: ambient,
        inputs: { args: input.args ?? [] },
      });
      await emitMarkerRun(marker);
      propagate = marker;
    }
    const headers = withContextHeader(input.headers, runHeaders(propagate));
    return next({ ...input, headers });
  };

  return {
    start(input: StartInput, next: NextFn<StartInput, string>): Promise<string> {
      return peerStart(input, next, startWorkflowRunName(input.workflowType));
    },
    // Output typed loosely because the descriptor shape varies across SDK versions.
    startWithDetails<O>(input: StartInput, next: NextFn<StartInput, O>): Promise<O> {
      return peerStart(input, next, startWorkflowRunName(input.workflowType));
    },
    signal(input: SignalInput, next: NextFn<SignalInput, void>): Promise<void> {
      return parentMessage(input, next, signalWorkflowRunName(input.signalName));
    },
    signalWithStart(input: SignalWithStartInput, next: NextFn<SignalWithStartInput, string>): Promise<string> {
      return parentMessage(input, next, signalWithStartRunName(input.workflowType));
    },
    query(input: QueryInput, next: NextFn<QueryInput, unknown>): Promise<unknown> {
      return parentMessage(input, next, queryWorkflowRunName(input.queryType));
    },
    startUpdate(input: UpdateInput, next: NextFn<UpdateInput, unknown>): Promise<unknown> {
      return parentMessage(input, next, startWorkflowUpdateRunName(updateName(input)));
    },
    startUpdateWithStart(input: UpdateInput, next: NextFn<UpdateInput, unknown>): Promise<unknown> {
      return parentMessage(input, next, startUpdateWithStartRunName(updateName(input)));
    },
    terminate<I, O>(input: I, next: NextFn<I, O>): Promise<O> {
      return next(input);
    },
    cancel<I, O>(input: I, next: NextFn<I, O>): Promise<O> {
      return next(input);
    },
    describe<I, O>(input: I, next: NextFn<I, O>): Promise<O> {
      return next(input);
    },
  };
}
