/**
 * Handler registry.
 *
 * A "handler" is the user-supplied async function that actually does the
 * work for a given jobName. The worker doesn't know what jobs mean —
 * it just looks up the function by jobName and calls it with the payload.
 *
 * This indirection is what lets the same worker binary process every
 * kind of job your app has, and what makes the Producer SDK type-agnostic.
 */
export type JobHandler<TPayload = unknown, TResult = unknown> = (
  payload: TPayload,
  ctx: HandlerContext,
) => Promise<TResult>;

export interface HandlerContext {
  jobId: string;
  jobName: string;
  attempt: number; // 1-indexed; meaningful starting Phase 2
  workerId: string;
}

export class HandlerRegistry {
  private readonly handlers = new Map<string, JobHandler>();

  register<TPayload, TResult>(
    jobName: string,
    handler: JobHandler<TPayload, TResult>,
  ): this {
    if (this.handlers.has(jobName)) {
      throw new Error(`handler already registered for jobName="${jobName}"`);
    }
    this.handlers.set(jobName, handler as JobHandler);
    return this;
  }

  get(jobName: string): JobHandler | undefined {
    return this.handlers.get(jobName);
  }

  list(): string[] {
    return [...this.handlers.keys()];
  }
}
