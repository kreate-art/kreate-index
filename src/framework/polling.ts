import fastq from "fastq";

import { TimeDifference, UnixTime } from "@teiki/protocol/types";
import { assert } from "@teiki/protocol/utils";

import { Connections } from "../connections";
import { MaybePromise, NonEmpty } from "../types/typelevel";

import { ErrorHandler, reduceErrorHandler } from "./base";

export type VitalConnections = Connections<"sql" | "notifications">;

type PollingTrigger =
  | "kick-off"
  | "continue"
  | { scheduled: TimeDifference }
  | { channel: string };

// TODO: Find a way to expose Task here, so we don't have to pass the TaskBoard to initialize
export type PollingThis<Context, Connections extends VitalConnections> = {
  context: Context;
  connections: Connections;
};

type ITask<Id> = { id: Id };

export type PollingTaskBoard<Task extends ITask<Id>, Id> = {
  queue: fastq.queueAsPromised<Task, void>;
  inQueue: Map<Id, boolean>; // Map<Id, IsRunning>
};

type PollingIndexParams<
  Context,
  Connections extends VitalConnections,
  Task extends ITask<Id>,
  Id
> = {
  name: string;
  connections: Connections;
  initialize?: (
    this: PollingThis<Context, Connections>,
    board?: PollingTaskBoard<Task, Id>
  ) => MaybePromise<void>;
  fetch: (
    this: PollingThis<Context, Connections>
  ) => MaybePromise<{ tasks: Task[]; continue: boolean } | null>;
  handle?: (
    this: PollingThis<Context, Connections>,
    task: Task
  ) => MaybePromise<void>;
  batch?: (
    this: PollingThis<Context, Connections>,
    tasks: NonEmpty<Task[]>
  ) => MaybePromise<void>;
  triggers: {
    channels?: string[];
    interval?: TimeDifference;
  };
  concurrency?: {
    tasks?: number;
    workers?: number;
  };
  onError?: ErrorHandler;
};

export type PollingIndexer<Context> = {
  start: (context: Context, immediate?: boolean) => Promise<void>;
  stop: () => Promise<void>;
};

export function createPollingIndexer<
  Context,
  Connections extends VitalConnections,
  Task extends ITask<Id>,
  Id
>({
  name,
  connections,
  initialize,
  fetch,
  handle,
  batch,
  triggers: {
    channels = [],
    interval = 600_000, // Default to 10 minutes, since we have channels
  },
  concurrency,
  onError,
}: PollingIndexParams<
  Context,
  Connections,
  Task,
  Id
>): PollingIndexer<Context> {
  let finalize: (() => Promise<void>) | null = null;

  // To prevent things from exploding, we disallow unbounded queue.
  // Therefore, undefined or 0 means the polling indexer will wait
  // until the queue is drained each cycle.
  const tasksConcurrency = concurrency?.tasks ?? 0;
  const workerConcurrency = concurrency?.workers ?? 8;

  async function start(context: Context) {
    assert(!finalize, `[${name}] Already started.`);

    let lastExecution: UnixTime = 0;
    let scheduled: NodeJS.Timeout | null = null;

    const finish = (error: Error) => {
      assert(finalize, `[${name}] Finalize must be set.`);
      finalize();
      throw error;
    };

    const errorCallback = reduceErrorHandler(onError, finish);
    const pollingQueue = fastq.promise(doPoll, 1);
    pollingQueue.error(errorCallback);

    const self = { context, connections };

    const inQueue: PollingTaskBoard<Task, Id>["inQueue"] = new Map();
    let taskQueue: PollingTaskBoard<Task, Id>["queue"] | undefined;
    if (handle) {
      const doHandle = async (task: Task) => {
        const id = task.id;
        const running = inQueue.get(id);
        assert(running != null, `[${name}] Unknown task: ${id}`);
        assert(!running, `[${name}] Task is already running: ${id}`);
        inQueue.set(id, true);
        try {
          await handle.call(self, task);
        } finally {
          inQueue.delete(id);
        }
      };
      taskQueue = fastq.promise(doHandle, workerConcurrency);
      taskQueue.error(errorCallback);
    } else {
      taskQueue = undefined;
    }

    console.log(`[${name}] Starting...`);

    if (initialize) {
      const board = taskQueue ? { queue: taskQueue, inQueue } : undefined;
      await initialize?.call(self, board);
    }

    const notifications = connections.notifications;
    const unlistens = await Promise.all(
      channels.map(async (channel) =>
        notifications.listen(
          channel,
          () => {
            pollingQueue.kill();
            pollingQueue.push({ channel });
          },
          () => console.log(`[${name}] Listening on channel: ${channel}`)
        )
      )
    );

    console.log(`[${name}] Started!`);

    finalize = async () => {
      console.log(`[${name}] Stopping...`);
      pollingQueue.killAndDrain();
      taskQueue?.killAndDrain();
      await Promise.all(unlistens.map((un) => un()));
      finalize = null;
      console.log(`[${name}] Stopped...`);
    };

    async function doPoll(trigger: PollingTrigger) {
      if (finalize == null) return;

      if (scheduled) {
        clearTimeout(scheduled);
        scheduled = null;
      }

      const now = new Date();
      lastExecution = +now;

      const inQueueCount = inQueue.size;
      let willContinue = false;

      if (tasksConcurrency && inQueueCount >= tasksConcurrency)
        console.warn(
          `[${name}] Queue is overloaded (${inQueueCount}). Try again later.`
        );
      else {
        console.log(`[${name}] Fetching at ${now.toISOString()} ~`, trigger);
        const fetched = await fetch.call(self);
        if (!fetched) console.log(`[${name}] Fetcher skips. Move on.`);
        else {
          let tasks: Task[];
          if (tasksConcurrency) {
            tasks = [];
            for (const task of fetched.tasks)
              if (!inQueue.has(task.id)) tasks.push(task);
          } else {
            tasks = fetched.tasks;
          }
          willContinue = fetched.continue;
          const fetchedCount = fetched.tasks.length;
          const taskCount = tasks.length;
          console.log(
            `[${name}] Fetched ${fetchedCount} tasks.` +
              (taskCount !== fetchedCount
                ? ` ${fetchedCount - taskCount} of them are already in queue.`
                : "")
          );
          if (taskCount) {
            if (taskQueue) {
              for (const task of tasks) {
                inQueue.set(task.id, false);
                taskQueue.push(task);
              }
              if (!tasksConcurrency) await taskQueue.drained();
            }
            batch && (await batch.call(self, tasks));
            if (tasksConcurrency)
              console.log(`[${name}] Batched ${taskCount} tasks.`);
            else console.log(`[${name}] Finished ${taskCount} tasks.`);
          }
        }
      }

      if (pollingQueue.length()) return;
      if (willContinue) pollingQueue.push("continue");
      else {
        // Introduce some jitters
        const jittered = Math.trunc((interval / 2) * (1 + Math.random()));
        const delay = lastExecution + jittered - Date.now();
        if (delay < 100) pollingQueue.push({ scheduled: 0 });
        else {
          console.log(
            `[${name}] Schedule one in ${(delay / 1000).toFixed(1)}s`
          );
          scheduled = setTimeout(
            // If executing, skipped.
            (time) =>
              pollingQueue.idle() && pollingQueue.push({ scheduled: time }),
            delay,
            delay
          );
        }
      }
    }

    pollingQueue.push("kick-off");
  }

  async function stop() {
    assert(finalize, `[${name}] Already stopped.`);
    finalize();
  }

  return { start, stop };
}
