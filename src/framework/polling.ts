import fastq from "fastq";

import { TimeDifference, UnixTime } from "@kreate/protocol/types";

import { Connections } from "../connections";
import { MaybePromise, NonEmpty, WithId } from "../types/typelevel";
import { noop$async } from "../utils";

import { ErrorHandler, reduceErrorHandler } from "./base";

export type VitalConnections = Connections<"sql" | "notifications">;

type PollingTrigger =
  | "kick-off"
  | "continue"
  | "retry"
  | { scheduled: TimeDifference }
  | { channel: string; payload: string };

type PollingThis<Context, Connections extends VitalConnections> = {
  context: Context;
  connections: Connections;
  retry: () => void; // TODO: Proper retry
  board: PollingTaskBoard;
};

type TaskId = string;

export type PollingTaskBoard = {
  queue: fastq.queueAsPromised<WithId<unknown, TaskId>, void>;
  inQueue: Map<TaskId, boolean>; // Map<TaskId, IsRunning>
};

// TODO: Make non-blocking polling indexer the default
// TODO: Only allow `handle` OR `batch`

type PollingIndexParams<Context, Connections extends VitalConnections, Task> = {
  name: string;
  connections: Connections;
  $id: (task: Task) => TaskId;
  initialize?: (this: PollingThis<Context, Connections>) => MaybePromise<void>;
  finalize?: (this: PollingThis<Context, Connections>) => MaybePromise<void>;
  fetch: (
    this: PollingThis<Context, Connections>,
    trigger: PollingTrigger
  ) => MaybePromise<{ tasks: Task[]; continue: boolean } | null>;
  handle?: (
    this: PollingThis<Context, Connections>,
    task: WithId<Task, TaskId>
  ) => MaybePromise<void>;
  batch?: (
    this: PollingThis<Context, Connections>,
    tasks: NonEmpty<WithId<Task, TaskId>[]>
  ) => MaybePromise<void>;
  triggers: {
    channels?: (
      | string
      | { channel: string; filter: (payload: string) => boolean }
    )[];
    interval?: TimeDifference;
    retry?: TimeDifference;
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
  Task
>({
  name,
  connections,
  $id,
  initialize,
  finalize,
  fetch,
  handle,
  batch,
  triggers: {
    channels = [],
    interval = 600_000, // Default to 10 minutes, since we have channels
    retry: retryDelay = 60_000, // Default to 1 minute
  },
  concurrency,
  onError,
}: PollingIndexParams<Context, Connections, Task>): PollingIndexer<Context> {
  let shutdown: (() => Promise<void>) | null = null;

  // To prevent things from exploding, we disallow unbounded queue.
  // Therefore, undefined or 0 means the polling indexer will wait
  // until the queue is drained each cycle.
  const tasksConcurrency = concurrency?.tasks ?? 0;
  const workerConcurrency = concurrency?.workers ?? 8;

  async function start(context: Context) {
    if (shutdown) {
      console.warn(`[${name}] Already started.`);
      return;
    }

    let lastExecution: UnixTime = 0;
    let scheduled: NodeJS.Timeout | null = null;

    let willContinue = false;
    let willRetry = false;

    function clearScheduled() {
      if (scheduled) {
        clearTimeout(scheduled);
        scheduled = null;
      }
    }

    const errorCallback = reduceErrorHandler(onError, async (error) => {
      shutdown && (await shutdown());
      throw error;
    });
    const pollingQueue = fastq.promise(doPoll, 1);
    pollingQueue.error(errorCallback);

    const inQueue = new Map<TaskId, boolean>();
    let taskQueue: fastq.queueAsPromised<WithId<Task, TaskId>, void>;
    if (handle) {
      const doHandle = async (task: WithId<Task, TaskId>) => {
        const id = task.id;
        const running = inQueue.get(id);
        if (running == null) {
          console.error(`[${name}] Unknown task: ${id}`);
        } else if (running) {
          console.warn(`[${name}] Task is already running: ${id}`);
        } else {
          inQueue.set(id, true);
          try {
            await handle.call(self, task);
          } finally {
            inQueue.delete(id);
          }
        }
      };
      taskQueue = fastq.promise(doHandle, workerConcurrency);
      taskQueue.error(errorCallback);
    } else {
      // It's just easier to handle things...
      taskQueue = fastq.promise(noop$async, 1);
    }

    if (tasksConcurrency)
      taskQueue.drain = () => {
        if (willContinue) pollingQueue.idle() && poll("continue");
        else if (willRetry)
          scheduled = setTimeout(
            () => pollingQueue.idle() && poll("retry"),
            retryDelay
          );
      };

    console.log(`[${name}] Starting...`);

    const self = {
      context,
      connections,
      board: { queue: taskQueue, inQueue },
      retry: () => (willRetry = true),
    };
    if (initialize) await initialize?.call(self);

    const notifications = connections.notifications;
    const unlistens = await Promise.all(
      channels.map(async (argument) => {
        if (typeof argument === "string") {
          const channel = argument;
          return notifications.listen(
            channel,
            (payload) => poll({ channel, payload }),
            () => console.log(`[${name}] Listening on channel: ${channel}`)
          );
        } else {
          const { channel, filter } = argument;
          return notifications.listen(
            channel,
            (payload) => {
              if (filter(payload)) poll({ channel, payload });
              else
                console.log(
                  `[${name}] Notification ignored: ${channel} | ${payload}`
                );
            },
            () =>
              console.log(
                `[${name}] Listening with custom filter on channel: ${channel}`
              )
          );
        }
      })
    );

    console.log(`[${name}] Started!`);

    shutdown = async () => {
      console.log(`[${name}] Stopping...`);
      clearScheduled();
      pollingQueue.killAndDrain();
      taskQueue.kill(); // We don't want to trigger the continuation here
      await Promise.all(unlistens.map((un) => un()));
      if (finalize) await finalize?.call(self);
      shutdown = null;
      console.log(`[${name}] Stopped...`);
    };

    function poll(trigger: PollingTrigger) {
      pollingQueue.kill();
      pollingQueue.push(trigger);
    }

    async function doPoll(trigger: PollingTrigger) {
      clearScheduled();

      if (shutdown == null) return;

      const now = new Date();
      lastExecution = +now;

      const inQueueCount = inQueue.size;

      if (tasksConcurrency && inQueueCount >= tasksConcurrency) {
        willContinue = true;
        console.warn(
          `[${name}] Queue is overloaded (${inQueueCount}). Try again later.`
        );
      } else {
        if (taskQueue.idle()) willContinue = willRetry = false;
        console.log(`[${name}] Fetching at ${now.toISOString()} ~`, trigger);
        const fetched = await fetch.call(self, trigger);
        if (!fetched) console.log(`[${name}] Fetcher skips. Move on.`);
        else {
          const tasks: WithId<Task, TaskId>[] = [];
          for (const task of fetched.tasks) {
            const id = $id(task);
            (task as WithId<Task, TaskId>).id = id;
            if (!tasksConcurrency || !inQueue.has(id))
              tasks.push(task as WithId<Task, TaskId>);
          }
          willContinue ||= fetched.continue;
          const fetchedCount = fetched.tasks.length;
          const taskCount = tasks.length;
          console.log(
            `[${name}] Fetched ${fetchedCount} tasks.` +
              (taskCount !== fetchedCount
                ? ` ${fetchedCount - taskCount} of them are already in queue.`
                : "")
          );
          if (taskCount) {
            if (handle) {
              for (const task of tasks) inQueue.set(task.id, false);
              for (const task of tasks) taskQueue.push(task);
              if (!tasksConcurrency) await taskQueue.drained();
            }
            batch && (await batch.call(self, tasks));
            if (tasksConcurrency)
              console.log(
                `[${name}] Batched ${taskCount} tasks.` +
                  (willContinue
                    ? " Will continue after all tasks finished."
                    : "")
              );
            else console.log(`[${name}] Finished ${taskCount} tasks.`);
          }
        }
      }
      if (!tasksConcurrency && (willContinue || willRetry)) {
        if (willContinue) poll("continue");
        else if (willRetry)
          scheduled = setTimeout(
            () => pollingQueue.idle() && poll("retry"),
            retryDelay
          );
      } else if (!pollingQueue.length()) {
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
    if (shutdown) shutdown();
    else console.error(`[${name}] Already stopped.`);
  }

  return { start, stop };
}
