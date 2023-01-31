import fastq from "fastq";

import { Connections } from "../connections";
import { MaybePromise } from "../types/typelevel";

type Essential = Connections<"sql">;

export type Setup<Resources = Essential> = (
  resources: Resources
) => MaybePromise<void>;

export type Indexer<Resources = Essential> = {
  setup: Setup<Resources>;
  run: () => MaybePromise<() => MaybePromise<void>>;
};

export function $setup(setup: Setup) {
  return setup;
}

export type ErrorHandler = (error: Error, finish: ErrorCallback) => void;

type ErrorCallback = (error: Error) => void;

export function queueCatch(
  queue: fastq.queue,
  onError?: ErrorHandler,
  finish?: ErrorCallback
) {
  queue.error(
    reduceErrorHandler(
      onError,
      finish ??
        ((error: Error) => {
          queue.killAndDrain();
          throw error;
        })
    )
  );
}

export function reduceErrorHandler(
  onError?: ErrorHandler,
  finish: ErrorCallback = (error: Error) => {
    throw error;
  }
): ErrorCallback {
  return onError
    ? (error) => error != null && onError(error, finish)
    : (error) => error != null && finish(error);
}
