import {
  ChainSync,
  createPointFromCurrentTip,
  InteractionContext,
  safeJSON,
  WebSocketClosed,
} from "@cardano-ogmios/client";
import * as O from "@cardano-ogmios/schema";
import fastq from "fastq";

import JsonBig from "@teiki/protocol/json";

import { ErrorHandler, queueCatch } from "../base";

type RequestNextResponse = O.Ogmios["RequestNextResponse"];
export type ChainSyncQueue = fastq.queueAsPromised<RequestNextResponse, void>;
export type ChainSyncReturn = {
  queue: ChainSyncQueue;
  intersection: ChainSync.Intersection;
};
export interface ChainSyncClient {
  context: InteractionContext;
  start: (
    points: "origin" | "tip" | O.Point[],
    inFlight?: number,
    onError?: ErrorHandler
  ) => Promise<ChainSyncReturn>;
  stop: (immediate: boolean) => Promise<void>;
}

export function createChainSyncClient(
  context: InteractionContext,
  rollForward: ChainSync.ChainSyncMessageHandlers["rollForward"],
  rollBackward: ChainSync.ChainSyncMessageHandlers["rollBackward"],
  options?: { sanitizeJson?: boolean }
): ChainSyncClient {
  // https://github.com/CardanoSolutions/ogmios/blob/master/clients/TypeScript/packages/client/src/ChainSync/ChainSyncClient.ts
  // Does not silence errors, and sequential by default (it's safer to handle blocks sequentially)
  const { socket } = context;
  const requestNext = () => ChainSync.requestNext(socket);
  const queue: ChainSyncQueue = fastq.promise(async (response) => {
    const result = response.result;
    if ("RollForward" in result)
      await rollForward(result.RollForward, requestNext);
    else if ("RollBackward" in result)
      await rollBackward(result.RollBackward, requestNext);
    else throw new ChainSync.UnknownResultError(result);
  }, 1);
  const parseJson =
    options?.sanitizeJson ?? true
      ? (raw: string) => safeJSON.sanitize(JsonBig.parse(raw))
      : JsonBig.parse;
  return {
    context,
    start: async (points, inFlight = 100, onError) => {
      socket.on("message", async (message: string) => {
        const response: RequestNextResponse = parseJson(message);
        if (response.methodname === "RequestNext") await queue.push(response);
      });
      queueCatch(queue, onError);
      const source =
        points === "origin"
          ? ["origin" as O.Origin]
          : points === "tip"
          ? [await createPointFromCurrentTip(context)]
          : points;
      const intersection = await ChainSync.findIntersect(context, source);
      if (socket.readyState !== socket.OPEN) throw new WebSocketClosed();
      for (let n = 0; n < inFlight; n++) requestNext();
      return { queue, intersection };
    },
    stop: (immediate = true) =>
      new Promise((resolve) => {
        const cleanup = () => {
          queue.drain = () => {
            console.log("[Chain Sync] Queue Drained!");
            resolve();
          };
          if (immediate) {
            console.log("[Chain Sync] Queue Kill & Drain.");
            queue.killAndDrain();
          } else {
            console.log("[Chain Sync] Queue Draining...");
          }
        };
        if (socket.readyState !== socket.CLOSED) {
          socket.once("close", cleanup);
          socket.close();
        } else cleanup();
      }),
  };
}
