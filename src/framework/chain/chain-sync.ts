import {
  ChainSync,
  createPointFromCurrentTip,
  safeJSON,
  WebSocketClosed,
} from "@cardano-ogmios/client";
import * as O from "@cardano-ogmios/schema";
import fastq from "fastq";

import JsonBig from "@teiki/protocol/json";

import { OgmiosContext } from "../../ogmios";
import { ErrorHandler, queueCatch } from "../base";

type RequestNextResponse = O.Ogmios["RequestNextResponse"];
export type ChainSyncQueue = fastq.queueAsPromised<RequestNextResponse, void>;
export type ChainSyncReturn = {
  queue: ChainSyncQueue;
  intersection: ChainSync.Intersection;
};
export interface ChainSyncClient {
  context: OgmiosContext;
  startSync: (
    points: "origin" | "tip" | O.Point[],
    inFlight?: number,
    onError?: ErrorHandler
  ) => Promise<ChainSyncReturn>;
  shutdown: (immediate: boolean) => Promise<void>;
}

export function createChainSyncClient(
  context: OgmiosContext,
  rollForward: ChainSync.ChainSyncMessageHandlers["rollForward"],
  rollBackward: ChainSync.ChainSyncMessageHandlers["rollBackward"],
  options?: { sanitizeJson?: boolean; sharedContext?: boolean }
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
  const willShutdownContext = !options?.sharedContext;
  async function onMessage(message: string) {
    const response: RequestNextResponse = parseJson(message);
    if (response.methodname === "RequestNext") await queue.push(response);
  }
  return {
    context,
    startSync: async function (points, inFlight = 100, onError) {
      socket.on("message", onMessage);
      queueCatch(queue, onError, async (error) => {
        await this.shutdown(true);
        throw error;
      });
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
    shutdown: async function (immediate = true) {
      socket.removeListener("message", onMessage);
      if (immediate) {
        console.log("[Chain Sync] Queue Kill & Drain.");
        queue.killAndDrain();
      } else console.log("[Chain Sync] Queue Draining...");
      await queue.drained();
      console.log("[Chain Sync] Queue Drained!");
      willShutdownContext && (await context.shutdown());
    },
  };
}
