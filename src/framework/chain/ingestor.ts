import * as O from "@cardano-ogmios/schema";

import { UnixTime } from "@teiki/protocol/types";

import {
  CHAIN_BLOCK_INGESTION_CHECKPOINT,
  CHAIN_BLOCK_INGESTION_REPORT_RESOLUTION,
} from "../../config";
import { ChainBlock } from "../../types/chain";

export type BlockIngestor = ReturnType<typeof createBlockIngestor>;

type WithTime<T> = T & { time: UnixTime };

export function createBlockIngestor() {
  let counter = 0;
  let currentBlock: ChainBlock;
  let lastDoneBlock: ChainBlock | null = null;
  let lastFlush: UnixTime = 0;
  let inSync = false;

  return {
    flush: function (): void {
      if (counter) {
        const { slot, hash, height, time } = currentBlock;
        console.log(
          `-> ${slot} | ${hash} | ${new Date(
            time
          ).toISOString()} | ${height} (${counter})`
        );
        counter = 0;
        lastFlush = Date.now();
      }
    },
    rollForward: function (block: ChainBlock): boolean {
      currentBlock = block;
      ++counter;
      if (!lastFlush) {
        // Special cases:
        // Last block was a relevant one
        if (lastDoneBlock) lastFlush = Date.now();
        // First block or just rollbacked
        else this.flush();
        return true;
      } else if (counter >= CHAIN_BLOCK_INGESTION_CHECKPOINT) {
        this.flush();
        return true;
      } else if (
        inSync &&
        Date.now() >= lastFlush + CHAIN_BLOCK_INGESTION_REPORT_RESOLUTION
      ) {
        this.flush();
        return false;
      } else {
        return false;
      }
    },
    rollForwardDone: function (isRelevant: boolean) {
      lastDoneBlock = currentBlock;
      // So the next restart wouldn't reprocess this block
      if (inSync && isRelevant) lastFlush = 0;
    },
    rollBackward: function (point: WithTime<O.Point> | O.Origin): void {
      lastDoneBlock = null;
      this.flush();
      lastFlush = 0;
      if (point === "origin") console.log("<- Origin");
      else {
        const { slot, hash, time } = point;
        console.log(`<- ${slot} | ${hash} | ${new Date(time).toISOString()}`);
      }
    },
    set inSync(state: boolean) {
      inSync = state;
      this.flush();
    },
    get lastBlock() {
      return lastDoneBlock;
    },
  };
}
