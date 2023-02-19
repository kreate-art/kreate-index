import { scheduler } from "node:timers/promises";

import { ChainSync, isBabbageBlock, StateQuery } from "@cardano-ogmios/client";
import * as O from "@cardano-ogmios/schema";
import fastq from "fastq";
import * as L from "lucid-cardano";

import { Hex, TimeDifference, UnixTime } from "@teiki/protocol/types";
import { assert } from "@teiki/protocol/utils";

import {
  CHAIN_BLOCK_GC_INTERVAL,
  CHAIN_CHASING_BATCH_INTERVAL,
} from "../../config";
import { Connections } from "../../connections";
import { Sql } from "../../db";
import prexit from "../../prexit";
import {
  ChainBlock,
  ChainOutput,
  ChainScript,
  WithId,
} from "../../types/chain";
import { MaybePromise, NonEmpty } from "../../types/typelevel";
import { $setup, ErrorHandler, queueCatch, Setup } from "../base";

import {
  ChainSyncClient,
  ChainSyncReturn,
  createChainSyncClient,
} from "./chain-sync";
import {
  ogmiosScriptToLucidScript,
  ogmiosValueToLucidAssets,
  prettyOutRef,
  slotFrom,
} from "./conversions";
import { BlockIngestor, createBlockIngestor } from "./ingestor";
import { createSlotTimeInterpreter, SlotTimeInterpreter } from "./time";

// TODO: Ensure progress

// TODO: Avoid using such global var
let endTimeout: NodeJS.Timeout | undefined = undefined;

export const setupGenesis = $setup(async ({ sql }) => {
  await sql`
    -- Technically we can use "hash" as the pk. But "slot" is far more efficient.
    CREATE TABLE IF NOT EXISTS chain.block (
      slot integer PRIMARY KEY,
      hash varchar(64) NOT NULL,  -- Hex
      height integer NOT NULL,
      time timestamptz NOT NULL
    )
  `;
  await sql`
    CREATE TABLE IF NOT EXISTS chain.output (
      -- We don't cache the sequence...
      id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
      tag text,
      tx_id varchar(64) NOT NULL,  -- Hex
      tx_ix smallint NOT NULL,     -- 2 bytes should be enough
      address text NOT NULL,       -- Bech32
      value jsonb NOT NULL,        -- Lucid: Assets: Record<string | "lovelace", bigint>
                                    -- I think "value" is clearer than "assets"
      datum text,                  -- CBOR Hex
      datum_hash varchar(64),      -- CBOR Hex
      script_hash varchar(56),
      created_slot integer NOT NULL REFERENCES chain.block (slot) ON DELETE CASCADE,
      spent_slot integer REFERENCES chain.block (slot) ON DELETE SET NULL,
      UNIQUE (tx_id, tx_ix)     -- Just in case we mess up
    )
  `;
  await sql`
    CREATE INDEX IF NOT EXISTS output_created_slot
      ON chain.output(created_slot)
  `;
  await sql`
    CREATE INDEX IF NOT EXISTS output_spent_slot
      ON chain.output(spent_slot)
  `;
  // await sql`
  //   CREATE INDEX IF NOT EXISTS output_tag
  //     ON chain.output(tag)
  // `;
  await sql`
    CREATE TABLE IF NOT EXISTS chain.script (
      script_hash varchar(56) PRIMARY KEY,
      script_type varchar(8) NOT NULL,
      script text NOT NULL
    )
  `;
});

export type BaseChainIndexConnections = Connections<
  "sql" | "ogmios" | "lucid" | "notifications" | "views"
>;
export type ChainIndexConnections = BaseChainIndexConnections & {
  synchronized: fastq.queueAsPromised<() => unknown, unknown>;
  slotTimeInterpreter: SlotTimeInterpreter;
};

interface IEvent {
  type: string;
}

type ChainIndexInitializer<TContext> = (_: {
  connections: ChainIndexConnections;
  context: TContext;
}) => MaybePromise<void>;

type ChainIndexFilter<TContext, TEvent> = (_: {
  connections: ChainIndexConnections;
  context: TContext;
  block: ChainBlock;
  tx: O.TxBabbage;
  inSync: boolean;
}) => MaybePromise<TEvent[] | null>;

type ChainIndexEventHandler<TContext, TEvent> = (_: {
  driver: ChainIndexEventDriver;
  connections: ChainIndexConnections;
  context: TContext;
  block: ChainBlock;
  tx: O.TxBabbage;
  event: TEvent;
  inSync: boolean;
}) => MaybePromise<void>;

type ChainIndexRollbackHandler<TContext> = (_: {
  connections: ChainIndexConnections;
  context: TContext;
  point: O.PointOrOrigin;
  action: "begin" | "rollback" | "end";
}) => MaybePromise<void>;

// Blocks are handled sequentially
// Transactions in a single block are handled sequentially
type ChainIndexHandlers<TContext, TEvent extends IEvent> = {
  // Initializers are ran sequentially
  readonly initializers?: ChainIndexInitializer<TContext>[];
  // Filters are ran concurrently - note that events are not deduped
  readonly filters: ChainIndexFilter<TContext, TEvent>[];
  // Event Handlers are ran concurrently among events, but sequentially for a specific event.
  readonly events: {
    [EventType in TEvent["type"]]: ChainIndexEventHandler<
      TContext,
      Extract<TEvent, { type: EventType }>
    >[];
  };
  // Rollback Handlers are ran sequentially
  readonly rollbacks: ChainIndexRollbackHandler<TContext>[];
  // Called once
  readonly onceInSync?: ((_: {
    connections: ChainIndexConnections;
    context: TContext;
    tip: O.Tip;
  }) => MaybePromise<void>)[];
};

type ChainIndexerStatus = "inactive" | "starting" | "active" | "stopping";

// TODO: Revisit all usages of 'for (const ...)' and see if they're really need to be sequential.
export class ChainIndexer<TContext, TEvent extends IEvent> {
  private status: ChainIndexerStatus = "inactive";

  // `clients` will be set by ChainIndexer.new. It's fine since the constructor is private.
  private readonly clients!: {
    readonly chainSync: ChainSyncClient;
    readonly stateQuery: StateQuery.StateQueryClient;
  };
  private context!: TContext;

  private inSync!: boolean;
  private isBootstrapped!: boolean;
  private endAt!: O.Slot | undefined;
  private endDelay!: TimeDifference;

  private nextBlockGc!: UnixTime;
  private nextChasingBatchFlush!: UnixTime;
  private batch!: {
    notifications: Set<string>;
    refreshes: Set<string>;
  };

  private blockIngestor!: BlockIngestor;

  private constructor(
    private readonly connections: ChainIndexConnections,
    private readonly handlers: ChainIndexHandlers<TContext, TEvent>
  ) {}

  public static async new<TContext, TEvent extends IEvent>({
    connections,
    handlers,
  }: {
    connections: BaseChainIndexConnections;
    handlers: ChainIndexHandlers<TContext, TEvent>;
  }): Promise<ChainIndexer<TContext, TEvent>> {
    // Will be filled in by .start()
    const synchronized = undefined as unknown as fastq.queueAsPromised<
      () => unknown,
      unknown
    >;
    const slotTimeInterpreter = undefined as unknown as SlotTimeInterpreter;
    const self = new ChainIndexer(
      { ...connections, synchronized, slotTimeInterpreter },
      handlers
    );
    const ogmiosContext = await connections.ogmios.create("chain");
    // eslint-disable-next-line @typescript-eslint/ban-ts-comment
    // @ts-ignore: This is the only time `clients` is set.
    self.clients = {
      ogmiosContext,
      chainSync: createChainSyncClient(
        ogmiosContext,
        self.rollForward.bind(self),
        self.rollBackward.bind(self)
      ),
      stateQuery: await StateQuery.createStateQueryClient(ogmiosContext),
    };
    return self;
  }

  public async start({
    context,
    begin,
    end,
    reset = false,
    inFlight,
    // For finding intersection on start
    checkpointHistory = 20,
    onError,
  }: {
    context: TContext;
    begin: "origin" | "tip" | O.Point[];
    end?: { at: "tip" | O.Slot; delay?: TimeDifference };
    reset?: boolean;
    inFlight?: number;
    // For finding intersection on start
    checkpointHistory?: number;
    onError?: ErrorHandler;
  }): Promise<ChainSyncReturn> {
    assert(this.status === "inactive", "[Chain] Already started.");
    this.status = "starting";
    this.context = context;
    console.log("... Starting");
    const synchronized = fastq.promise((fn) => fn(), 1);
    queueCatch(synchronized, onError, async (error) => {
      await this.stop({ immediate: true });
      throw error;
    });

    const connections = this.connections;
    connections.synchronized = synchronized;

    const params = { connections, context };
    for (const initialize of this.handlers.initializers ?? [])
      await initialize(params);

    this.batch = { notifications: new Set(), refreshes: new Set() };
    this.blockIngestor = createBlockIngestor();

    this.inSync = false;
    this.isBootstrapped = false;
    this.nextBlockGc = 0;
    this.nextChasingBatchFlush = 0;

    let points;
    if (reset || begin === "tip") {
      points = begin;
    } else {
      const blocks = await connections.sql<ChainBlock[]>`
        SELECT *
          FROM chain.block
          ORDER BY slot DESC
          LIMIT ${checkpointHistory}
      `;
      points = blocks.length ? blocks : begin;
    }

    const startAt = points instanceof Array ? slotFrom(points[0]) : points;
    console.log(`[Chain] Will start at: ${startAt}`);

    const csr = await (async () => {
      const chainSync = this.clients.chainSync;
      for (;;) {
        const { tip } = await ChainSync.findIntersect(chainSync.context, [
          "origin",
        ]);
        if (typeof startAt === "number" && slotFrom(tip) >= startAt) {
          await this.reloadSlotTimeInterpreter();
          return chainSync.startSync(points, inFlight, onError);
        } else {
          const tipStr =
            tip === "origin"
              ? tip
              : `${tip.slot} | ${tip.hash} ~ ${tip.blockNo}`;
          console.warn(
            `[Chain] Wait until chain is sufficiently synced :: ${tipStr}`
          );
          await scheduler.wait(10_000);
        }
      }
    })();

    console.log("!!! Started");
    this.status = "active";

    const endAt = end?.at === "tip" ? slotFrom(csr.intersection.tip) : end?.at;
    this.endAt = endAt;
    this.endDelay = end?.delay ?? 0;

    if (endAt && slotFrom(csr.intersection.point) >= endAt) {
      console.log(`[Chain] Intersection >= ${endAt}`);
      console.log(`[Chain] Goodbye in ${(this.endDelay / 1000).toFixed(1)}s`);
      endTimeout = setTimeout(prexit.exit0, this.endDelay);
    } else if (endAt) {
      console.log(`[Chain] Will stop at: ${endAt}`);
    }
    return csr;
  }

  public async stop({
    immediate,
    saveCheckpoint = true,
  }: {
    immediate: boolean;
    saveCheckpoint?: boolean;
  }) {
    if (this.status === "inactive") {
      console.warn("[Chain] Already stopped.");
    } else if (this.status === "stopping") {
      console.warn("[Chain] Already stopping...");
    } else if (this.status === "starting") {
      console.warn(
        "[Chain] Not fully started yet. Try stopping again later..."
      );
      await scheduler.wait(1_000);
      await this.stop({ immediate, saveCheckpoint });
    } else {
      this.status = "stopping";
      console.log("... Stopping");
      await this.clients.chainSync.shutdown(immediate);
      const { blockIngestor, connections, context } = this;
      const { sql, synchronized } = connections;
      synchronized.killAndDrain();
      await synchronized.drained();
      const lastBlock = blockIngestor.lastBlock;
      if (lastBlock) {
        console.log(`^^ Checkpoint revert...`);
        this.blockIngestor.rollBackward(lastBlock);
        const params = {
          point: lastBlock,
          connections,
          context,
          action: "end" as const,
        };
        for (const handler of this.handlers.rollbacks) await handler(params);
        await sqlDeleteChainBlockAfter(sql, lastBlock.slot);
        if (saveCheckpoint) {
          await sqlInsertChainBlock(sql, lastBlock, true);
          console.log("^^ Checkpoint saved!");
        } else {
          console.warn("^^ Checkpoint ignored!");
        }
      } else {
        console.warn("^^ No checkpoint :(");
      }
      console.log("!!! Stopped");
      this.status = "inactive";
    }
  }

  private async rollForward(
    { block: anyBlock, tip }: O.RollForward["RollForward"],
    requestNext: () => void
  ) {
    requestNext();
    if (!isBabbageBlock(anyBlock)) {
      console.error("Only support Babbage blocks!");
      return;
    }
    const block = anyBlock.babbage;
    const {
      header: { slot, blockHeight: height },
      headerHash: hash,
    } = block;

    const {
      connections,
      context,
      handlers: { filters, events },
      blockIngestor,
      batch,
      endAt,
    } = this;

    let interpreter = connections.slotTimeInterpreter;
    if (slot > interpreter.staleSlot) {
      interpreter = await this.reloadSlotTimeInterpreter();
      if (slot > interpreter.staleSlot)
        throw new Error(`Forever stale for ${slot}`);
    }
    const time = interpreter.slotToAbsoluteTime(slot);

    const cblock: ChainBlock = { slot, hash, height, time };

    let inSync = this.inSync;
    if (
      !inSync &&
      (inSync = tip !== "origin" && slot === tip.slot && hash === tip.hash)
    ) {
      this.inSync = true;
      blockIngestor.inSync = true;
      console.log(`!! NOW IN SYNC !!`);
      for (const once of this.handlers.onceInSync ?? [])
        await once({ connections, context, tip });
    }

    const shouldStore = blockIngestor.rollForward(cblock);
    let isBlockStored = false;

    const sql = connections.sql;
    try {
      for (const tx of block.body) {
        const params = {
          tx,
          block: cblock,
          connections,
          context,
          inSync,
        };
        const foundEvents: TEvent[] = [];
        const filterResults = await Promise.all(filters.map((f) => f(params)));
        for (const result of filterResults)
          if (result != null) foundEvents.push(...result);
        if (!foundEvents.length) continue;

        if (!isBlockStored) {
          blockIngestor.flush();
          await sqlInsertChainBlock(sql, cblock);
          isBlockStored = true;
        }

        const driver = createEventDriver(connections, batch, tx, slot);
        const paramsWithDriver = { ...params, driver };

        await Promise.all(
          foundEvents.map(async (event) => {
            const eventType: TEvent["type"] = event.type;
            const eventHandlers = events[eventType];
            const eventParams = {
              event: event as Extract<TEvent, { type: typeof eventType }>,
              ...paramsWithDriver,
            };
            for (const handler of eventHandlers) await handler(eventParams);
          })
        );

        // TODO: Handle taken collaterals...
        // Transaction JSON objects from the **Alonzo** era now contains an extra field `inputSource` which a string set to either `inputs` or `collaterals`.
        // This captures the fact that since the introduction of Plutus scripts in Alonzo, some transactions may be recorded as _failed_ transactions in the ledger.
        // Those transactions do not successfully spend their inputs but instead, consume their collaterals as an input source to compensate block validators for their work.

        // Mark UTxOs as spent
        await sql`
          UPDATE chain.output
          SET spent_slot = ${slot}
          WHERE (tx_id, tx_ix) IN ${sql(
            tx.body.inputs.map((input) => sql([input.txId, input.index]))
          )}
        `;

        await driver._finally();
      }
      if (inSync && time >= this.nextBlockGc) {
        const res = await sqlDeleteDetachedChainBlock(sql);
        console.log(`// GC detached blocks: ${res.count}`);
        this.nextBlockGc = time + CHAIN_BLOCK_GC_INTERVAL;
      }
      if (inSync || time >= this.nextChasingBatchFlush) {
        const { notifications, refreshes } = batch;
        if (notifications.size) {
          const notify = connections.notifications.notify;
          for (const c of notifications) notify(c);
          notifications.clear();
        }
        if (refreshes.size) {
          const refresh = connections.views.refresh;
          for (const v of refreshes) refresh(v);
          refreshes.clear();
        }
        if (!inSync)
          this.nextChasingBatchFlush = time + CHAIN_CHASING_BATCH_INTERVAL;
      }
      if (shouldStore && !isBlockStored) await sqlInsertChainBlock(sql, cblock);
      blockIngestor.rollForwardDone(isBlockStored);
    } catch (e) {
      blockIngestor.flush();
      throw e;
    }

    if (endAt && slot >= endAt && !endTimeout) {
      console.log(`[Chain] Goodbye in ${(this.endDelay / 1000).toFixed(1)}s`);
      endTimeout = setTimeout(prexit.exit0, this.endDelay);
    }
  }

  private async rollBackward(
    { point }: O.RollBackward["RollBackward"],
    requestNext: () => void
  ) {
    requestNext();
    let action: "begin" | "rollback" = "rollback";
    if (!this.isBootstrapped) {
      action = "begin";
      this.isBootstrapped = true;
    }
    const connections = this.connections;
    if (point === "origin") {
      await this.reloadSlotTimeInterpreter();
      this.blockIngestor.rollBackward("origin");
    } else {
      const { slot, hash } = point;
      let interpreter = connections.slotTimeInterpreter;
      if (slot < interpreter.ledgerTipSlot)
        interpreter = await this.reloadSlotTimeInterpreter();
      const time = interpreter.slotToAbsoluteTime(slot);
      this.blockIngestor.rollBackward({ slot, hash, time });
    }
    const {
      context,
      handlers: { rollbacks },
    } = this;
    const params = { point, connections, context, action };
    for (const handler of rollbacks) await handler(params);
    await sqlDeleteChainBlockAfter(connections.sql, slotFrom(point));
  }

  private async reloadSlotTimeInterpreter(): Promise<SlotTimeInterpreter> {
    const stateQuery = this.clients.stateQuery;
    const interpreter = createSlotTimeInterpreter(
      ...(await Promise.all([
        stateQuery.ledgerTip(),
        stateQuery.eraSummaries(),
        stateQuery.systemStart().then((d) => +d),
      ]))
    );
    console.log(
      `// Slot time interpreter reloaded at ${interpreter.ledgerTipSlot}, ` +
        `stale after ${interpreter.staleSlot}.`
    );
    this.connections.slotTimeInterpreter = interpreter;
    return interpreter;
  }
}

// Type-level utilities for easier registration of handlers
export function $handlers<TContext, TEvent extends IEvent = { type: never }>() {
  return {
    setup: (fn: Setup) => fn,
    initialize: (fn: ChainIndexInitializer<TContext>) => fn,
    filter: (fn: ChainIndexFilter<TContext, TEvent>) => fn,
    event: <EventType extends TEvent["type"]>(
      fn: ChainIndexEventHandler<TContext, Extract<TEvent, { type: EventType }>>
    ) => fn,
    rollback: (fn: ChainIndexRollbackHandler<TContext>) => fn,
  };
}

export interface ChainIndexEventDriver {
  notify(channel: string): void;
  refresh(view: string): void;
  store<T>(
    indicies: number[],
    each: (output: ChainOutput, index: number) => [string | null, T] | undefined
  ): Promise<WithId<T>[]>;
  storeWithScript<T>(
    indicies: number[],
    each: (output: ChainOutput, index: number) => [string | null, T] | undefined
  ): Promise<WithId<T>[]>;
  _finally(): Promise<void>;
}

function createEventDriver(
  { synchronized, sql, lucid }: ChainIndexConnections,
  batch: {
    notifications: Set<string>;
    refreshes: Set<string>;
  },
  tx: O.TxBabbage,
  slot: O.Slot
): ChainIndexEventDriver {
  const createdOutputs = new Map<number, WithId<ChainOutput>>();
  let cachedScripts: Map<L.ScriptHash, ChainScript> | undefined;

  const txId = tx.id;
  const txOutputs = tx.body.outputs;

  async function doStore<T>(
    indicies: number[],
    each: (
      output: ChainOutput,
      index: number
    ) => [string | null, T] | undefined,
    withScript = false
  ) {
    const result: WithId<T>[] = [];
    const outputs: WithId<ChainOutput>[] = [];
    const dirty: ChainOutput[] = [];

    for (const index of indicies) {
      let output = createdOutputs.get(index);
      if (output === undefined) {
        const {
          address,
          datumHash,
          datum,
          value,
          script: ogmiosScript,
        } = txOutputs[index];

        let normalizedDatum: Hex | null;
        if (datum == null) normalizedDatum = null;
        else if (typeof datum === "string") normalizedDatum = datum;
        // FIXME: Based on what I've seen, no output has this.
        else throw new Error(`We can't handle this kind of datum: ${datum}`);

        let scriptHash: L.ScriptHash | null = null;
        let script: L.Script | null = null;
        if (withScript && ogmiosScript != null) {
          script = ogmiosScriptToLucidScript(ogmiosScript);
          scriptHash = lucid.utils.validatorToScriptHash(script);
        }

        const dirtyOutput: ChainOutput = {
          tag: null,
          txId,
          txIx: index,
          address,
          datumHash: datumHash ?? null,
          datum: normalizedDatum,
          value: ogmiosValueToLucidAssets(value),
          scriptHash,
          createdSlot: slot,
          spentSlot: null,
        };

        const ret = each(dirtyOutput, index);
        if (ret === undefined) continue;
        const [tag, ins] = ret;
        dirtyOutput.tag = tag;

        // This is a hack, we will assign the ids later
        dirty.push(dirtyOutput);
        output = dirtyOutput as WithId<ChainOutput>;
        createdOutputs.set(index, output);

        result.push(ins as WithId<T>);
        if (scriptHash != null && script != null)
          // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
          cachedScripts!.set(scriptHash, {
            scriptHash,
            scriptType: script.type,
            script: script.script,
          });
      } else {
        const ret = each(output, index);
        if (ret === undefined) continue;
        const [tag, ins] = ret;
        if (tag != null && output.tag != null && output.tag != tag)
          console.warn(
            `Different tags for output ${prettyOutRef(output)}: ` +
              `${output.tag} | ${tag}`
          );
        result.push(ins as WithId<T>);
      }
      outputs.push(output);
    }
    await synchronized.push(async () => {
      dirty.length && assignIds(dirty, await sqlInsertChainOutputs(sql, dirty));
    });
    assignIds(result, outputs);
    return result;
  }

  return {
    notify: batch.notifications.add.bind(batch.notifications),
    refresh: batch.refreshes.add.bind(batch.refreshes),

    async store(indicies, each) {
      return doStore(indicies, each, false);
    },

    async storeWithScript(indicies, each) {
      if (cachedScripts === undefined) cachedScripts = new Map();
      return doStore(indicies, each, true);
    },

    async _finally() {
      if (cachedScripts?.size)
        await sqlInsertChainScripts(sql, Array.from(cachedScripts.values()));
    },
  };
}

function assignIds<T, I = bigint>(array: T[], withIds: { id: I }[]) {
  for (const [i, element] of array.entries())
    (element as WithId<T, I>).id = withIds[i].id;
}

async function sqlInsertChainBlock(
  sql: Sql,
  block: ChainBlock,
  resilient = false
) {
  return sql`
    INSERT INTO chain.block ${sql(block)}
      ${resilient ? sql`ON CONFLICT DO NOTHING` : sql``}
  `;
}

async function sqlDeleteChainBlockAfter(sql: Sql, slot: O.Slot) {
  return sql`DELETE FROM chain.block WHERE slot > ${slot}`;
}

async function sqlDeleteDetachedChainBlock(sql: Sql) {
  return sql`
    DELETE FROM chain.block b
    WHERE
      NOT EXISTS (
        SELECT FROM chain.output o
        WHERE o.created_slot = b.slot
      ) AND
      NOT EXISTS (
        SELECT FROM chain.output o
        WHERE o.spent_slot = b.slot
      );
  `;
}

async function sqlInsertChainOutputs(
  sql: Sql,
  outputs: NonEmpty<ChainOutput[]>
): Promise<{ id: bigint }[]> {
  return sql`
    INSERT INTO chain.output ${sql(outputs)}
      RETURNING id
  `;
}

async function sqlInsertChainScripts(
  sql: Sql,
  scripts: NonEmpty<ChainScript[]>
) {
  return sql`
    INSERT INTO chain.script ${sql(scripts)}
      ON CONFLICT DO NOTHING
  `;
}
