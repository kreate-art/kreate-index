import { StateQuery } from "@cardano-ogmios/client";
import { Slot } from "@cardano-ogmios/schema";
import { PoolId, RewardAddress } from "lucid-cardano";
import { debounce } from "throttle-debounce";

import { Connections } from "../../connections";
import { $setup, ErrorHandler, reduceErrorHandler } from "../../framework/base";
import { $handlers } from "../../framework/chain";
import { slotFrom } from "../../framework/chain/conversions";
import { OgmiosContext } from "../../ogmios";
import { Lovelace } from "../../types/chain";
import { StakingHash, StakingType } from "../../types/staking";
import { MaybePromise } from "../../types/typelevel";

import { TeikiChainIndexContext } from "./context";

// TODO: This will break if a StakeKey and a StakeScript shares the same blake2b-224
// However it is fine since we never use StakeKey in Teiki anyways.

const DEFAULT_DEBOUNCE = 250; // 0.25s
const DEFAULT_INTERVAL = 300_000; // 5 minute

export type ChainStaking = {
  hash: StakingHash;
  address: RewardAddress;
  poolId: PoolId | null;
  rewards: Lovelace;
  reloadedSlot: Slot;
};

export type Event = {
  type: "staking";
  reload: StakingHash[];
  remove: StakingHash[];
};
const $ = $handlers<TeikiChainIndexContext, Event>();

export const setup = $setup(async ({ sql }) => {
  // TODO: Index slot if we want to fine-tune rollback later
  await sql`
    CREATE TABLE IF NOT EXISTS chain.staking (
      hash varchar(56) PRIMARY KEY,
      address TEXT NOT NULL,
      pool_id TEXT,
      rewards bigint NOT NULL,
      reloaded_slot integer NOT NULL
    )
  `;
});

type VitalConnections = Connections<"sql" | "ogmios" | "lucid" | "views">;

export function createIndexer({
  connections,
  options,
  onError,
  onReloaded,
}: {
  connections: VitalConnections;
  options?: {
    debounce?: number;
    interval?: number;
  };
  onError?: ErrorHandler;
  onReloaded?: (_: {
    connections: VitalConnections;
    reloaded: StakingHash[];
    fullReload: boolean;
  }) => MaybePromise<void>;
}) {
  const { sql, ogmios, lucid } = connections;
  const toReload: Set<StakingHash> = new Set();
  const toRemove: Set<StakingHash> = new Set();
  const registry: Map<StakingHash, RewardAddress> = new Map();
  const revRegistry: Map<RewardAddress, StakingHash> = new Map();
  let fullReload = false;
  let willReloadDynamically = false;
  let isActive = false;

  let ogmiosContext: OgmiosContext;
  let client: StateQuery.StateQueryClient;

  let debounced: debounce<() => Promise<void>>;
  let timer: NodeJS.Timer;

  const errorCallback = reduceErrorHandler(onError, (error) => {
    stop();
    throw error;
  });

  function reload(hashes: StakingHash[] | null) {
    if (fullReload) return;
    if (hashes == null) {
      fullReload = true;
      debounced();
    } else {
      for (const hash of hashes) toReload.add(hash);
      willReloadDynamically && debounced();
    }
  }

  async function stop() {
    if (isActive) {
      console.log("[staking] Stopping...");
      isActive = false;
      clearInterval(timer);
      debounced.cancel();
      await ogmiosContext.shutdown(() => client.shutdown());
      console.log("[staking] Stopped!");
    } else {
      console.warn("[staking] Already stopped.");
    }
  }

  return {
    start: async function () {
      console.log("[staking] Starting...");
      ogmiosContext = await ogmios.create("staking");
      client = await StateQuery.createStateQueryClient(ogmiosContext);

      debounced = debounce(options?.debounce ?? DEFAULT_DEBOUNCE, () =>
        doReload().catch(errorCallback)
      );
      timer = setInterval(
        () => reload(null),
        options?.interval ?? DEFAULT_INTERVAL
      );

      isActive = true;
      console.log("[staking] Started!");

      // Multiple instances of this might be run at the same time, but it's completely fine
      async function doReload() {
        if (!isActive) {
          console.warn(
            `[staking] Inactive. Ignored Reload: ${
              fullReload ? "All" : Array.from(toReload.values())
            }`
          );
          toReload.size && toReload.clear();
          return;
        }

        let batch: RewardAddress[] | undefined = undefined;
        const wasFullReload = fullReload;
        if (wasFullReload) {
          console.log("[staking] Reloading: All...");
          batch = Array.from(registry.values());
          fullReload = false;
          toReload.size && toReload.clear();
        } else if (toReload.size) {
          console.log(`[staking] Reloading: ${toReload.size}...`);
          batch = [];
          for (const hash of toReload.values()) {
            const address = registry.get(hash);
            if (address) batch.push(address);
            else console.warn(`[staking] Address (to) not found for: ${hash}`);
          }
          toReload.clear();
        }

        if (batch?.length) {
          const [tip, response] = await Promise.all([
            client.ledgerTip(),
            client.delegationsAndRewards(batch),
          ]);
          const stakings: ChainStaking[] = [];
          for (const [hash, entry] of Object.entries(response)) {
            const address = registry.get(hash) ?? "";
            if (!address)
              console.warn(`[staking] Address (from) not found for: ${hash}`);
            stakings.push({
              hash,
              address,
              poolId: entry.delegate ?? null,
              rewards: entry.rewards ?? 0n,
              reloadedSlot: slotFrom(tip),
            });
            if (toRemove.has(hash)) {
              registry.delete(hash);
              revRegistry.delete(address);
              console.log(`[staking] Removed: ${hash} - ${address}`);
            }
          }
          await sql`
            INSERT INTO chain.staking ${sql(stakings)}
              ON CONFLICT (hash) DO UPDATE
              SET address = EXCLUDED.address,
                  pool_id = EXCLUDED.pool_id,
                  rewards = EXCLUDED.rewards,
                  reloaded_slot = EXCLUDED.reloaded_slot
          `;
          console.log(
            `[staking] Reloaded: ${batch.length} ! ` +
              (tip === "origin" ? "origin" : `${tip.slot} | ${tip.hash}`)
          );
          if (onReloaded)
            await onReloaded({ connections, reloaded: batch, fullReload });
        }
      }
    },
    stop,
    reset: function () {
      registry.clear();
      revRegistry.clear();
      toReload.clear();
      toRemove.clear();
      console.log(`[staking] Reset!`);
    },
    register: function (hash: StakingHash, type: StakingType) {
      const address = lucid.utils.credentialToRewardAddress({ type, hash });
      registry.set(hash, address);
      revRegistry.set(address, hash);
      console.log(`[staking] Registered: (${type}) ${hash} - ${address}`);
    },
    batchRegister: function (hashes: StakingHash[], type: StakingType) {
      for (const hash of hashes) {
        const address = lucid.utils.credentialToRewardAddress({ type, hash });
        registry.set(hash, address);
        revRegistry.set(address, hash);
      }
      console.log(`[staking] Registered: A batch of ${hashes.length} ${type}`);
    },
    deregister: function (hash: StakingHash): boolean {
      if (registry.has(hash)) {
        toRemove.add(hash);
        console.log(`[staking] Deregistered: ${hash}`);
        return true;
      } else {
        console.warn(`[staking] Not Found: ${hash}`);
        return false;
      }
    },
    fromHash: function (hash: StakingHash): RewardAddress | undefined {
      return registry.get(hash);
    },
    fromAddress: function (address: RewardAddress): StakingHash | undefined {
      return revRegistry.get(address);
    },
    isHashRegistered: function (hash: StakingHash): boolean {
      return registry.has(hash);
    },
    isAddressRegistered: function (address: RewardAddress): boolean {
      return revRegistry.has(address);
    },
    toggleReloadDynamically(state: boolean) {
      willReloadDynamically = state;
    },
    reload,
  };
}

export const filter = $.filter((params) => {
  if (!params.inSync) return null;
  const staking = params.context.staking;
  const body = params.tx.body;
  const reload = [];
  const remove = [];
  for (const cert of body.certificates) {
    if ("stakeDelegation" in cert) {
      const hash = cert.stakeDelegation.delegator;
      staking.isHashRegistered(hash) && reload.push(hash);
    } else if ("stakeKeyDeregistration" in cert) {
      const hash = cert.stakeKeyDeregistration;
      if (staking.isHashRegistered(hash)) {
        remove.push(hash);
        reload.push(hash);
      }
    }
  }
  for (const address in body.withdrawals) {
    const hash = staking.fromAddress(address);
    hash && reload.push(hash);
  }
  return reload.length ? [{ type: "staking", reload, remove }] : null;
});

export const event = $.event(
  ({ context: { staking }, event: { reload, remove } }) => {
    for (const hash of remove) staking.deregister(hash);
    staking.reload(reload);
  }
);
