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

export type Event = { type: "staking$reload"; hashes: Set<StakingHash> };
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
  const pending: Set<StakingHash> = new Set();
  const registry: Map<StakingHash, RewardAddress> = new Map();
  const revRegistry: Map<RewardAddress, StakingHash> = new Map();
  let fullReload = false;
  let willReloadDynamically = false;
  let isActive = false;

  let ogmiosContext: OgmiosContext;
  let client: StateQuery.StateQueryClient;

  let debounced: debounce<() => Promise<void>>;
  let scheduled: NodeJS.Timeout | null = null;
  const delay = options?.interval ?? DEFAULT_INTERVAL;

  const errorCallback = reduceErrorHandler(onError, (error) => {
    stop();
    throw error;
  });

  function clearScheduled() {
    if (scheduled) {
      clearTimeout(scheduled);
      scheduled = null;
    }
  }

  function reload(hashes: StakingHash[] | null) {
    if (fullReload) return;
    if (hashes == null) {
      fullReload = true;
      pending.size && pending.clear();
      debounced();
    } else {
      for (const hash of hashes) pending.add(hash);
      willReloadDynamically && debounced();
    }
  }

  async function stop() {
    if (isActive) {
      console.log("[staking] Stopping...");
      isActive = false;
      clearScheduled();
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

      isActive = true;
      console.log("[staking] Started!");

      // Multiple instances of this might be run at the same time, but it's completely fine
      async function doReload() {
        clearScheduled();
        if (!isActive) {
          console.warn(
            `[staking] Inactive. Ignored Reload: ${
              fullReload ? "Everything" : Array.from(pending.values())
            }`
          );
          pending.size && pending.clear();
          return;
        }
        console.log(
          `[staking] Reloading: ${fullReload ? "Everything" : pending.size}...`
        );

        let toReload: RewardAddress[] | undefined = undefined;
        const wasFullReload = fullReload;
        if (wasFullReload) {
          toReload = Array.from(registry.values());
          fullReload = false;
        } else if (pending.size) {
          toReload = [];
          for (const hash of pending.values()) {
            const address = registry.get(hash);
            if (address) toReload.push(address);
            else console.warn(`[staking] Address (to) not found for: ${hash}`);
          }
          pending.clear();
        }
        if (toReload?.length) {
          const [tip, response] = await Promise.all([
            client.ledgerTip(),
            client.delegationsAndRewards(toReload),
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
            `[staking] Reloaded: ${toReload.length} ! ` +
              (tip === "origin" ? "origin" : `${tip.slot} | ${tip.hash}`)
          );
          if (onReloaded)
            await onReloaded({ connections, reloaded: toReload, fullReload });
        }
        if (!scheduled) {
          console.log(
            `[staking] Schedule one in ${(delay / 1000).toFixed(1)}s`
          );
          scheduled = setTimeout(() => reload(null), delay);
        }
      }
    },
    stop,
    register: function (hash: StakingHash, type: StakingType) {
      const address = lucid.utils.credentialToRewardAddress({ type, hash });
      registry.set(hash, address);
      revRegistry.set(address, hash);
      console.log(`[staking] Registered: ${hash} - ${type} = ${address}`);
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
      const address = registry.get(hash);
      if (address) {
        registry.delete(hash);
        revRegistry.delete(address);
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
  const hashes = new Set<StakingHash>();
  for (const cert of body.certificates) {
    if ("stakeDelegation" in cert) {
      const hash = cert.stakeDelegation.delegator;
      staking.isHashRegistered(hash) && hashes.add(hash);
    } else if ("stakeKeyDeregistration" in cert) {
      const hash = cert.stakeKeyDeregistration;
      staking.isHashRegistered(hash) && hashes.add(hash);
    }
  }
  for (const address in body.withdrawals) {
    const hash = staking.fromAddress(address);
    hash && hashes.add(hash);
  }
  return hashes.size ? [{ type: "staking$reload", hashes }] : null;
});

export const reloadEvent = $.event(
  ({ context: { staking }, event: { hashes } }) => {
    staking.reload(Array.from(hashes));
  }
);
