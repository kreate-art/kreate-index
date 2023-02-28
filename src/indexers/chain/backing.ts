import * as O from "@cardano-ogmios/schema";
import { Address, TxHash } from "lucid-cardano";
import * as L from "lucid-cardano";

import {
  constructAddress,
  constructTxOutputId,
  deconstructAddress,
  constructPlantHashUsingBlake2b,
} from "@teiki/protocol/helpers/schema";
import * as S from "@teiki/protocol/schema";
import { BackingDatum, Plant } from "@teiki/protocol/schema/teiki/backing";
import { Hex, UnixTime } from "@teiki/protocol/types";
import { assert } from "@teiki/protocol/utils";

import { $handlers } from "../../framework/chain";
import { prettyOutRef } from "../../framework/chain/conversions";
import { BackingActionType, BackingActionTypes } from "../../types/backing";
import { Lovelace } from "../../types/chain";
import { NonEmpty } from "../../types/typelevel";

import { TeikiChainIndexContext } from "./context";

export type ChainBacking = {
  projectId: Hex;
  backerAddress: Address;
  backingAmount: Lovelace;
  milestoneBacked: number;
  backingMessage: string | null;
  unbackingMessage: string | null;
  backedAt: UnixTime;
  unbackedAt: UnixTime | null;
};

export type ChainBackingAction = {
  action: BackingActionType;
  projectId: Hex;
  actorAddress: Address;
  amount: Lovelace;
  time: UnixTime;
  message: string | null;
  slot: O.Slot;
  txId: TxHash;
};

export type Event = {
  type: "backing";
  indicies: NonEmpty<number[]> | null;
  claimedFlowerHashes: NonEmpty<string[]> | null;
};
const $ = $handlers<TeikiChainIndexContext, Event>();

export const setup = $.setup(async ({ sql }) => {
  await sql`
    CREATE TABLE IF NOT EXISTS chain.backing (
      id bigint PRIMARY KEY REFERENCES chain.output (id) ON DELETE CASCADE,
      project_id varchar(64) NOT NULL,
      backer_address text NOT NULL,
      backing_amount bigint NOT NULL,
      milestone_backed smallint NOT NULL,
      backed_at timestamptz NOT NULL
    )
  `;
  await sql`
    CREATE INDEX IF NOT EXISTS backing_pid_index
      ON chain.backing(project_id)
  `;
  await sql`
    CREATE INDEX IF NOT EXISTS backing_backer_address_index
      ON chain.backing(backer_address)
  `;

  await sql`
    DO $$ BEGIN
      IF to_regtype('chain.backing_action_type') IS NULL THEN
        CREATE TYPE chain.backing_action_type AS ENUM (${sql.unsafe(
          BackingActionTypes.map((a) => `'${a}'`).join(", ")
        )});
      END IF;
    END $$
  `;
  await sql`
    CREATE TABLE IF NOT EXISTS chain.backing_action (
      id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
      action chain.backing_action_type NOT NULL,
      project_id varchar(64) NOT NULL,
      actor_address text NOT NULL,
      amount bigint NOT NULL,
      time timestamptz NOT NULL,
      message text,
      slot integer NOT NULL REFERENCES chain.block (slot) ON DELETE CASCADE,
      tx_id varchar(64) NOT NULL
    )
  `;
  await sql`
    CREATE INDEX IF NOT EXISTS backing_action_pid_index
      ON chain.backing_action(project_id)
  `;
  await sql`
    CREATE INDEX IF NOT EXISTS backing_action_address_index
      ON chain.backing_action(actor_address)
  `;
  await sql`
    CREATE INDEX IF NOT EXISTS backing_action_type_index
      ON chain.backing_action(action)
  `;
  await sql`
    CREATE INDEX IF NOT EXISTS backing_action_slot_index
      ON chain.backing_action(slot)
  `;
  await sql`
    CREATE INDEX IF NOT EXISTS backing_action_time_index
      ON chain.backing_action(time)
  `;
  await sql`
    CREATE INDEX IF NOT EXISTS backing_action_tx_id_index
      ON chain.backing_action(tx_id)
  `;
  await sql`
    CREATE TABLE IF NOT EXISTS chain.proof_of_backing (
      id bigint PRIMARY KEY REFERENCES chain.output (id) ON DELETE CASCADE,
      slot integer NOT NULL REFERENCES chain.block (slot) ON DELETE CASCADE, -- plant slot
      is_matured boolean NOT NULL,
      hash varchar(64) NOT NULL,
      mph varchar(64) NOT NULL
    )
  `;
  await sql`
    CREATE INDEX IF NOT EXISTS flower_slot_index
      ON chain.proof_of_backing(slot)
  `;
  await sql`
    CREATE INDEX IF NOT EXISTS flower_is_matured_index
      ON chain.proof_of_backing(is_matured)
  `;
  await sql`
    CREATE UNIQUE INDEX IF NOT EXISTS flower_hash_mph_index
      ON chain.proof_of_backing(hash, mph)
  `;
});

export const filter = $.filter(
  ({
    tx,
    context: {
      config: {
        assetsProofOfBacking: { seed, wilted },
        mphProofOfBackings,
      },
    },
  }) => {
    const backingIndicies: number[] = [];
    for (const [index, output] of tx.body.outputs.entries()) {
      const assets = output.value.assets;
      if (assets && seed.some((a) => assets[a] === 1n))
        backingIndicies.push(index);
    }
    if (backingIndicies.length) {
      return [
        {
          type: "backing",
          indicies: backingIndicies,
          claimedFlowerHashes: null,
        },
      ];
    } else {
      const minted = tx.body.mint.assets;
      if (minted) {
        const isMinted = (a: string) => minted[a];
        if (seed.some(isMinted) || wilted.some(isMinted))
          return [
            { type: "backing", indicies: null, claimedFlowerHashes: null },
          ];

        // Since we have already checked for seed and wilted,
        // focusing solely on verifying the minting policy should suffice for now.
        const claimedFlowerHashes: string[] = [];
        for (const [asset, amount] of Object.entries(minted)) {
          if (amount !== -1n) continue;
          for (const mph of mphProofOfBackings) {
            const [currentMph, tokenName] = asset.split(".");
            if (currentMph === mph) claimedFlowerHashes.push(tokenName);
          }
        }

        if (claimedFlowerHashes.length > 0)
          return [{ type: "backing", indicies: null, claimedFlowerHashes }];
      }
    }
    return null;
  }
);

export const event = $.event(
  async ({
    driver,
    connections: { sql, lucid, slotTimeInterpreter },
    block: { slot, time },
    tx,
    event: { indicies, claimedFlowerHashes },
    context: {
      config: { mphProofOfBackings },
    },
  }) => {
    const txTimeStart = slotTimeInterpreter.slotToAbsoluteTime(
      tx.body.validityInterval.invalidBefore ?? slot
    );
    const mintedAssets = tx.body.mint.assets;
    const message = extractCip20Message(tx)?.join("\n") || null;
    const backingActions: ChainBackingAction[] = [];
    const txId = tx.id;

    if (claimedFlowerHashes) {
      const backingActions: ChainBackingAction[] = [];

      const chainBackingWithOutputIds = await sql<
        (ChainBacking & {
          txId: L.TxHash;
          txIx: number;
          id: bigint;
          mph: string;
        })[]
      >`
        SELECT
          o.tx_id, o.tx_ix, o.id, f.mph as mph,
          b.*
        FROM
          chain.backing b
          INNER JOIN chain.output o ON o.id = b.id
          INNER JOIN chain.proof_of_backing pob ON pob.id = b.id
        WHERE
          (pob.hash) IN ${sql(claimedFlowerHashes)}
          AND !pob.is_matured
      `;
      for (const chainBacking of chainBackingWithOutputIds) {
        const plant: Plant = {
          isMatured: false,
          backingOutputId: constructTxOutputId({
            txHash: chainBacking.txId,
            outputIndex: Number(chainBacking.txIx),
          }),
          backingAmount: BigInt(chainBacking.backingAmount),
          unbackedAt: { timestamp: BigInt(txTimeStart) },
          projectId: { id: chainBacking.projectId },
          backerAddress: constructAddress(chainBacking.backerAddress),
          backedAt: { timestamp: BigInt(chainBacking.backedAt) },
          milestoneBacked: BigInt(chainBacking.milestoneBacked),
        };

        const fruitHash = constructPlantHashUsingBlake2b({
          ...plant,
          isMatured: true,
        });
        if (
          mintedAssets != null &&
          mintedAssets[chainBacking.mph + "." + fruitHash] === 1n
        ) {
          await sql`INSERT INTO chain.proof_of_backing ${sql({
            id: chainBacking.id,
            slot,
            isMatured: true,
            mph: chainBacking.mph,
            hash: fruitHash,
          })}`;

          backingActions.push({
            action: "claim_rewards",
            projectId: chainBacking.projectId,
            actorAddress: chainBacking.backerAddress, // TODO: incorrect here
            amount: 0n,
            time, // block time at which executed the action
            message: null,
            slot,
            txId: txId,
          });
        }
      }
    }

    const chainBackingWithOutputIds = await sql<
      (ChainBacking & { txId: L.TxHash; txIx: number; id: bigint })[]
    >`
        SELECT
          o.tx_id, o.tx_ix, o.id,
          b.*
        FROM
          chain.backing b
          INNER JOIN chain.output o ON o.id = b.id
        WHERE
          (o.tx_id, o.tx_ix) IN ${sql(
            tx.body.inputs.map((input) => sql([input.txId, input.index]))
          )}
      `;

    const actionRef = new Map<string, { unback: Lovelace; back: Lovelace }>();

    for (const chainBacking of chainBackingWithOutputIds) {
      const key = `${chainBacking.projectId}|${chainBacking.backerAddress}`;
      const ref = actionRef.get(key);
      actionRef.set(
        key,
        ref
          ? {
              unback: chainBacking.backingAmount + ref.unback,
              back: ref.back,
            }
          : {
              unback: chainBacking.backingAmount,
              back: 0n,
            }
      );

      const plant: Plant = {
        isMatured: false,
        backingOutputId: constructTxOutputId({
          txHash: chainBacking.txId,
          outputIndex: Number(chainBacking.txIx),
        }),
        backingAmount: BigInt(chainBacking.backingAmount),
        unbackedAt: { timestamp: BigInt(txTimeStart) },
        projectId: { id: chainBacking.projectId },
        backerAddress: constructAddress(chainBacking.backerAddress),
        backedAt: { timestamp: BigInt(chainBacking.backedAt) },
        milestoneBacked: BigInt(chainBacking.milestoneBacked),
      };

      const flowerHash = constructPlantHashUsingBlake2b(plant);
      const fruitHash = constructPlantHashUsingBlake2b({
        ...plant,
        isMatured: true,
      });

      if (mintedAssets != null) {
        for (const mphProofOfBacking of mphProofOfBackings) {
          if (mintedAssets[mphProofOfBacking + "." + flowerHash] === 1n) {
            await sql`INSERT INTO chain.proof_of_backing ${sql({
              id: chainBacking.id,
              slot,
              isMatured: false,
              mph: mphProofOfBacking,
              hash: flowerHash,
            })}`;
          }
          if (mintedAssets[mphProofOfBacking + "." + fruitHash] === 1n) {
            await sql`INSERT INTO chain.proof_of_backing ${sql({
              id: chainBacking.id,
              slot,
              isMatured: true,
              mph: mphProofOfBacking,
              hash: fruitHash,
            })}`;
          }
        }
      }
    }

    if (indicies != null) {
      // Since backers can unback and back in the same transaction
      // If there's a CIP-20 message, we will count it towards unbackings if possible; otherwise, backings
      const backings = await driver.store(indicies, (output) => {
        if (output.datum == null) {
          console.warn(
            "datum should be available for backing",
            prettyOutRef(output)
          );
          return undefined;
        }
        const backingDatum = S.fromData(S.fromCbor(output.datum), BackingDatum);

        const backerAddress = deconstructAddress(
          lucid,
          backingDatum.backerAddress
        );
        const projectId = backingDatum.projectId.id;
        const backingAmount = output.value.lovelace;

        const key = `${projectId}|${backerAddress}`;
        let ref = actionRef.get(key);
        if (!ref) {
          ref = { unback: 0n, back: 0n };
          actionRef.set(key, ref);
        }
        ref.back += backingAmount;

        return [
          "backing",
          {
            projectId,
            backerAddress,
            backingAmount,
            milestoneBacked: Number(backingDatum.milestoneBacked),
            backedAt: txTimeStart,
          },
        ];
      });
      if (backings.length)
        await sql`INSERT INTO chain.backing ${sql(backings)}`;
      else console.warn("there is no valid backing");
    }

    for (const [key, value] of actionRef.entries()) {
      const [projectId, actorAddress] = key.split("|");
      const delta = value.back - value.unback;
      let action: BackingActionType;
      if (delta === 0n) action = "claim_rewards";
      else action = delta > 0n ? "back" : "unback";

      backingActions.push({
        action,
        projectId,
        actorAddress,
        amount: delta > 0 ? delta : -delta,
        time, // block time at which executed the action
        message,
        slot,
        txId,
      });
    }
    if (backingActions.length) {
      await sql`INSERT INTO chain.backing_action ${sql(backingActions)}`;
      driver.notify("discord.backing_alert");
    } else console.warn("there is no backing action");

    driver.refresh("views.project_summary");
  }
);

function extractCip20Message(tx: O.TxBabbage): string[] | null {
  const metadatum = tx.metadata?.body?.blob?.["674"];
  if (metadatum != null && "map" in metadatum)
    for (const { k, v } of metadatum.map)
      if ("string" in k && k.string === "msg") {
        assert("list" in v, "674.msg must be a list");
        const result = [];
        for (const e of v.list) {
          assert("string" in e, "674.msg elements must be strings");
          result.push(e.string);
        }
        return result;
      }
  return null;
}
