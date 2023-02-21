import * as O from "@cardano-ogmios/schema";
import { Address, TxHash } from "lucid-cardano";

import { deconstructAddress } from "@teiki/protocol/helpers/schema";
import * as S from "@teiki/protocol/schema";
import { BackingDatum } from "@teiki/protocol/schema/teiki/backing";
import { Hex, UnixTime } from "@teiki/protocol/types";
import { assert } from "@teiki/protocol/utils";

import { $handlers } from "../../framework/chain";
import { prettyOutRef } from "../../framework/chain/conversions";
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

const ActionTypes = ["back", "unback", "claim_rewards", "migrate"] as const;

export type ChainBackingAction = {
  action: (typeof ActionTypes)[number];
  projectId: Hex;
  actorAddress: Address;
  amount: Lovelace;
  time: UnixTime;
  message: string | null;
  slot: number;
  txId: TxHash;
};

export type Event = { type: "backing"; indicies: NonEmpty<number[]> | null };
const $ = $handlers<TeikiChainIndexContext, Event>();

export const setup = $.setup(async ({ sql }) => {
  // TODO: Rename staked_at => backed_at in contracts
  // TODO: Rename unstaked_at => unbacked_at in contracts
  // TODO: Rename unstake => unback in contracts
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
          Object.values(ActionTypes)
            .map((a) => `'${a}'`)
            .join(", ")
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
});

export const filter = $.filter(
  ({
    tx,
    context: {
      config: {
        assetsProofOfBacking: { seed, wilted },
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
      return [{ type: "backing", indicies: backingIndicies }];
    } else {
      const minted = tx.body.mint.assets;
      if (minted) {
        const isMinted = (a: string) => minted[a];
        if (seed.some(isMinted) || wilted.some(isMinted))
          return [{ type: "backing", indicies: null }];
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
    event: { indicies },
  }) => {
    const txTimeStart = slotTimeInterpreter.slotToAbsoluteTime(
      tx.body.validityInterval.invalidBefore ?? slot
    );
    const message = extractCip20Message(tx)?.join("\n") || null;

    const unbackResult = await sql<
      { projectId: Hex; totalAmount: Lovelace; actorAddress: Address }[]
    >`
      SELECT
        SUM(b.backing_amount)::bigint AS total_amount,
        b.project_id,
        b.backer_address AS actor_address
      FROM chain.backing b
      INNER JOIN chain.output o ON b.id = o.id
      WHERE (o.tx_id, o.tx_ix) IN ${sql(
        tx.body.inputs.map((input) => sql([input.txId, input.index])) // @sk-shishi
      )}
      GROUP BY
        (b.project_id, b.backer_address)
    `;

    const actionRef = new Map<string, { unback: Lovelace; back: Lovelace }>();
    unbackResult.forEach((value) => {
      const tupleKey = `${value.projectId}|${value.actorAddress}`;
      actionRef.set(tupleKey, {
        unback: value.totalAmount,
        back: 0n,
      });
    });

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
        const tupleKey = `${projectId}|${backerAddress}`;
        let ref = actionRef.get(tupleKey);
        if (ref == null) {
          ref = { unback: 0n, back: 0n };
          actionRef.set(tupleKey, ref);
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
      if (!backings.length) console.warn("there is no valid backing");
      else await sql`INSERT INTO chain.backing ${sql(backings)}`;
    }

    for (const [rawKey, value] of actionRef.entries()) {
      const [projectId, actorAddress] = rawKey.split("|");
      const delta = value.back - value.unback;
      const action = delta > 0n ? "back" : "unback";
      const backingAction: ChainBackingAction = {
        action,
        projectId,
        actorAddress,
        amount: delta > 0 ? delta : -delta,
        time, // block time at which executed the action
        message,
        slot,
        txId: tx.id,
      };
      await sql`INSERT INTO chain.backing_action ${sql(backingAction)}`;
    }
    driver.refresh("views.project_summary");
  }
);

function extractCip20Message(tx: O.TxBabbage): string[] | null {
  const metadatum = tx.metadata?.body?.blob?.["674"];
  if (metadatum != null && "map" in metadatum)
    for (const { k, v } of metadatum.map)
      if ("string" in k && k.string === "msg") {
        assert("list" in v, "374.msg must be a list");
        const result = [];
        for (const e of v.list) {
          assert("string" in e, "374.msg elements must be strings");
          result.push(e.string);
        }
        return result;
      }
  return null;
}
