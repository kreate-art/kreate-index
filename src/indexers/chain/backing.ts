import * as O from "@cardano-ogmios/schema";
import { Address } from "lucid-cardano";

import { deconstructAddress } from "@teiki/protocol/helpers/schema";
import { fromJson, toJson } from "@teiki/protocol/json";
import * as S from "@teiki/protocol/schema";
import { BackingDatum } from "@teiki/protocol/schema/teiki/backing";
import { Hex, UnixTime } from "@teiki/protocol/types";
import { assert } from "@teiki/protocol/utils";

import { $handlers } from "../../framework/chain";
import { prettyOutRef, slotFrom } from "../../framework/chain/conversions";
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

const AllAction = {
  0: "back",
  1: "unback",
  2: "claim_reward", // to be supported
  3: "migrate", // to be supported
};

export type ChainBackingAction = {
  projectId: Hex;
  actorAddress: Address;
  amount: Lovelace;
  message: string | null;
  time: UnixTime;
  slot: number;
  action: keyof typeof AllAction;
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
      backing_message text,
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
    CREATE TABLE IF NOT EXISTS chain.backing_action (
      id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
      project_id varchar(64) NOT NULL,
      actor_address text NOT NULL,
      amount bigint NOT NULL,
      message text,
      time timestamptz NOT NULL,
      slot integer NOT NULL,
      action smallint
    )
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

type BackingDiff = {
  unback: Lovelace;
  back: Lovelace;
};

type UnbackResult = {
  projectId: Hex;
  totalAmount: Lovelace;
  actorAddress: Address;
};

type BackingTupleKey = [Hex, Address];

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

    const unbackResult = await sql<UnbackResult[]>`
      SELECT
        SUM(b.backing_amount) AS total_amount,
        b.project_id,
        b.backer_address AS actor_address
      FROM chain.backing b
      INNER JOIN chain.output o ON b.id = o.id
      WHERE (o.tx_id, o.tx_ix) IN ${sql(
        tx.body.inputs.map((input) => sql([input.txId, input.index]))
      )}
      GROUP BY
        (b.project_id, b.backer_address)
    `;

    // BackingTupleKey
    const actionRef: Map<string, BackingDiff> = new Map<string, BackingDiff>();
    unbackResult.forEach((value) => {
      const key: BackingTupleKey = [value.projectId, value.actorAddress];
      actionRef.set(toJson(key), {
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
        const tupleKey: BackingTupleKey = [projectId, backerAddress];
        const res = actionRef.get(toJson(tupleKey));

        if (res) {
          const oldBacking = res.back;
          const tupleKey: BackingTupleKey = [projectId, backerAddress];
          actionRef.set(toJson(tupleKey), {
            unback: res.unback,
            back: oldBacking + output.value.lovelace,
          });
        } else {
          actionRef.set(toJson(tupleKey), {
            unback: 0n,
            back: output.value.lovelace,
          });
        }

        return [
          "backing",
          {
            projectId,
            backerAddress,
            backingAmount: output.value.lovelace,
            milestoneBacked: Number(backingDatum.milestoneBacked),
            backingMessage: message,
            backedAt: txTimeStart,
          },
        ];
      });
      if (!backings.length) console.warn("there is no valid backing");
      else await sql`INSERT INTO chain.backing ${sql(backings)}`;
    }

    for (const [_key, value] of actionRef) {
      const key: BackingTupleKey = fromJson(_key);
      const delta = BigInt(value.back) - BigInt(value.unback);
      const action = delta > 0n ? 0 : 1; // back vs unback
      const backerAction: ChainBackingAction = {
        projectId: key[0],
        actorAddress: key[1],
        amount: delta > 0 ? delta : -delta,
        message,
        time, // block time at which executed the action
        slot,
        action,
      };
      await sql`INSERT INTO chain.backing_action ${sql(backerAction)}`;
    }
    driver.refresh("views.project_summary");
  }
);

export const rollback = $.rollback(
  async ({ connections: { sql, views }, point }) => {
    await sql`
      DELETE FROM chain.backing_action ba
        WHERE ba.slot > ${slotFrom(point)}
    `;
    views.refresh("views.project_summary");
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
