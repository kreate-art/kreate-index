import * as O from "@cardano-ogmios/schema";
import { Address } from "lucid-cardano";

import { PROOF_OF_BACKING_TOKEN_NAMES } from "@teiki/protocol/contracts/common/constants";
import { deconstructAddress } from "@teiki/protocol/helpers/schema";
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
  backedAt: UnixTime;
  backingMessage: string | null;
  unbackedAt: UnixTime | null;
  unbackingMessage: string | null;
};

export type Event = { type: "backing"; indicies: NonEmpty<number[]> | null };
const $ = $handlers<TeikiChainIndexContext, Event>();

export const setup = $.setup(async ({ sql }) => {
  // TODO: Rename staked_at => backed_at in contracts
  // TODO: Rename unstake => unback in contracts
  await sql`
    CREATE TABLE IF NOT EXISTS chain.backing (
      id bigint PRIMARY KEY REFERENCES chain.output (id) ON DELETE CASCADE,
      project_id varchar(64) NOT NULL,
      backer_address text NOT NULL,
      backing_amount bigint NOT NULL,
      milestone_backed smallint NOT NULL,
      backed_at timestamptz NOT NULL,
      backing_message text,
      unbacked_at timestamptz,
      unbacking_message text
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
});

export const filter = $.filter(
  ({
    tx,
    context: {
      config: { PROOF_OF_BACKING_MPH },
    },
  }) => {
    const backingIndicies: number[] = [];
    const seedUnit = `${PROOF_OF_BACKING_MPH}.${PROOF_OF_BACKING_TOKEN_NAMES.SEED}`;
    const wiltedUnit = `${PROOF_OF_BACKING_MPH}.${PROOF_OF_BACKING_TOKEN_NAMES.WILTED_FLOWER}`;

    for (const [index, output] of tx.body.outputs.entries()) {
      const assets = output.value.assets;
      if (assets != null && assets[seedUnit] === 1n)
        backingIndicies.push(index);
    }

    const events: Event[] = [];
    if (backingIndicies.length) {
      events.push({ type: "backing", indicies: backingIndicies });
    } else {
      const mintedAssets = tx.body.mint.assets;
      if (
        mintedAssets != null &&
        (mintedAssets[seedUnit] != null || mintedAssets[wiltedUnit] != null)
      )
        events.push({ type: "backing", indicies: null });
    }
    return events;
  }
);

export const event = $.event(
  async ({
    driver,
    connections: { sql, lucid, slotTimeInterpreter },
    block: { slot },
    tx,
    event: { indicies },
  }) => {
    const unbackingMessage = extractCip20Message(tx)?.join("\n") || null;
    const unbackedAt = slotTimeInterpreter.slotToAbsoluteTime(
      tx.body.validityInterval.invalidBefore ?? slot
    );
    const unbackResult = await sql`
      UPDATE chain.backing b
      SET unbacked_at = ${unbackedAt},
          unbacking_message = ${unbackingMessage}
      FROM chain.output o
      WHERE
        o.id = b.id
        AND (o.tx_id, o.tx_ix) IN ${sql(
          tx.body.inputs.map((input) => sql([input.txId, input.index]))
        )}
    `;
    if (indicies != null) {
      // Since backers can unback and back in the same transaction
      // If there's a CIP-20 message, we will count it towards unbackings if possible; otherwise, backings
      const backingMessage = unbackResult.count ? null : unbackingMessage;
      const backings = await driver.store(indicies, (output) => {
        if (output.datum == null) {
          console.warn(
            "datum should be available for backing",
            prettyOutRef(output)
          );
          return undefined;
        }
        const backingDatum = S.fromData(S.fromCbor(output.datum), BackingDatum);
        return [
          "backing",
          {
            projectId: backingDatum.projectId.id,
            backerAddress: deconstructAddress(
              lucid,
              backingDatum.backerAddress
            ),
            backingAmount: output.value.lovelace,
            milestoneBacked: Number(backingDatum.milestoneBacked),
            backedAt: Number(backingDatum.stakedAt.timestamp),
            backingMessage,
          },
        ];
      });
      await sql`INSERT INTO chain.backing ${sql(backings)}`;
    }
    driver.refresh("views.project_summary");
  }
);

export const rollback = $.rollback(
  async ({ connections: { sql, views }, point }) => {
    await sql`
      UPDATE chain.backing b
      SET unbacked_at = NULL,
          unbacking_message = NULL
      FROM chain.output o
      WHERE
        o.id = b.id
        AND o.spent_slot > ${slotFrom(point)}
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
