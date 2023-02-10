import { parseProtocolParams } from "@teiki/protocol/helpers/schema";
import * as S from "@teiki/protocol/schema";
import {
  LegacyProtocolParamsDatum,
  ProtocolParamsDatum,
} from "@teiki/protocol/schema/teiki/protocol";
import { assert } from "@teiki/protocol/utils";

import { LEGACY } from "../../config";
import { PROTOCOL_NFT_TOKEN_NAMES } from "../../constants";
import { $handlers } from "../../framework/chain";
import { prettyOutRef } from "../../framework/chain/conversions";

import { TeikiChainIndexContext } from "./context";

export type Event = { type: "protocol_params"; index: number };
const $ = $handlers<TeikiChainIndexContext, Event>();

export const setup = $.setup(async ({ sql }) => {
  await sql`
    CREATE TABLE IF NOT EXISTS chain.protocol_params (
      id bigint PRIMARY KEY REFERENCES chain.output (id) ON DELETE CASCADE,
      datum_json jsonb NOT NULL
    )
  `;
});

export const initialize = $.initialize(
  async ({ connections: { sql }, context }) => {
    const result = await sql<
      { datumJson: ProtocolParamsDatum | LegacyProtocolParamsDatum }[]
    >`
      -- TODO: Only select one of the current protocol
      SELECT
        pp.datum_json
      FROM
        chain.protocol_params pp
        INNER JOIN chain.output o ON pp.id = o.id
      WHERE
        o.spent_slot IS NULL
    `;
    if (result.length) {
      const row = result[result.length - 1];
      context.projectSponsorshipMinFee = row.datumJson.projectSponsorshipMinFee;
    }
  }
);

export const filter = $.filter(
  ({
    tx,
    context: {
      config: { PROTOCOL_NFT_MPH },
    },
  }) => {
    const index = tx.body.outputs.findIndex(
      ({ value: { assets } }) =>
        assets != null &&
        assets[`${PROTOCOL_NFT_MPH}.${PROTOCOL_NFT_TOKEN_NAMES.PARAMS}`] === 1n
    );
    return index >= 0 ? [{ type: "protocol_params", index }] : null;
  }
);

export const event = $.event(
  async ({ driver, connections: { sql }, event: { index }, context }) => {
    const protocolParams = await driver.store([index], (output) => {
      if (output.datum == null) {
        throw new Error(
          `An protocol params output does not have datum: ${prettyOutRef(
            output
          )}`
        );
      }

      const { legacy, protocolParams } = parseProtocolParams(
        S.fromCbor(output.datum)
      );
      assert(
        legacy === LEGACY,
        `Protocol Params should be a ${LEGACY ? "legacy" : "current"} one`
      );

      const registry = protocolParams.registry;
      const hashes = context.scriptHashes;
      hashes.dedicatedTreasuryV.add(
        registry.dedicatedTreasuryValidator.latest.script.hash
      );
      hashes.sharedTreasuryV.add(
        registry.sharedTreasuryValidator.latest.script.hash
      );
      hashes.openTreasuryV.add(
        registry.openTreasuryValidator.latest.script.hash
      );

      context.staking.register(
        registry.protocolStakingValidator.script.hash,
        "Script"
      );
      context.projectSponsorshipMinFee =
        protocolParams.projectSponsorshipMinFee;

      return ["protocol-params", { datumJson: protocolParams }];
    });
    await sql`INSERT INTO chain.protocol_params ${sql(protocolParams)}`;
  }
);
