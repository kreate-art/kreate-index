import * as S from "@teiki/protocol/schema";
import { ProtocolParamsDatum } from "@teiki/protocol/schema/teiki/protocol";

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

export const filter = $.filter(
  ({
    tx,
    context: {
      config: { authProtocolParams },
    },
  }) => {
    const index = tx.body.outputs.findIndex(
      ({ value: { assets } }) => assets && assets[authProtocolParams] === 1n
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

      const protocolParams = S.fromData(
        S.fromCbor(output.datum),
        ProtocolParamsDatum
      );

      const registry = protocolParams.registry;
      context.staking.watch(
        registry.protocolStakingValidator.script.hash,
        "Script"
      );
      const hashes = context.scriptHashes;
      hashes.dedicatedTreasury.add(
        registry.dedicatedTreasuryValidator.latest.script.hash
      );
      hashes.sharedTreasury.add(
        registry.sharedTreasuryValidator.latest.script.hash
      );
      hashes.openTreasury.add(
        registry.openTreasuryValidator.latest.script.hash
      );

      return ["protocol-params", { datumJson: protocolParams }];
    });
    await sql`INSERT INTO chain.protocol_params ${sql(protocolParams)}`;
  }
);
