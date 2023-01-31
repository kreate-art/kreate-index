import { TEIKI_PLANT_NFT_TOKEN_NAME } from "@teiki/protocol/contracts/common/constants";

import { $handlers } from "../../framework/chain";
import { prettyOutRef } from "../../framework/chain/conversions";

import { TeikiChainIndexContext } from "./context";

export type Event = { type: "teiki_plant"; index: number };
const $ = $handlers<TeikiChainIndexContext, Event>();

export const setup = $.setup(async ({ sql }) => {
  await sql`
    CREATE TABLE IF NOT EXISTS chain.teiki_plant (
      id bigint PRIMARY KEY REFERENCES chain.output (id) ON DELETE CASCADE
    )
  `;
});

export const filter = $.filter(
  ({
    tx,
    context: {
      config: { TEIKI_PLANT_NFT_MPH },
    },
  }) => {
    const index = tx.body.outputs.findIndex(
      ({ value: { assets } }) =>
        assets != null &&
        assets[`${TEIKI_PLANT_NFT_MPH}.${TEIKI_PLANT_NFT_TOKEN_NAME}`] === 1n
    );
    return index >= 0 ? [{ type: "teiki_plant", index }] : null;
  }
);

export const event = $.event(
  async ({ driver, connections: { sql }, event: { index } }) => {
    const teikiPlant = await driver.store([index], (output) => {
      if (output.datum == null) {
        throw new Error(
          `A teiki plant output does not have datum: ${prettyOutRef(output)}`
        );
      }

      return ["teiki-plant", {}];
    });
    await sql`INSERT INTO chain.teiki_plant ${sql(teikiPlant)}`;
  }
);
