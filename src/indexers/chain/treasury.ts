import { getAddressDetailsSafe } from "@teiki/protocol/helpers/lucid";
import * as S from "@teiki/protocol/schema";
import {
  ProtocolParamsDatum,
  LegacyProtocolParamsDatum,
} from "@teiki/protocol/schema/teiki/protocol";
import {
  DedicatedTreasuryDatum,
  SharedTreasuryDatum,
} from "@teiki/protocol/schema/teiki/treasury";
import { Hex } from "@teiki/protocol/types";

import { $handlers } from "../../framework/chain";
import { prettyOutRef } from "../../framework/chain/conversions";
import { NonEmpty } from "../../types/typelevel";

import { TeikiChainIndexContext } from "./context";

export type ChainDedicatedTreasury = { projectId: Hex };

export type ChainSharedTreasury = { projectId: Hex };

export type Event =
  | { type: "dedicated_treasury"; indicies: NonEmpty<number[]> }
  | { type: "shared_treasury"; indicies: NonEmpty<number[]> }
  | { type: "open_treasury"; indicies: NonEmpty<number[]> };

const $ = $handlers<TeikiChainIndexContext, Event>();

export const setup = $.setup(async ({ sql }) => {
  await sql`
    CREATE TABLE IF NOT EXISTS chain.dedicated_treasury (
      id bigint PRIMARY KEY REFERENCES chain.output (id) ON DELETE CASCADE,
      project_id varchar(64) NOT NULL
    )
  `;

  await sql`
    CREATE TABLE IF NOT EXISTS chain.shared_treasury (
      id bigint PRIMARY KEY REFERENCES chain.output (id) ON DELETE CASCADE,
      project_id varchar(64) NOT NULL
    )
  `;

  await sql`
    CREATE TABLE IF NOT EXISTS chain.open_treasury (
      id bigint PRIMARY KEY REFERENCES chain.output (id) ON DELETE CASCADE
    )
  `;
});

export const initialize = $.initialize(
  async ({
    connections: { sql },
    context: {
      scriptHashes: { dedicatedTreasury, sharedTreasury, openTreasury },
    },
  }) => {
    const result = await sql<
      { datumJson: ProtocolParamsDatum | LegacyProtocolParamsDatum }[]
    >`
      SELECT datum_json FROM chain.protocol_params
    `;

    for (const row of result) {
      const registry = row.datumJson.registry;
      dedicatedTreasury.add(
        registry.dedicatedTreasuryValidator.latest.script.hash
      );
      sharedTreasury.add(registry.sharedTreasuryValidator.latest.script.hash);
      openTreasury.add(registry.openTreasuryValidator.latest.script.hash);
    }
  }
);

export const filter = $.filter(
  ({
    tx,
    context: {
      scriptHashes: { dedicatedTreasury, sharedTreasury, openTreasury },
    },
  }) => {
    const dedicatedTreasuryIndicies: number[] = [];
    const sharedTreasuryIndicies: number[] = [];
    const openTreasuryIndicies: number[] = [];

    for (const [index, { address }] of tx.body.outputs.entries()) {
      const scriptHash =
        getAddressDetailsSafe(address)?.paymentCredential?.hash;
      if (!scriptHash) continue;

      if (dedicatedTreasury.has(scriptHash))
        dedicatedTreasuryIndicies.push(index);
      else if (sharedTreasury.has(scriptHash))
        sharedTreasuryIndicies.push(index);
      else if (openTreasury.has(scriptHash)) openTreasuryIndicies.push(index);
    }

    const events: Event[] = [];
    if (dedicatedTreasuryIndicies.length)
      events.push({
        type: "dedicated_treasury",
        indicies: dedicatedTreasuryIndicies,
      });
    if (sharedTreasuryIndicies.length)
      events.push({
        type: "shared_treasury",
        indicies: sharedTreasuryIndicies,
      });
    if (openTreasuryIndicies.length)
      events.push({ type: "open_treasury", indicies: openTreasuryIndicies });

    return events;
  }
);

export const dedicatedTreasuryEvent = $.event<"dedicated_treasury">(
  async ({ driver, connections: { sql }, event: { indicies } }) => {
    const dedicatedTreasuries = await driver.store(indicies, (output) => {
      if (output.datum == null) {
        console.warn(
          "datum should be available for dedicated treasuries",
          prettyOutRef(output)
        );
        return undefined;
      }

      const dedicatedTreasururyDatum = S.fromData(
        S.fromCbor(output.datum),
        DedicatedTreasuryDatum
      );
      const projectId = dedicatedTreasururyDatum.projectId.id;

      return [`dedicated-treasury:${projectId}`, { projectId }];
    });

    if (!dedicatedTreasuries) {
      console.warn("there is no valid dedicated treasury");
      return;
    }

    await sql`INSERT INTO chain.dedicated_treasury ${sql(dedicatedTreasuries)}`;
  }
);

export const sharedTreasuryEvent = $.event<"shared_treasury">(
  async ({ driver, connections: { sql }, event: { indicies } }) => {
    const sharedTreasuries = await driver.store(indicies, (output) => {
      if (output.datum == null) {
        console.warn(
          "datum should be available for shared treasuries",
          prettyOutRef(output)
        );
        return undefined;
      }

      const sharedTreasuryDatum = S.fromData(
        S.fromCbor(output.datum),
        SharedTreasuryDatum
      );
      const projectId = sharedTreasuryDatum.projectId.id;

      return [`shared-treasury:${projectId}`, { projectId }];
    });

    if (!sharedTreasuries) {
      console.warn("there is no valid shared treasury");
      return;
    }

    await sql`INSERT INTO chain.shared_treasury ${sql(sharedTreasuries)}`;
  }
);

export const openTreasuryEvent = $.event<"open_treasury">(
  async ({ driver, connections: { sql }, event: { indicies } }) => {
    const openTreasuries = await driver.store(indicies, (output) => {
      if (output.datum == null) {
        console.warn(
          "datum should be available for open treasuries",
          prettyOutRef(output)
        );
        return undefined;
      }

      return ["open-treasury", {}];
    });

    if (!openTreasuries) {
      console.warn("there is no valid open treasury");
      return;
    }

    await sql`INSERT INTO chain.open_treasury ${sql(openTreasuries)}`;
  }
);
