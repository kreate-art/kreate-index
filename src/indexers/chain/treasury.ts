import { getAddressDetailsSafe } from "@kreate/protocol/helpers/lucid";
import * as S from "@kreate/protocol/schema";
import { ProtocolParamsDatum } from "@kreate/protocol/schema/teiki/protocol";
import {
  DedicatedTreasuryDatum,
  OpenTreasuryDatum,
  SharedTreasuryDatum,
} from "@kreate/protocol/schema/teiki/treasury";
import { Hex } from "@kreate/protocol/types";

import { $handlers } from "../../framework/chain";
import { prettyOutRef } from "../../framework/chain/conversions";
import { NonEmpty } from "../../types/typelevel";

import { KreateChainIndexContext } from "./context";

export type ChainDedicatedTreasury = { projectId: Hex };

export type ChainSharedTreasury = { projectId: Hex };

export type Event =
  | { type: "dedicated_treasury"; indicies: NonEmpty<number[]> }
  | { type: "shared_treasury"; indicies: NonEmpty<number[]> }
  | { type: "open_treasury"; indicies: NonEmpty<number[]> };

const $ = $handlers<KreateChainIndexContext, Event>();

export const setup = $.setup(async ({ sql }) => {
  await sql`
    CREATE TABLE IF NOT EXISTS chain.dedicated_treasury (
      id bigint PRIMARY KEY REFERENCES chain.output (id) ON DELETE CASCADE,
      project_id varchar(64) NOT NULL,
      governor_ada bigint NOT NULL,
      total_ada bigint NOT NULL
    )
  `;

  await sql`
    CREATE TABLE IF NOT EXISTS chain.shared_treasury (
      id bigint PRIMARY KEY REFERENCES chain.output (id) ON DELETE CASCADE,
      project_id varchar(64) NOT NULL,
      governor_teiki bigint NOT NULL,
      total_teiki bigint NOT NULL
    )
  `;

  await sql`
    CREATE TABLE IF NOT EXISTS chain.open_treasury (
      id bigint PRIMARY KEY REFERENCES chain.output (id) ON DELETE CASCADE,
      governor_ada bigint NOT NULL,
      total_ada bigint NOT NULL
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
    const result = await sql<{ datumJson: ProtocolParamsDatum }[]>`
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
      const governorAda = dedicatedTreasururyDatum.governorAda;
      const totalAda = output.value.lovelace;

      return [
        `dedicated-treasury:${projectId}`,
        { projectId, governorAda, totalAda },
      ];
    });

    if (!dedicatedTreasuries.length) {
      console.warn("there is no valid dedicated treasury");
      return;
    }
    await sql`INSERT INTO chain.dedicated_treasury ${sql(dedicatedTreasuries)}`;
  }
);

export const sharedTreasuryEvent = $.event<"shared_treasury">(
  async ({
    driver,
    connections: { sql },
    event: { indicies },
    context: {
      config: { assetTeiki },
    },
  }) => {
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
      const governorTeiki = sharedTreasuryDatum.governorTeiki;
      const totalTeiki = output.value[assetTeiki.replace(".", "")] ?? 0n;

      return [
        `shared-treasury:${projectId}`,
        { projectId, governorTeiki, totalTeiki },
      ];
    });

    if (!sharedTreasuries.length) {
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

      const openTreasuryDatum = S.fromData(
        S.fromCbor(output.datum),
        OpenTreasuryDatum
      );
      const governorAda = openTreasuryDatum.governorAda;
      const totalAda = output.value.lovelace;

      return ["open-treasury", { governorAda, totalAda }];
    });

    if (!openTreasuries.length) {
      console.warn("there is no valid open treasury");
      return;
    }
    await sql`INSERT INTO chain.open_treasury ${sql(openTreasuries)}`;
  }
);
