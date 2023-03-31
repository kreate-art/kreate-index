import { unsafeMetadatumAsJSON } from "@cardano-ogmios/client";
import { Slot } from "@cardano-ogmios/schema";
import * as L from "lucid-cardano";

import { KOLOURS_CONFIRMATION_SLOTS } from "../../config";
import { Sql } from "../../db";
import { $handlers, ChainIndexCoreDriver } from "../../framework/chain";

import { KreateChainIndexContext } from "./context";

type TokenName = string;
type Meta = Record<TokenName, unknown>;
type NftTransfer = { id: string; address: L.Address };

export type Event =
  | { type: "kolour$mint"; names: TokenName[]; meta: Meta | undefined }
  | { type: "kolour$transfer"; transfers: NftTransfer[] }
  | {
      type: "genesis_kreation$mint";
      names: TokenName[];
      meta: Meta | undefined;
    }
  | { type: "genesis_kreation$transfer"; transfers: NftTransfer[] };
const $ = $handlers<KreateChainIndexContext, Event>();

export const KolourStatuses = [
  "booked",
  "minted", // After the mint is indexed + confirmations
  "expired", // After the tx is expired + confirmations
] as const;

export const KolourSources = ["present", "free", "genesis_kreation"] as const;

export const setup = $.setup(async ({ sql }) => {
  await sql`
    DO $$ BEGIN
      IF to_regtype('kolours.status') IS NULL THEN
        CREATE TYPE kolours.status AS ENUM (${sql.unsafe(
          KolourStatuses.map((a) => `'${a}'`).join(", ")
        )});
      END IF;
    END $$
  `;

  await sql`
    DO $$ BEGIN
      IF to_regtype('kolours.kolour_source') IS NULL THEN
        CREATE TYPE kolours.kolour_source AS ENUM (${sql.unsafe(
          KolourSources.map((a) => `'${a}'`).join(", ")
        )});
      END IF;
    END $$
  `;

  await sql`
    CREATE TABLE IF NOT EXISTS kolours.referral (
      id text PRIMARY KEY,
      pool_id text,
      discount numeric(4, 4) NOT NULL
    )
  `;
  await sql`
    CREATE UNIQUE INDEX IF NOT EXISTS referral_pool_id
      ON kolours.referral(pool_id) WHERE pool_id IS NOT NULL
  `;

  await sql`
    CREATE TABLE IF NOT EXISTS kolours.kolour_book (
      id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
      kolour varchar(6) NOT NULL,
      status kolours.status NOT NULL,
      source kolours.kolour_source NOT NULL,
      source_id text,
      tx_id varchar(64) NOT NULL,
      tx_exp_slot integer NOT NULL,
      tx_exp_time timestamptz NOT NULL,
      fee bigint NOT NULL,
      listed_fee bigint NOT NULL,
      image_cid text NOT NULL,
      user_address text NOT NULL,
      fee_address text NOT NULL,
      referral text,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )
  `;
  await sql`
    CREATE UNIQUE INDEX IF NOT EXISTS kolour_book_tx_kolour_index
      ON kolours.kolour_book(tx_id, kolour)
  `;
  await sql`
    CREATE UNIQUE INDEX IF NOT EXISTS kolour_book_unique_kolour_index
      ON kolours.kolour_book(kolour) WHERE status <> 'expired'
  `;
  await sql`
    CREATE INDEX IF NOT EXISTS kolour_book_user_address_index
      ON kolours.kolour_book(user_address)
  `;
  await sql`
    CREATE INDEX IF NOT EXISTS kolour_book_source_index
      ON kolours.kolour_book(source)
  `;
  await sql`
    CREATE INDEX IF NOT EXISTS kolour_book_tx_exp_index
      ON kolours.kolour_book(tx_exp_slot) WHERE status = 'booked'
  `;

  // Follow chain
  await sql`
    CREATE TABLE IF NOT EXISTS kolours.kolour_mint (
      id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
      kolour varchar(6) NOT NULL,
      slot integer NOT NULL REFERENCES chain.block (slot) ON DELETE CASCADE,
      tx_id varchar(64) NOT NULL,
      metadata jsonb NOT NULL
    )
  `;
  await sql`
    CREATE INDEX IF NOT EXISTS kolour_mint_kolour_index
      ON kolours.kolour_mint(kolour)
  `;
  await sql`
    CREATE INDEX IF NOT EXISTS kolour_mint_slot_index
      ON kolours.kolour_mint(slot)
  `;
  await sql`
    CREATE INDEX IF NOT EXISTS kolour_mint_tx_id_index
      ON kolours.kolour_mint(tx_id)
  `;

  await sql`
    CREATE TABLE IF NOT EXISTS kolours.kolour_free_mint (
      address text PRIMARY KEY,
      quota integer NOT NULL
    )
  `;

  await sql`
    CREATE TABLE IF NOT EXISTS kolours.genesis_kreation_list (
      id integer PRIMARY KEY,
      kreation text UNIQUE,
      slug text UNIQUE,
      initial_image_cid text NOT NULL,
      final_image_cid text NOT NULL,
      listed_fee bigint NOT NULL,
      base_discount numeric(4, 4) NOT NULL,
      attrs jsonb NOT NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )
  `;

  await sql`
    CREATE TABLE IF NOT EXISTS kolours.genesis_kreation_palette (
      id integer PRIMARY KEY,
      kreation_id integer NOT NULL REFERENCES kolours.genesis_kreation_list (id)
        ON DELETE CASCADE ON UPDATE CASCADE,
      kolour varchar(6) NOT NULL,
      layer_image_cid text NOT NULL,
      mask_image_cid text,
      UNIQUE (kreation_id, kolour)
    )
  `;
  await sql`
    CREATE INDEX IF NOT EXISTS genesis_kreation_palette_kolour_index
      ON kolours.genesis_kreation_palette(kolour)
  `;

  await sql`
    CREATE TABLE IF NOT EXISTS kolours.genesis_kreation_book (
      id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
      kreation text NOT NULL,
      status kolours.status NOT NULL,
      tx_id varchar(64) NOT NULL,
      tx_exp_slot integer NOT NULL,
      tx_exp_time timestamptz NOT NULL,
      fee bigint NOT NULL,
      listed_fee bigint NOT NULL,
      name text NOT NULL,
      description text[] NOT NULL,
      user_address text NOT NULL,
      fee_address text NOT NULL,
      referral text,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )
  `;
  await sql`
    CREATE UNIQUE INDEX IF NOT EXISTS genesis_kreation_book_tx_kreation_index
      ON kolours.genesis_kreation_book(tx_id, kreation)
  `;
  await sql`
    CREATE UNIQUE INDEX IF NOT EXISTS genesis_kreation_book_unique_kreation_index
      ON kolours.genesis_kreation_book(kreation) WHERE status <> 'expired'
  `;
  await sql`
    CREATE INDEX IF NOT EXISTS genesis_kreation_book_user_address_index
      ON kolours.genesis_kreation_book(user_address)
  `;
  await sql`
    CREATE INDEX IF NOT EXISTS genesis_kreation_book_tx_exp_index
      ON kolours.genesis_kreation_book(tx_exp_slot) WHERE status = 'booked'
  `;

  // Follow chain
  await sql`
    CREATE TABLE IF NOT EXISTS kolours.genesis_kreation_mint (
      id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
      kreation text NOT NULL,
      slot integer NOT NULL REFERENCES chain.block (slot) ON DELETE CASCADE,
      tx_id varchar(64) NOT NULL,
      metadata jsonb NOT NULL
    )
  `;
  await sql`
    CREATE INDEX IF NOT EXISTS genesis_kreation_kreation_index
      ON kolours.genesis_kreation_mint(kreation)
  `;
  await sql`
    CREATE INDEX IF NOT EXISTS genesis_kreation_mint_slot_index
      ON kolours.genesis_kreation_mint(slot)
  `;
  await sql`
    CREATE INDEX IF NOT EXISTS genesis_kreation_mint_tx_id_index
      ON kolours.genesis_kreation_mint(tx_id)
  `;

  await sql`
    CREATE TABLE IF NOT EXISTS kolours.kolour_trace (
      id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
      kolour varchar(6) NOT NULL,
      address text NOT NULL,
      slot integer NOT NULL REFERENCES chain.block (slot) ON DELETE CASCADE,
      tx_id varchar(64) NOT NULL
    )
  `;
  await sql`
    CREATE INDEX IF NOT EXISTS kolour_trace_kolour_index
      ON kolours.kolour_trace(kolour)
  `;
  await sql`
    CREATE INDEX IF NOT EXISTS kolour_trace_slot_index
      ON kolours.kolour_trace(slot)
  `;

  await sql`
    CREATE TABLE IF NOT EXISTS kolours.genesis_kreation_trace (
      id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
      kreation text NOT NULL,
      address text NOT NULL,
      slot integer NOT NULL REFERENCES chain.block (slot) ON DELETE CASCADE,
      tx_id varchar(64) NOT NULL
    )
  `;
  await sql`
    CREATE INDEX IF NOT EXISTS genesis_kreation_trace_kreation_index
      ON kolours.genesis_kreation_trace(kreation)
  `;
  await sql`
    CREATE INDEX IF NOT EXISTS genesis_kreation_trace_slot_index
      ON kolours.genesis_kreation_trace(slot)
  `;
});

export async function confirm(
  sql: Sql,
  slot: Slot,
  driver: ChainIndexCoreDriver
) {
  const confirmSlot = slot - KOLOURS_CONFIRMATION_SLOTS;
  const [kolourUpdatedCount, genesisKreationUpdatedCount] = await Promise.all([
    confirmKolourBook(sql, confirmSlot),
    confirmGenesisKreationBook(sql, confirmSlot),
  ]);
  if (kolourUpdatedCount) driver.notify("discord.kolour_nft_alert");
  if (genesisKreationUpdatedCount)
    driver.notify("discord.genesis_kreation_nft_alert");
}

// Returns the number of updated rows
async function confirmKolourBook(sql: Sql, confirmSlot: Slot) {
  const updatedToMinted = await sql`
    UPDATE
      kolours.kolour_book kb
    SET
      status = 'minted'
    FROM
      kolours.kolour_mint km
    WHERE
      kb.status = 'booked'
      AND km.tx_id = kb.tx_id
      AND km.slot <= ${confirmSlot}
  `;
  const updatedToExpired = await sql`
    UPDATE
      kolours.kolour_book kb
    SET
      status = 'expired'
    WHERE
      kb.status = 'booked'
      AND kb.tx_exp_slot <= ${confirmSlot}
  `;
  return updatedToMinted.count + updatedToExpired.count;
}

// Returns the number of updated rows
async function confirmGenesisKreationBook(sql: Sql, confirmSlot: Slot) {
  const updatedToMinted = await sql`
    UPDATE
      kolours.genesis_kreation_book gb
    SET
      status = 'minted'
    FROM
      kolours.genesis_kreation_mint gm
    WHERE
      gb.status = 'booked'
      AND gm.tx_id = gb.tx_id
      AND gm.slot <= ${confirmSlot}
  `;
  const updatedToExpired = await sql`
    UPDATE
      kolours.genesis_kreation_book gb
    SET
      status = 'expired'
    WHERE
      gb.status = 'booked'
      AND gb.tx_exp_slot <= ${confirmSlot}
  `;
  return updatedToMinted.count + updatedToExpired.count;
}

export const filter = $.filter(
  ({
    tx,
    context: {
      config: { kolourNftPolicyId, genesisKreationNftPolicyId },
    },
  }) => {
    const events: Event[] = [];

    const minted = tx.body.mint.assets;
    if (minted) {
      const kolourNfts: TokenName[] = [];
      const genesisKreationNfts: TokenName[] = [];
      for (const [asset, amount] of Object.entries(minted)) {
        if (amount <= 0) continue;
        const [mph, tn] = asset.split(".");
        if (mph === kolourNftPolicyId) kolourNfts.push(L.toText(tn));
        else if (mph === genesisKreationNftPolicyId)
          genesisKreationNfts.push(L.toText(tn));
      }

      if (kolourNfts.length || genesisKreationNfts.length) {
        const metadatum = tx.metadata?.body?.blob?.["721"];
        let metadatumJson;
        if (metadatum) {
          metadatumJson = unsafeMetadatumAsJSON(metadatum);
        } else {
          metadatumJson = undefined;
          console.warn("Metadatum 721 should be available for NFTs");
        }
        kolourNfts.length &&
          events.push({
            type: "kolour$mint",
            names: kolourNfts,
            meta: metadatumJson?.[kolourNftPolicyId],
          });
        genesisKreationNfts.length &&
          events.push({
            type: "genesis_kreation$mint",
            names: genesisKreationNfts,
            meta: metadatumJson?.[genesisKreationNftPolicyId],
          });
      }
    }

    const kolourTransfers: NftTransfer[] = [];
    const genesisKreationTransfers: NftTransfer[] = [];
    for (const output of tx.body.outputs) {
      const assets = output.value.assets;
      if (assets == null) continue;
      const address = output.address;
      for (const asset of Object.keys(assets)) {
        const [mph, tn] = asset.split(".");
        if (mph === kolourNftPolicyId) {
          const id = L.toText(tn).split("#", 2)[1];
          kolourTransfers.push({ id, address });
        } else if (mph === genesisKreationNftPolicyId) {
          const id = L.toText(tn);
          genesisKreationTransfers.push({ id, address });
        }
      }
    }
    kolourTransfers.length &&
      events.push({
        type: "kolour$transfer",
        transfers: kolourTransfers,
      });
    genesisKreationTransfers.length &&
      events.push({
        type: "genesis_kreation$transfer",
        transfers: genesisKreationTransfers,
      });

    return events;
  }
);

export const kolourEvent$Mint = $.event<"kolour$mint">(
  async ({
    driver,
    connections: { sql },
    block: { slot },
    tx: { id },
    event: { names, meta },
  }) => {
    const mints = names.map((token) => {
      return {
        kolour: token.split("#", 2)[1],
        slot,
        txId: id,
        metadata: meta?.[token] ?? {},
      };
    });
    await sql`INSERT INTO kolours.kolour_mint ${sql(mints)}`;
    driver.notify("discord.kolour_nft_alert");
  }
);

export const genesisKreationEvent$Mint = $.event<"genesis_kreation$mint">(
  async ({
    driver,
    connections: { sql },
    block: { slot },
    tx: { id },
    event: { names, meta },
  }) => {
    const mints = names.map((token) => {
      return {
        kreation: token,
        slot,
        txId: id,
        metadata: meta?.[token] ?? {},
      };
    });
    await sql`INSERT INTO kolours.genesis_kreation_mint ${sql(mints)}`;
    driver.notify("discord.genesis_kreation_nft_alert");
  }
);

export const kolourEvent$Transfer = $.event<"kolour$transfer">(
  async ({
    connections: { sql },
    block: { slot },
    tx: { id },
    event: { transfers },
  }) => {
    const records = transfers.map((item) => ({
      kolour: item.id,
      address: item.address,
      slot,
      txId: id,
    }));
    await sql`INSERT INTO kolours.kolour_trace ${sql(records)}`;
  }
);

export const genesisKreationEvent$Transfer =
  $.event<"genesis_kreation$transfer">(
    async ({
      connections: { sql },
      block: { slot },
      tx: { id },
      event: { transfers },
    }) => {
      const records = transfers.map((item) => ({
        kreation: item.id,
        address: item.address,
        slot,
        txId: id,
      }));
      await sql`INSERT INTO kolours.genesis_kreation_trace ${sql(records)}`;
    }
  );
