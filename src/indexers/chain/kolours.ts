import * as L from "lucid-cardano";

import { $handlers } from "../../framework/chain";

import { KreateChainIndexContext } from "./context";

export type Event = { type: "kolour_nft" } | { type: "genesis_kreation_nft" };
const $ = $handlers<KreateChainIndexContext, Event>();

export const KolourStatuses = [
  "booked",
  "minted", // After the mint is indexed + confirmation
  "expired", // After the tx is expired + confirmation
] as const;

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
    CREATE TABLE IF NOT EXISTS kolours.referral (
      code text PRIMARY KEY,
      discount numeric(4, 4) NOT NULL
    )
  `;

  await sql`
    CREATE TABLE IF NOT EXISTS kolours.kolour_book (
      id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
      kolour varchar(6) NOT NULL,
      status kolours.status NOT NULL,
      tx_id varchar(64) NOT NULL,
      tx_exp_slot integer NOT NULL,
      tx_exp_time timestamptz NOT NULL,
      fee bigint NOT NULL,
      listed_fee bigint NOT NULL,
      image_cid text NOT NULL,
      user_address text NOT NULL,
      fee_address text NOT NULL,
      referral text
    )
  `;
  await sql`
    CREATE UNIQUE INDEX IF NOT EXISTS kolour_book_unique_kolour_index
      ON kolours.kolour_book(kolour) WHERE status <> 'expired'
  `;
  await sql`
    CREATE UNIQUE INDEX IF NOT EXISTS kolour_book_tx_kolour_index
      ON kolours.kolour_book(tx_id, kolour)
  `;

  // Follow chain
  await sql`
    CREATE TABLE IF NOT EXISTS kolours.kolour_mint (
      id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
      kolour varchar(6) UNIQUE,
      slot integer NOT NULL REFERENCES chain.block (slot) ON DELETE CASCADE,
      tx_id varchar(64) NOT NULL,
      metadata jsonb NOT NULL
    )
  `;

  await sql`
    CREATE TABLE IF NOT EXISTS kolours.genesis_kreation_list (
      id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
      kreation text UNIQUE,
      initial_image_cid text NOT NULL,
      final_image_cid text NOT NULL,
      listed_fee bigint NOT NULL,
      -- [{k(olour): kolour, l(ayer): cid}] Because array handling with this lib is dumb
      palette jsonb NOT NULL,
      attrs jsonb NOT NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
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
      user_address text NOT NULL,
      fee_address text NOT NULL,
      referral text
    )
  `;
  await sql`
    CREATE UNIQUE INDEX IF NOT EXISTS genesis_kreation_book_unique_kreation_index
      ON kolours.genesis_kreation_book(kreation) WHERE status <> 'expired'
  `;
  await sql`
    CREATE UNIQUE INDEX IF NOT EXISTS genesis_kreation_book_tx_kreation_index
      ON kolours.genesis_kreation_book(tx_id, kreation)
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
});

// Only index minting NFT
export const filter = $.filter(
  ({
    tx,
    context: {
      config: { kolourNftMph, genesisKreactionNftMph },
    },
  }) => {
    const minted = tx.body.mint.assets;
    if (minted) {
      for (const [asset, amount] of Object.entries(minted)) {
        const [mph, _] = asset.split(".");
        if (mph === kolourNftMph && amount > 0) return [{ type: "kolour_nft" }];
        if (mph === genesisKreactionNftMph && amount > 0)
          return [{ type: "genesis_kreation_nft" }];
      }
    } else {
      return null;
    }
    return null;
  }
);

export const kolourNftevent = $.event(
  async ({
    connections: { sql },
    block: { slot },
    tx,
    context: {
      config: { kolourNftMph },
    },
  }) => {
    const txId = tx.id;
    const minted = tx.body.mint.assets;
    if (!minted) return;

    const metadatum = tx.metadata?.body?.blob?.["721"];
    if (!metadatum) return;

    const kolourMinted = [];
    for (const [asset, amount] of Object.entries(minted)) {
      if (amount < 0) continue;
      const [mph, tokenName] = asset.split(".");
      if (mph === kolourNftMph) {
        kolourMinted.push({
          kolour: L.toText(tokenName).split("#")[1],
          slot,
          txId,
          metadata: metadatum,
        });
      }
    }

    if (kolourMinted.length)
      await sql`INSERT INTO kolours.kolour_mint ${sql(kolourMinted)}`;
  }
);

export const genesisKreationNftevent = $.event(
  async ({
    connections: { sql },
    block: { slot },
    tx,
    context: {
      config: { genesisKreactionNftMph },
    },
  }) => {
    const txId = tx.id;
    const minted = tx.body.mint.assets;
    if (!minted) return;

    const metadatum = tx.metadata?.body?.blob?.["721"];
    if (!metadatum) return;

    const genesisKreationMinted = [];
    for (const [asset, amount] of Object.entries(minted)) {
      if (amount < 0) continue;
      const [mph, tokenName] = asset.split(".");
      if (mph === genesisKreactionNftMph) {
        genesisKreationMinted.push({
          gkId: L.toText(tokenName),
          slot,
          txId,
          metadata: metadatum,
        });
      }
    }

    if (genesisKreationMinted.length)
      await sql`INSERT INTO kolours.genesis_kreation_mint ${sql(
        genesisKreationMinted
      )}`;
  }
);
