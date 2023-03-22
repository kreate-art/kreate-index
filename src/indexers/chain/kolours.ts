import { $handlers } from "../../framework/chain";

import { KreateChainIndexContext } from "./context";

export type Event = { type: "" };
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
    CREATE TABLE IF NOT EXISTS kolours.kolour_book (
      id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
      kolour varchar(6) NOT NULL,
      status kolours.status NOT NULL,
      tx_id varchar(64) NOT NULL,
      tx_exp_slot integer NOT NULL,
      tx_exp_time timestamptz NOT NULL,
      fee bigint NOT NULL,
      image_cid text NOT NULL,
      user_address text NOT NULL,
      fee_address text NOT NULL,
      referral text
    )
  `;
  await sql`
    CREATE UNIQUE INDEX IF NOT EXISTS kolour_book_unique_kolour_index
      ON kolours.kolour_book(status) WHERE status <> 'expired'
  `;
  await sql`
    CREATE UNIQUE INDEX IF NOT EXISTS kolour_book_tx_kolour_index
      ON kolours.kolour_book(tx_id, kolour)
  `;
  await sql`
    CREATE TABLE IF NOT EXISTS kolours.kolour_mint (
      id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
      kolour varchar(6) UNIQUE,
      slot integer NOT NULL REFERENCES chain.block (slot) ON DELETE CASCADE,
      tx_id varchar(64) NOT NULL,
      metadata jsonb NOT NULL
    )
  `;
});
