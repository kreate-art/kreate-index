import { ConnectionConfig } from "@cardano-ogmios/client";
import * as O from "@cardano-ogmios/schema";
import dotenv from "dotenv";

import { assert } from "@teiki/protocol/utils";

dotenv.config();

function requiredEnv(key: string): string {
  const value = process.env[key];
  if (value) return value;
  else throw new Error(`${key} must be set`);
}

// TODO: Env vars validation

export type Env = "development" | "staging" | "testnet";
export type Network = "preview" | "preprod" | "mainnet";

export const ENV = (process.env.ENV || "development") as Env;

export const LEGACY = Boolean(Number(process.env.LEGACY || 0));

export function pick<T extends Record<string, unknown>, K extends keyof T>(
  base: T,
  ...keys: K[]
): { [P in K]: T[P] } {
  const entries = keys.map((key) => [key, base[key]]);
  return Object.fromEntries(entries);
}

export function cardano() {
  const network = requiredEnv("NETWORK");
  assert(
    network === "preview" || network === "preprod" || network === "mainnet",
    "Network must be either: preview, preprod, mainnet."
  );
  return { NETWORK: network as Network };
}

export function database() {
  return {
    DATABASE_URL: requiredEnv("DATABASE_URL"),
    DATABASE_MAX_CONNECTIONS: Number(process.env.DATABASE_MAX_CONNECTIONS || 8),
  };
}

export function ogmios() {
  return {
    host: requiredEnv("OGMIOS_HOST"),
    port: parseInt(requiredEnv("OGMIOS_PORT")),
    // TODO: Add OGMIOS_TLS, or better, parse from a single env
  } as ConnectionConfig;
}

export function ipfs() {
  return {
    IPFS_SERVER_URL: requiredEnv("IPFS_SERVER_URL"),
    IPFS_SERVER_TIMEOUT: Number(process.env.IPFS_SERVER_TIMEOUT || 30_000),
  };
}

export function discord() {
  return {
    DISCORD_BOT_ID: requiredEnv("DISCORD_BOT_ID"),
    DISCORD_BOT_TOKEN: requiredEnv("DISCORD_BOT_TOKEN"),
    DISCORD_CONTENT_MODERATION_CHANNEL_ID: requiredEnv(
      "DISCORD_CONTENT_MODERATION_CHANNEL_ID"
    ),
    DISCORD_SHINKA_ROLE_ID: requiredEnv("DISCORD_SHINKA_ROLE_ID"),
  };
}

// TODO: Remove these...
export function chainIndex() {
  return {
    CHAIN_INDEX_BEGIN: parseChainIndexStart(requiredEnv("CHAIN_INDEX_BEGIN")),
    CHAIN_INDEX_END: parseChainIndexEnd(process.env.CHAIN_INDEX_END),
    CHAIN_INDEX_END_DELAY: Number(process.env.CHAIN_INDEX_END_DELAY || 0),
    ALWAYS_FAIL_SCRIPT_HASH: requiredEnv("ALWAYS_FAIL_SCRIPT_HASH"),
    PROTOCOL_NFT_MPH: requiredEnv("PROTOCOL_NFT_MPH"),
    PROTOCOL_SCRIPT_V_SCRIPT_HASH: requiredEnv("PROTOCOL_SCRIPT_V_SCRIPT_HASH"),
    PROJECT_AT_MPH: requiredEnv("PROJECT_AT_MPH"),
    PROOF_OF_BACKING_MPH: requiredEnv("PROOF_OF_BACKING_MPH"),
    TEIKI_PLANT_NFT_MPH: requiredEnv("TEIKI_PLANT_NFT_MPH"),
  };
}

export function ai() {
  return {
    AI_SERVER_URL: requiredEnv("AI_SERVER_URL"),
    AI_S3_BUCKET: requiredEnv("AI_S3_BUCKET"),
    IPFS_GATEWAY_URL: requiredEnv("IPFS_GATEWAY_URL"),
  };
}

export const CHAIN_CHASING_BATCH_INTERVAL = Number(
  process.env.CHAIN_CHASING_BATCH_INTERVAL || 86_400_000 // 1 day
);
export const CHAIN_BLOCK_GC_INTERVAL = Number(
  process.env.CHAIN_BLOCK_GC_INTERVAL || 36_000_000 // 1 hour
);
export const CHAIN_BLOCK_INGESTION_CHECKPOINT = Number(
  process.env.CHAIN_BLOCK_INGESTION_CHECKPOINT || 1_000
);
export const CHAIN_BLOCK_INGESTION_REPORT_RESOLUTION = Number(
  process.env.CHAIN_BLOCK_INGESTION_RESOLUTION || 60_000 // 1 minute
);

function parseChainIndexStart(raw: string): "origin" | "tip" | O.Point {
  if (raw === "origin" || raw === "tip") return raw;
  const [slotStr, hash] = raw.split(":", 2);
  const slot = parseInt(slotStr);
  assert(!isNaN(slot) && hash, "Must be <slot>:<hash>");
  return { slot, hash };
}

function parseChainIndexEnd(
  raw: string | undefined
): "tip" | O.Slot | undefined {
  if (!raw) return undefined;
  if (raw === "tip") return raw;
  const slot = parseInt(raw);
  assert(!isNaN(slot), "Must be <slot>");
  return slot;
}
