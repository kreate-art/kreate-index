import fs from "node:fs";

import { ConnectionConfig } from "@cardano-ogmios/client";
import * as O from "@cardano-ogmios/schema";
import dotenv from "dotenv";
import YAML from "yaml";

import { assert } from "@teiki/protocol/utils";

import { loadConfig } from "./indexers/chain/context";

dotenv.config();

function requiredEnv(key: string): string {
  const value = process.env[key];
  if (value) return value;
  else throw new Error(`${key} must be set`);
}

// TODO: Env vars validation

export type Env = "development" | "testnet" | "mainnet";
export type Network = "preview" | "preprod" | "mainnet";

export const ENV = (process.env.ENV || "development") as Env;

export const TEIKI_HOST = requiredEnv("TEIKI_HOST");

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
  const cexplorerUrl = requiredEnv("CEXPLORER_URL");
  assert(
    /^(http|https):\/\/.*[^/]$/.test(cexplorerUrl),
    "Cexplorer url must starts with " +
      "'http://' or 'https://' and must not end with '/'"
  );
  return { NETWORK: network as Network, CEXPLORER_URL: cexplorerUrl };
}

export function database() {
  return {
    DATABASE_URL: requiredEnv("DATABASE_URL"),
    DATABASE_MAX_CONNECTIONS: Number(
      process.env.DATABASE_MAX_CONNECTIONS || 16
    ),
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
    DISCORD_BACKING_ALERT_CHANNEL_ID: requiredEnv(
      "DISCORD_BACKING_ALERT_CHANNEL_ID"
    ),
    DISCORD_SHINKA_ROLE_ID: requiredEnv("DISCORD_SHINKA_ROLE_ID"),
  };
}

export function chainIndex() {
  // TODO: Validate config
  const rawConfig = YAML.parse(
    fs.readFileSync(requiredEnv("CHAIN_INDEX_CONFIG"), "utf8")
  );
  const config = loadConfig(rawConfig);
  const bootstrap = config.bootstrap.length ? config.bootstrap[0] : undefined;
  const begin =
    parseChainIndexBegin(process.env.CHAIN_INDEX_BEGIN) ?? bootstrap;
  if (!begin)
    throw new Error("CHAIN_INDEX_BEGIN or 'bootstrap' in config must be set");
  return {
    CHAIN_INDEX_BEGIN: begin,
    CHAIN_INDEX_END: parseChainIndexEnd(process.env.CHAIN_INDEX_END),
    CHAIN_INDEX_END_DELAY: Number(process.env.CHAIN_INDEX_END_DELAY || 0),
    CONFIG: config,
  };
}

export function aws() {
  return {
    ASSETS_S3_BUCKET: requiredEnv("ASSETS_S3_BUCKET"),
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

function parseChainIndexBegin(
  raw: string | undefined
): "origin" | "tip" | O.Point | undefined {
  if (!raw) return undefined;
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
