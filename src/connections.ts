import { S3Client } from "@aws-sdk/client-s3";
import * as discord from "discord.js";
import { GatewayIntentBits as DiscordGatewayIntentBits } from "discord.js";
import * as redis from "ioredis";
import * as IpfsHttpClient from "ipfs-http-client";
import { HTTPError as IpfsHttpError } from "ipfs-utils/src/http/error";
import { Lucid, Network as LucidNetwork } from "lucid-cardano";

import { assert } from "@kreate/protocol/utils";

import * as config from "./config";
import * as db from "./db";
import { createNotificationsService, Notifications } from "./db/notifications";
import { createViewsController, ViewsController } from "./db/views";
import { createOgmiosContextFactory, OgmiosContextFactory } from "./ogmios";
import { MaybePromise } from "./types/typelevel";

import type { IPFS } from "ipfs-core-types";

const DISCORD_NOTIFICATION_DEBOUNCE = 5000; // 5s
const DISCORD_NOTIFICATION_OPTIONS_ENTRY = {
  debounce: DISCORD_NOTIFICATION_DEBOUNCE,
};

export type AllConnections = {
  readonly sql: db.Sql;
  readonly redis: redis.Redis;
  readonly ipfs: Ipfs;
  readonly ogmios: OgmiosContextFactory;
  readonly lucid: Lucid;
  readonly discord: discord.Client<boolean>;
  readonly s3: S3Client;
  readonly notifications: Notifications;
  readonly views: ViewsController;
};

export type ConnectionKey = keyof AllConnections;

export type Connections<K extends ConnectionKey> = {
  [P in K]: AllConnections[P];
};

type Registration<K extends ConnectionKey> = {
  readonly instruction: {
    connect: () => MaybePromise<AllConnections[K]>;
    disconnect?: (self: AllConnections[K]) => MaybePromise<void>;
  };
  connection?: Promise<AllConnections[K]>;
};

const registry: { [K in ConnectionKey]?: Registration<K> } = {};

const cleanups: [ConnectionKey, () => MaybePromise<void>][] = [];

export function register<K extends ConnectionKey>(
  key: K,
  instruction: Registration<K>["instruction"]
) {
  assert(
    registry[key] === undefined,
    `Connection <${key}> is already registered.`
  );
  (registry[key] as Registration<K>) = { instruction };
}

export function deregister<K extends ConnectionKey>(key: K) {
  if (registry[key] === undefined)
    console.warn(`Connection <${key}> is already deregistered.`);
  delete registry[key];
}

export async function provide<K extends ConnectionKey>(
  ...keys: K[]
): Promise<Connections<K>> {
  return Object.fromEntries(
    await Promise.all(keys.map(async (key) => [key, await provideOne(key)]))
  );
}

export async function provideOne<K extends ConnectionKey>(
  key: K
): Promise<AllConnections[K]> {
  const reg = registry[key];
  assert(reg !== undefined, `Connection <${key}> is not registered yet!`);
  if (reg.connection === undefined)
    reg.connection = (async function () {
      console.log(`<${key}> Connecting...`);
      const connection = await reg.instruction.connect();
      const disconnect = reg.instruction.disconnect;
      if (disconnect !== undefined)
        cleanups.push([
          key,
          async () => {
            console.log(`<${key}> Disconnecting...`);
            await disconnect(connection);
            console.log(`<${key}> Disconnected!`);
          },
        ]);
      console.log(`<${key}> Connected!`);
      return connection;
    })();
  return reg.connection;
}

export async function cleanup(): Promise<
  { key: ConnectionKey; error: unknown }[]
> {
  const errors = [];
  for (const [key, cleanup] of [...cleanups].reverse()) {
    try {
      await cleanup();
    } catch (error) {
      console.error(error);
      errors.push({ key, error });
    }
  }
  return errors;
}

// Connections setup
register("sql", {
  connect: () => {
    const cc = config.database();
    return db.postgres(
      cc.DATABASE_URL,
      db.options({ max: cc.DATABASE_MAX_CONNECTIONS })
    );
  },
  disconnect: (self) => self.end({ timeout: 10 }),
});

register("redis", {
  connect: () => {
    const cc = config.redis();
    return new redis.Redis(cc.REDIS_URL, {
      username: cc.REDIS_USERNAME,
      password: cc.REDIS_PASSWORD,
      connectionName: "index",
      enableAutoPipelining: true,
      scripts: {
        delif: {
          lua: `
            if redis.call("get", KEYS[1]) == ARGV[1] then
              return redis.call("del", KEYS[1])
            else
              return 0
            end
          `,
          numberOfKeys: 1,
          readOnly: false,
        },
      },
    });
  },
  disconnect: async (self) => {
    await self.quit();
  },
});

declare module "ioredis" {
  interface RedisCommander<Context> {
    delif(
      key: redis.RedisKey,
      value: redis.RedisValue
    ): redis.Result<number, Context>;
  }
}

register("ipfs", {
  connect: () => {
    const cc = config.ipfs();
    return Object.assign(
      IpfsHttpClient.create({
        url: cc.IPFS_SERVER_URL,
        timeout: cc.IPFS_SERVER_TIMEOUT,
      }),
      {
        isOfflineError: function (error: unknown): boolean {
          return (
            error instanceof IpfsHttpError &&
            error.name === "HTTPError" &&
            error.message.includes("block was not found locally (offline)")
          );
        },
      }
    );
  },
});

interface Ipfs
  extends IPFS<IpfsHttpClient.HTTPClientExtraOptions & { offline?: boolean }> {
  isOfflineError: (error: unknown) => boolean;
}

register("ogmios", {
  connect: () => createOgmiosContextFactory(config.ogmios()),
  disconnect: (self) => self.shutdown(),
});

register("lucid", {
  connect: () => {
    const network = config.cardano().NETWORK;
    return Lucid.new(
      undefined,
      (network.charAt(0).toUpperCase() + network.substring(1)) as LucidNetwork
    );
  },
});

register("discord", {
  connect: async () => {
    const client = new discord.Client({
      intents: [DiscordGatewayIntentBits.DirectMessages],
      partials: [discord.Partials.Channel],
    });
    const { DISCORD_BOT_TOKEN } = config.discord();
    client.once(discord.Events.ClientReady, async (c) =>
      console.log(`<discord> Logged in as ${c.user.tag}!`)
    );
    await client.login(DISCORD_BOT_TOKEN);
    return client;
  },
  disconnect: (self) => self.destroy(),
});

register("notifications", {
  connect: async () =>
    createNotificationsService(await provideOne("sql"), {
      channels: {
        "discord.project_alert": DISCORD_NOTIFICATION_OPTIONS_ENTRY,
        "discord.backing_alert": DISCORD_NOTIFICATION_OPTIONS_ENTRY,
        "discord.withdraw_funds_alert": DISCORD_NOTIFICATION_OPTIONS_ENTRY,
        "discord.delegation_alert": DISCORD_NOTIFICATION_OPTIONS_ENTRY,
        "discord.project_update_alert": DISCORD_NOTIFICATION_OPTIONS_ENTRY,
      },
    }),
  disconnect: (self) => self.shutdown(),
});

register("views", {
  connect: async () =>
    createViewsController(await provideOne("sql"), {
      views: {
        "views.project_custom_url": { concurrently: true },
        "views.project_summary": { concurrently: true, debounce: 1_000 },
      },
    }),
  disconnect: (self) => self.shutdown(),
});

register("s3", {
  connect: () => new S3Client({}),
  disconnect: (self) => self.destroy(),
});
