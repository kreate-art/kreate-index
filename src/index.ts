import * as os from "os";

import * as config from "./config";
import * as connections from "./connections";
import { Indexer, Setup } from "./framework/base";
import { PollingIndexer } from "./framework/polling";
import { aiLogoIndexer } from "./indexers/ai/logo";
import { aiOcrIndexer } from "./indexers/ai/ocr";
import { aiPodcastIndexer } from "./indexers/ai/podcast";
import { aiProjectModerationIndexer } from "./indexers/ai/project-moderation";
import { getChainIndexer } from "./indexers/chain";
import { createDiscordAlertContext } from "./indexers/discord";
import { discordBackingAlertIndexer } from "./indexers/discord/backing";
import { discordProjectAlertIndexer } from "./indexers/discord/project";
import { discordWithdrawFundsAlertIndexer } from "./indexers/discord/withdraw-funds";
import {
  ipfsProjectAnnouncementIndexer,
  ipfsProjectInfoIndexer,
} from "./indexers/ipfs/project";
import {
  setupAdminTables,
  setupMigrationTables,
  setupSchemas,
  setupViews,
} from "./indexers/schema";
import prexit from "./prexit";
import { MaybePromise } from "./types/typelevel";
import { objectKeys } from "./utils";

// Cleanups
const shutdowns: (() => MaybePromise<void>)[] = [];

// Global states
let willSaveCheckpoint = true;

prexit(async (signal, error, idk) => {
  console.warn(`$ TIME: ${((Date.now() - RUN_AT) / 1000).toFixed(2)}`);
  console.warn("$ EXIT:", signal, error, idk);
  for (const shutdown of [...shutdowns].reverse()) await shutdown();
  await connections.cleanup();
  if (process.exitCode == null && error != null)
    process.exitCode = os.constants.signals[signal as NodeJS.Signals] ?? 1;
  if (signal === "uncaughtException") {
    // TODO: Sentry here?
    console.error("!! UNCAUGHT EXCEPTION !!");
  } else if ((signal as string) === "unhandledRejection") {
    // TODO: Sentry here?
    console.error("!! UNHANDLED REJECTION !!");
  }
  console.log("=== GOOD BYE ===");
});

// Indexers
const teikiChainIndexer: Indexer = {
  // TODO: Fix those abstraction leak later...
  setup: getChainIndexer.setup,
  run: async () => {
    const cc = config.chain();
    const indexer = await getChainIndexer(
      await connections.provide(
        "sql",
        "ogmios",
        "lucid",
        "notifications",
        "views"
      )
    );
    await indexer.staking.start();
    const iConfig = cc.CONFIG;
    const { intersection } = await indexer.start({
      context: {
        staking: indexer.staking,
        config: iConfig,
        protocolVersion: 0,
        // TODO: Reuse config.hashesTreasury directly
        scriptHashes: {
          dedicatedTreasury: new Set(iConfig.hashesTreasury.dedicated),
          sharedTreasury: new Set(iConfig.hashesTreasury.shared),
          openTreasury: new Set(iConfig.hashesTreasury.open),
        },
      },
      begin:
        typeof cc.CHAIN_INDEX_BEGIN === "string"
          ? cc.CHAIN_INDEX_BEGIN
          : [cc.CHAIN_INDEX_BEGIN],
      end:
        cc.CHAIN_INDEX_END == undefined
          ? undefined
          : {
              at: cc.CHAIN_INDEX_END,
              delay: cc.CHAIN_INDEX_END_DELAY,
            },
    });
    console.log("<> Chain intersection found:", intersection);
    return async () => {
      await indexer.stop({
        immediate: true,
        saveCheckpoint: willSaveCheckpoint,
      });
      await indexer.staking.stop();
    };
  },
};

function wrapPollingIndexer<Context, Keys extends connections.ConnectionKey[]>(
  creator: {
    setup: Setup;
    (
      connections: connections.Connections<Keys[number]>
    ): PollingIndexer<Context>;
  },
  connectionKeys: Keys,
  getContext: () => MaybePromise<Context>
): Indexer {
  return {
    setup: creator.setup,
    run: async () => {
      const context = await getContext();
      const indexer = creator(await connections.provide(...connectionKeys));
      void indexer.start(context);
      return indexer.stop.bind(indexer);
    },
  };
}

const AllIndexers = {
  chain: teikiChainIndexer,
  "ipfs.project_info": wrapPollingIndexer(
    ipfsProjectInfoIndexer,
    ["sql", "ipfs", "notifications", "views"],
    () => null
  ),
  "ipfs.project_announcement": wrapPollingIndexer(
    ipfsProjectAnnouncementIndexer,
    ["sql", "ipfs", "notifications"],
    () => null
  ),
  "ai.logo": wrapPollingIndexer(
    aiLogoIndexer,
    ["sql", "ipfs", "s3", "notifications"],
    () => {
      const cc = config.ai();
      return { s3Bucket: cc.AI_S3_BUCKET, fetched: new Set<string>() };
    }
  ),
  "ai.podcast": wrapPollingIndexer(
    aiPodcastIndexer,
    ["sql", "s3", "notifications"],
    () => {
      return {
        aiServerUrl: config.ai().AI_SERVER_URL,
        s3Bucket: config.aws().ASSETS_S3_BUCKET,
        s3Prefix: "podcasts/",
        summaryWordsLimit: config.cardano().NETWORK === "mainnet" ? 1_500 : 100,
      };
    }
  ),
  "ai.ocr": wrapPollingIndexer(aiOcrIndexer, ["sql", "notifications"], () => {
    const cc = config.ai();
    return {
      aiServerUrl: cc.AI_SERVER_URL,
      ipfsGatewayUrl: cc.IPFS_GATEWAY_URL,
    };
  }),
  "ai.project_moderation": wrapPollingIndexer(
    aiProjectModerationIndexer,
    ["sql", "notifications"],
    () => {
      const cc = config.ai();
      return {
        aiServerUrl: cc.AI_SERVER_URL,
      };
    }
  ),
  "discord.project_alert": wrapPollingIndexer(
    discordProjectAlertIndexer,
    ["sql", "discord", "notifications"],
    () =>
      createDiscordAlertContext(
        config.discord().DISCORD_CONTENT_MODERATION_CHANNEL_ID
      )
  ),
  "discord.backing_alert": wrapPollingIndexer(
    discordBackingAlertIndexer,
    ["sql", "discord", "notifications"],
    () =>
      createDiscordAlertContext(
        config.discord().DISCORD_BACKING_ALERT_CHANNEL_ID
      )
  ),
  "discord.withdraw_funds_alert": wrapPollingIndexer(
    discordWithdrawFundsAlertIndexer,
    ["sql", "discord", "notifications"],
    () => {
      const cc = config.discord();
      return {
        ignored: [],
        notificationChannelId: cc.DISCORD_WITHDRAW_FUNDS_ALERT_CHANNEL_ID,
        shinkaRoleId: cc.DISCORD_SHINKA_ROLE_ID,
        cexplorerUrl: config.cardano().CEXPLORER_URL,
      };
    }
  ),
} as const;

const AllIndexerKeys = objectKeys(AllIndexers);

const DisabledIndexers: Partial<Record<config.Env, string[]>> = {
  development: AllIndexerKeys.filter(
    (k) => k.startsWith("ai.") || k.startsWith("discord.")
  ),
  testnet: AllIndexerKeys.filter((k) => k.startsWith("discord.")),
};
const disabled = DisabledIndexers[config.ENV] ?? [];

// Main
const args = process.argv.slice(2);
if (!args.length) {
  console.error("Arguments:");
  console.error("* all");
  console.error("+ setup");
  Object.keys(AllIndexers).forEach((o) =>
    console.error(`${disabled.includes(o) ? "." : "-"} ${o}`)
  );
  process.exit(1);
}

const indexers: [string, Indexer][] = [];
const includesAll = args.includes("all");
if (includesAll) {
  for (const name of Object.keys(AllIndexers))
    if (name === "chain" && !includesAll) continue;
    else if (disabled.includes(name))
      console.warn(`[${name}] is disabled by default on {${config.ENV}}`);
    else args.push(name);
}
let willSetup = false;
const expandedArgs = args.flatMap((arg) => {
  if (arg.endsWith(".")) {
    const expanded = AllIndexerKeys.filter((k) => k.startsWith(arg));
    if (!expanded.length) throw new Error(`No indexer with prefix: ${arg}`);
    return expanded;
  } else {
    return [arg];
  }
});
const sorted = Array.from(new Set(expandedArgs))
  .map((name): [number, string] => [
    Object.keys(AllIndexers).indexOf(name),
    name,
  ])
  .sort((a, b) => a[0] - b[0]);
for (const [index, name] of sorted) {
  if (name === "setup" || name === "all") willSetup = true;
  else if (index < 0) throw new Error(`Unexpected indexer: ${name}`);
  else {
    indexers.push([name, AllIndexers[name as keyof typeof AllIndexers]]);
  }
}
for (const [name, _] of indexers) console.log(`% ${name}`);

const notifications = await connections.provideOne("notifications");
notifications.listen(
  "index",
  (payload: string) => {
    if (payload === "reload") {
      console.log("Received signal: reload");
      // Just shutdown for now...
      willSaveCheckpoint = false;
      prexit.exit(174);
    } else {
      console.warn(`Unrecognized signal: ${payload}`);
    }
  },
  () => console.log('Listening on "index" for signals...')
);

if (willSetup) {
  const resources = await connections.provide("sql");
  await setupSchemas(resources);
  await Promise.all(
    Object.values(AllIndexers).map((ix) => ix.setup(resources))
  );
  await setupAdminTables(resources);
  await setupMigrationTables(resources);
  await setupViews(resources);
}

const RUN_AT = Date.now();
if (indexers.length)
  for (const [_, indexer] of indexers) shutdowns.push(await indexer.run());
else {
  console.warn("No indexer specified, I'm done!");
  prexit.exit0();
}
