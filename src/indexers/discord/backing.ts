import { EmbedBuilder } from "discord.js";
import { Address, Lovelace } from "lucid-cardano";

import { Hex } from "@teiki/protocol/types";
import { assert } from "@teiki/protocol/utils";

import { sqlNotIn } from "../../db/fragments";
import { $setup } from "../../framework/base";
import { createPollingIndexer, PollingIndexer } from "../../framework/polling";
import { BackingActionType } from "../../types/action";

import { shortenNumber } from "./utils";

import { ConnectionsWithDiscord, DiscordAlertContext } from ".";

type ProjectId = string;
type Task = {
  txId: Hex;
  projectId: ProjectId;
  actorAddress: Address;
  amount: Lovelace;
  action: BackingActionType;
};
type BackingAlertKey = string; // txId|projectId|actorAddress
type DiscordAlertContext$Backing = DiscordAlertContext & {
  ignored: BackingAlertKey[];
};

const TASKS_PER_FETCH = 8;

discordBackingAlertIndexer.setup = $setup(async ({ sql }) => {
  await sql`
    CREATE TABLE IF NOT EXISTS discord.backing_alert (
      tx_id TEXT NOT NULL,
      project_id TEXT NOT NULL,
      actor_address TEXT NOT NULL,
      completed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      PRIMARY KEY (tx_id, project_id, actor_address)
    )
  `;
});

export function discordBackingAlertIndexer(
  connections: ConnectionsWithDiscord
): PollingIndexer<DiscordAlertContext$Backing> {
  return createPollingIndexer({
    name: "discord.backing_alert",
    connections,
    triggers: { channels: ["discord.backing_alert"] },
    concurrency: { workers: 1 },

    $id: ({ txId, projectId, actorAddress }: Task) =>
      `${txId}|${projectId}|${actorAddress}`,

    fetch: async function () {
      const {
        connections: { sql },
        context: { ignored },
      } = this;
      const tasks = await sql<Task[]>`
        SELECT
          ba.tx_id,
          ba.project_id,
          ba.actor_address,
          ba.action,
          ba.amount
        FROM
          chain.backing_action ba
        LEFT JOIN
          discord.backing_alert dba
          ON (dba.tx_id, dba.project_id, dba.actor_address)
               = (ba.tx_id, ba.project_id, ba.actor_address)
        WHERE
          dba.project_id IS NULL
          AND ${sqlNotIn(
            sql,
            "(ba.tx_id, ba.project_id, ba.actor_address)",
            ignored.map((item) => {
              const [txId, projectId, actorAddress] = item.split("|");
              return sql([txId, projectId, actorAddress]);
            })
          )}
        LIMIT ${TASKS_PER_FETCH}
      `;
      return { tasks, continue: tasks.length >= TASKS_PER_FETCH };
    },

    handle: async function ({ txId, projectId, actorAddress, amount, action }) {
      const {
        connections: { sql, discord },
        context: { ignored },
      } = this;
      try {
        const { contentModerationChannelId: channelId } = this.context;
        const embed = new EmbedBuilder()
          .setColor(0x006e46)
          .setTitle(action)
          .addFields({ name: "Backer", value: actorAddress })
          .addFields({ name: "Project ID", value: projectId })
          .addFields({
            name: "Amount",
            value: `${shortenNumber(amount, { shift: -6 })} ₳`,
          });

        const channel = await discord.channels.fetch(channelId);
        assert(channel, `Channel ${channelId} not found`);
        assert("send" in channel, `Channel ${channelId} is not sendable`);
        channel.send({
          embeds: [embed],
        });

        // TODO: Error handling?
        await sql`
          INSERT INTO discord.backing_alert ${sql([
            { txId, projectId, actorAddress },
          ])}
            ON CONFLICT DO NOTHING
        `;
      } catch (error) {
        // TODO: Better log here
        const id = `${txId}|${projectId}|${actorAddress}`;
        console.error("ERROR:", id, error);
        ignored.push(id);
      }
    },
  });
}
