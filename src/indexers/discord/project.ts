import { ActionRowBuilder, ButtonBuilder, ButtonStyle } from "discord.js";

import { assert } from "@teiki/protocol/utils";

import { $setup } from "../../framework/base";
import { createPollingIndexer, PollingIndexer } from "../../framework/polling";

import {
  ConnectionsWithDiscord,
  DiscordAlertContext,
  startDiscordBotInteractionListener,
} from ".";

type ProjectId = string;
type Task = { projectId: ProjectId; customUrl: string | null };

const TASKS_PER_FETCH = 20;

discordProjectAlertIndexer.setup = $setup(async ({ sql }) => {
  await sql`
    CREATE TABLE IF NOT EXISTS discord.project_alert (
      project_id varchar(64) PRIMARY KEY,
      completed_at timestamptz NOT NULL DEFAULT NOW()
    )
  `;
});

export function discordProjectAlertIndexer(
  connections: ConnectionsWithDiscord
): PollingIndexer<DiscordAlertContext> {
  return createPollingIndexer({
    name: "discord.project_alert",
    connections,
    triggers: { channels: ["discord.project_alert"] },
    concurrency: { workers: 1 },

    $id: ({ projectId }: Task) => projectId,

    initialize: function () {
      startDiscordBotInteractionListener(this.connections, this.context);
    },

    fetch: async function () {
      const {
        connections: { sql },
      } = this;
      const tasks = await sql<Task[]>`
        SELECT DISTINCT
          d.project_id as project_id,
          pi.custom_url as custom_url
        FROM
          chain.project_detail d
        INNER JOIN
          ipfs.project_info pi
          ON d.information_cid = pi.cid
        WHERE
          NOT EXISTS (
            SELECT FROM discord.project_alert dpa
            WHERE dpa.project_id = d.project_id
          )
        LIMIT ${TASKS_PER_FETCH}
      `;
      return { tasks, continue: tasks.length >= TASKS_PER_FETCH };
    },

    handle: async function ({ projectId, customUrl }) {
      const {
        connections: { sql, discord },
        context: { teikiHost },
      } = this;
      try {
        // NOTE: This function is copied from teiki-backend/src/indexer/project-info.ts
        // TODO: Proper type
        const buttons = new ActionRowBuilder()
          .addComponents(
            new ButtonBuilder()
              .setCustomId(`block-${projectId}`)
              .setLabel("Block!")
              .setStyle(ButtonStyle.Danger)
          )
          .addComponents(
            new ButtonBuilder()
              .setCustomId(`unblock-${projectId}`)
              .setLabel("Unblock")
              .setStyle(ButtonStyle.Secondary)
          );

        const { channelId, shinkaRoleId } = this.context;
        const channel = await discord.channels.fetch(channelId);
        assert(channel, `Channel ${channelId} not found`);
        assert("send" in channel, `Channel ${channelId} is not sendable`);
        const projectUrl = customUrl
          ? `${teikiHost}/projects/${customUrl}`
          : `${teikiHost}/projects-by-id/${projectId}`;
        channel.send({
          content: `New project: ${projectUrl}\n<@&${shinkaRoleId}>`,
          // eslint-disable-next-line @typescript-eslint/no-explicit-any
          components: [buttons as any],
        });

        // TODO: Error handling?
        await sql`
          INSERT INTO discord.project_alert ${sql({ projectId })}
            ON CONFLICT DO NOTHING
        `;
      } catch (error) {
        // TODO: Better log here
        console.error("ERROR:", projectId, error);
        this.retry();
      }
    },
  });
}
