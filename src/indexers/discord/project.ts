import { ActionRowBuilder, ButtonBuilder, ButtonStyle } from "discord.js";

import { assert } from "@teiki/protocol/utils";

import { TEIKI_HOST } from "../../config";
import { sqlNotIn } from "../../db/fragments";
import { $setup } from "../../framework/base";
import { createPollingIndexer, PollingIndexer } from "../../framework/polling";

import {
  ConnectionsWithDiscord,
  DiscordAlertContext,
  startDiscordBotInteractionListener,
} from ".";

type ProjectId = string;
type Task = { projectId: ProjectId; customUrl: string | null };
type DiscordAlertContext$Project = DiscordAlertContext & {
  ignored: ProjectId[];
};

const TASKS_PER_FETCH = 8;

discordProjectAlertIndexer.setup = $setup(async ({ sql }) => {
  await sql`
    CREATE TABLE IF NOT EXISTS discord.project_alert (
      project_id text PRIMARY KEY,
      completed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `;
});

export function discordProjectAlertIndexer(
  connections: ConnectionsWithDiscord
): PollingIndexer<DiscordAlertContext$Project> {
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
        context: { ignored },
      } = this;
      const tasks = await sql<Task[]>`
        SELECT DISTINCT
          d.project_id as project_id,
          pi.custom_url as custom_url
        FROM
          chain.project_detail d
        LEFT JOIN
          discord.project_alert dpa
          ON dpa.project_id = d.project_id
        INNER JOIN
          ipfs.project_info pi
          ON d.information_cid = pi.cid
        WHERE
          dpa.project_id IS NULL
          AND ${sqlNotIn(sql, "d.project_id", ignored)}
        LIMIT ${TASKS_PER_FETCH}
      `;
      return { tasks, continue: tasks.length >= TASKS_PER_FETCH };
    },

    handle: async function ({ projectId, customUrl }) {
      const {
        connections: { sql, discord },
        context: { ignored },
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

        const { contentModerationChannelId: channelId, shinkaRoleId } =
          this.context;
        const channel = await discord.channels.fetch(channelId);
        assert(channel, `Channel ${channelId} not found`);
        assert("send" in channel, `Channel ${channelId} is not sendable`);
        const projectUrl = customUrl
          ? `${TEIKI_HOST}/projects/${customUrl}`
          : `${TEIKI_HOST}/projects-by-id/${projectId}`;
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
        ignored.push(projectId);
      }
    },
  });
}
