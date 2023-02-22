import {
  ActionRowBuilder,
  ButtonBuilder,
  ButtonStyle,
  Events,
} from "discord.js";

import { assert } from "@teiki/protocol/utils";

import { TEIKI_HOST } from "../config";
import { Connections } from "../connections";
import { sqlNotIn } from "../db/fragments";
import { $setup } from "../framework/base";
import {
  createPollingIndexer,
  PollingIndexer,
  VitalConnections,
} from "../framework/polling";

// TODO: Proper failures handling
type ConnectionsWithDiscord = VitalConnections & Connections<"discord">;
type ProjectId = string;
export type DiscordBotContext = {
  ignored: ProjectId[];
  contentModerationChannelId: string;
  shinkaRoleId: string;
};
type Task = { projectId: ProjectId; customUrl: string | null };

const TASKS_PER_FETCH = 8;

discordProjectAlertIndexer.setup = $setup(async ({ sql }) => {
  await sql`
    -- TODO: Rename this table to: discord.project_alert
    CREATE TABLE IF NOT EXISTS discord.notified_project (
      project_id text PRIMARY KEY
    )
  `;
});

export function discordProjectAlertIndexer(
  connections: ConnectionsWithDiscord
): PollingIndexer<DiscordBotContext> {
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
          discord.notified_project np
          ON np.project_id = d.project_id
        INNER JOIN
          ipfs.project_info pi
          ON d.information_cid = pi.cid
        WHERE
          np.project_id IS NULL
          AND ${sqlNotIn(sql, "d.project_id", ignored)}
        LIMIT ${TASKS_PER_FETCH};
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
          INSERT INTO discord.notified_project ${sql({ projectId })}
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

function startDiscordBotInteractionListener(
  { sql, discord }: ConnectionsWithDiscord,
  { contentModerationChannelId }: DiscordBotContext
) {
  discord.once(Events.ClientReady, async (c) => {
    console.log(`Logged in as ${c.user.tag}!`);
  });

  discord.on(Events.InteractionCreate, async (i) => {
    try {
      if (i.isButton()) {
        if (i.channelId !== contentModerationChannelId) return;

        const [action, projectId] = i.customId.split("-");
        const actionUser = `${i.member?.user.username}#${i.member?.user.discriminator}`;

        if (action === "unblock") {
          await sql`
            DELETE FROM
              ADMIN.blocked_project
            WHERE
              project_id = ${projectId};
          `;
          await i.update({
            content: `${i.message.content} \n\n Unblocked by ${actionUser}`,
            components: i.message.components,
          });
        } else if (action === "block") {
          await sql`
            INSERT INTO
              admin.blocked_project ${sql({ projectId })}
            ON CONFLICT DO NOTHING
          `;
          await i.update({
            content: `${i.message.content} \n\n Blocked by ${actionUser}`,
            components: i.message.components,
          });
        }
      }
    } catch (e) {
      // FIXME: Proper error handling!
      // We are catching to keep the bot alive.
      console.log(e);
    }
  });
}
