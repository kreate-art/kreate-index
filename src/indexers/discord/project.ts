import {
  ActionRowBuilder,
  ButtonBuilder,
  ButtonStyle,
  CacheType,
  Events,
  Interaction,
} from "discord.js";

import { assert } from "@kreate/protocol/utils";

import { $setup } from "../../framework/base";
import { createPollingIndexer, PollingIndexer } from "../../framework/polling";

import { VitalDiscordConnections, DiscordAlertContext } from "./base";

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
  connections: VitalDiscordConnections
): PollingIndexer<DiscordAlertContext & { interactionCleanup?: () => void }> {
  return createPollingIndexer({
    name: "discord.project_alert",
    connections,
    triggers: { channels: ["discord.project_alert"] },
    concurrency: { workers: 1 },

    $id: ({ projectId }: Task) => projectId,

    initialize: function () {
      this.context.interactionCleanup = startDiscordBotInteractionListener(
        "discord.project_alert:interaction",
        this.connections,
        this.context
      );
    },

    finalize: function () {
      this.context.interactionCleanup?.();
    },

    fetch: async function () {
      const {
        connections: { sql },
        context: { discordIgnoredNotificationsBefore },
      } = this;
      const tasks = await sql<Task[]>`
        SELECT
          pd.project_id,
          pi.custom_url,
          b.time
        FROM (
          SELECT
            DISTINCT ON (project_id)
            *
          FROM
            CHAIN.project_detail
          ORDER BY
            project_id ASC, id ASC
        ) AS pd
        INNER JOIN
          ipfs.project_info pi ON pi.cid = pd.information_cid
        INNER JOIN
          chain.output o ON o.id = pd.id
        INNER JOIN
          chain.block b ON b.slot = o.created_slot
        WHERE
          NOT EXISTS (
            SELECT FROM discord.project_alert dpa
            WHERE dpa.project_id = pd.project_id
          )
          AND ${
            discordIgnoredNotificationsBefore
              ? sql`${discordIgnoredNotificationsBefore} <= b.time`
              : sql`TRUE`
          }
        LIMIT ${TASKS_PER_FETCH}
      `;
      return { tasks, continue: tasks.length >= TASKS_PER_FETCH };
    },

    handle: async function ({ projectId, customUrl }) {
      const {
        connections: { sql, discord },
        context: { channelId, shinkaRoleId, kreateOrigin },
      } = this;
      try {
        // NOTE: This function is copied from kreate-backend/src/indexer/project-info.ts
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

        const channel = await discord.channels.fetch(channelId);
        assert(channel, `Channel ${channelId} not found`);
        assert("send" in channel, `Channel ${channelId} is not sendable`);
        const projectUrl = customUrl
          ? `${kreateOrigin}/k/${customUrl}`
          : `${kreateOrigin}/kreator-by-id/${projectId}`;
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

function startDiscordBotInteractionListener(
  name: string,
  { sql, discord }: VitalDiscordConnections,
  { channelId }: DiscordAlertContext
) {
  async function onInteractionCreate(i: Interaction<CacheType>) {
    try {
      if (i.isButton()) {
        if (i.channelId !== channelId) return;

        const [action, projectId] = i.customId.split("-");
        const actionUser = `${i.member?.user.username}#${i.member?.user.discriminator}`;

        switch (action) {
          case "unblock":
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
            return;
          case "block":
            await sql`
            INSERT INTO
              admin.blocked_project ${sql({ projectId })}
            ON CONFLICT DO NOTHING
          `;
            await i.update({
              content: `${i.message.content} \n\n Blocked by ${actionUser}`,
              components: i.message.components,
            });
            return;
          default:
            return;
        }
      }
    } catch (e) {
      // FIXME: Proper error handling!
      // We are catching to keep the bot alive.
      console.error(e);
    }
  }
  console.log(`[${name}] Listen for interactions on: ${channelId}`);
  discord.on(Events.InteractionCreate, onInteractionCreate);
  return () => {
    console.log(`[${name}] Unlisten interactions on: ${channelId}`);
    discord.off(Events.InteractionCreate, onInteractionCreate);
  };
}
