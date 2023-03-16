import { ActionRowBuilder, ButtonBuilder, EmbedBuilder } from "discord.js";
import { UnixTime } from "lucid-cardano";

import { Hex } from "@kreate/protocol/types";
import { assert } from "@kreate/protocol/utils";

import { TASKS_PER_FETCH } from "../../constants";
import { $setup } from "../../framework/base";
import { PollingIndexer, createPollingIndexer } from "../../framework/polling";
import { WithId } from "../../types/typelevel";

import { DiscordAlertContext, VitalDiscordConnections } from "./base";

type ProjectId = string;
type Task = {
  projectId: ProjectId;
  txId: Hex;
  time: UnixTime;
  projectTitle: string;
  customUrl: string | null;
  title: string;
};

type ProjectAnnouncementAlertKey = string; // projectId|txId

discordProjectAnnouncementAlertIndexer.setup = $setup(async ({ sql }) => {
  await sql`
    CREATE TABLE IF NOT EXISTS discord.project_announcement_alert (
      project_id varchar(64) NOT NULL,
      tx_id varchar(64) NOT NULL,
      completed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      PRIMARY KEY (project_id, tx_id)
    )
  `;
});

export function discordProjectAnnouncementAlertIndexer(
  connections: VitalDiscordConnections
): PollingIndexer<DiscordAlertContext> {
  return createPollingIndexer({
    name: "discord.project_announcement_alert",
    connections,
    triggers: { channels: ["discord.project_announcement_alert"] },
    concurrency: { workers: 1 },

    $id: ({ projectId, txId }: Task): ProjectAnnouncementAlertKey =>
      `${projectId}|${txId}`,

    fetch: async function () {
      const {
        connections: { sql },
        context: { discordIgnoredNotificationsBefore },
      } = this;

      const tasks = await sql<Task[]>`
        WITH al AS (
          SELECT
            pd.id,
            pd.project_id,
            pd.information_cid,
            pd.last_announcement_cid,
            LAG(pd.last_announcement_cid) OVER w AS prev_last_announcement_cid
          FROM chain.project_detail pd
          WHERE
            EXISTS (SELECT FROM ipfs.project_announcement pa WHERE pa.cid = pd.last_announcement_cid)
          WINDOW w AS (PARTITION BY project_id ORDER BY id)
        )
        SELECT
          al.project_id,
          o.tx_id,
          b.time,
          pa1.title,
          pi.custom_url,
          pi.title as project_title
        FROM
          al
        INNER JOIN ipfs.project_announcement pa1
          ON pa1.cid = al.last_announcement_cid
        INNER JOIN ipfs.project_info pi
          ON pi.cid = al.information_cid
        INNER JOIN chain.output o
          ON o.id = al.id
        INNER JOIN chain.block b
          ON b.slot = o.created_slot
        WHERE
          al.last_announcement_cid IS DISTINCT FROM al.prev_last_announcement_cid
          AND
            NOT EXISTS (
              SELECT FROM discord.project_announcement_alert dpaa
              WHERE
                (al.project_id, o.tx_id) = (dpaa.project_id, dpaa.tx_id)
            )
          AND ${
            discordIgnoredNotificationsBefore
              ? sql`${discordIgnoredNotificationsBefore} <= b.time `
              : sql`TRUE`
          }
        ORDER BY
          al.id
        LIMIT ${TASKS_PER_FETCH}
      `;

      return { tasks, continue: tasks.length >= TASKS_PER_FETCH };
    },

    handle: async function (task: WithId<Task, ProjectAnnouncementAlertKey>) {
      const {
        connections: { sql, discord },
        context: { channelId, cexplorerUrl, kreateOrigin },
      } = this;

      try {
        const formattedProjectTitle = task.projectTitle.replace(
          /(.{100})..+/,
          "$1..."
        );
        const formattedAnnoucementTitle = task.title.replace(
          /(.{100})..+/,
          "$1..."
        );
        const embed = new EmbedBuilder()
          .setColor(0xf74055)
          .setTitle(
            `${formattedProjectTitle} has just posted the announcement "${formattedAnnoucementTitle}"`
          )
          .setTimestamp(task.time);

        const links = new ActionRowBuilder()
          .addComponents(
            new ButtonBuilder()
              .setStyle(5)
              .setLabel("View announcement")
              .setURL(`${kreateOrigin}/projects-by-id/${task.projectId}#posts`)
          )
          .addComponents(
            new ButtonBuilder()
              .setStyle(5)
              .setLabel("View transaction")
              .setURL(`${cexplorerUrl}/tx/${task.txId}`)
          );

        const channel = await discord.channels.fetch(channelId);
        assert(channel, `Channel ${channelId} not found`);
        assert("send" in channel, `Channel ${channelId} is not sendable`);
        channel.send({
          embeds: [embed],
          // eslint-disable-next-line @typescript-eslint/no-explicit-any
          components: [links as any],
        });

        await sql`
            INSERT INTO discord.project_announcement_alert ${sql([
              { projectId: task.projectId, txId: task.txId },
            ])}
              ON CONFLICT DO NOTHING
          `;
      } catch (error) {
        console.error("ERROR:", task.id, error);
        this.retry();
      }
    },
  });
}
