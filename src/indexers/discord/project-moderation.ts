import { ActionRowBuilder, ButtonBuilder, EmbedBuilder } from "discord.js";
import { UnixTime } from "lucid-cardano";

import { Cid, Hex } from "@kreate/protocol/types";
import { assert } from "@kreate/protocol/utils";

import { $setup } from "../../framework/base";
import { createPollingIndexer, PollingIndexer } from "../../framework/polling";
import {
  DISPLAYED_LABELS,
  MODERATION_LABELS,
} from "../../types/project/moderation";
import { WithId } from "../../types/typelevel";

import { VitalDiscordConnections, DiscordAlertContext } from "./base";

type ProjectId = string;
type Task = {
  cid: Cid;
  projectId: ProjectId;
  txId: Hex;
  projectTitle: string;
  time: UnixTime;
  // Moderation labels
  toxicity: number;
  obscene: number;
  identityAttack: number;
  insult: number;
  threat: number;
  sexualExplicit: number;
  political: number;
  discrimination: number;
  drug: number;
  gun: number;
  pornographic: number;
};
type ProjectModerationAlertKey = string; // projectId|txId

const TASKS_PER_FETCH = 20;

discordProjectModerationAlertIndexer.setup = $setup(async ({ sql }) => {
  await sql`
    CREATE TABLE IF NOT EXISTS discord.project_moderation_alert (
      project_id varchar(64) NOT NULL,
      cid TEXT NOT NULL,
      completed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      PRIMARY KEY (project_id, cid)
    )
  `;
});

export function discordProjectModerationAlertIndexer(
  connections: VitalDiscordConnections
): PollingIndexer<DiscordAlertContext> {
  return createPollingIndexer({
    name: "discord.project_moderation_alert",
    connections,
    triggers: { channels: ["discord.project_moderation_alert"] },
    concurrency: { workers: 1 },

    $id: ({ projectId, cid }: Task): ProjectModerationAlertKey =>
      `${projectId}|${cid}`,

    fetch: async function () {
      const {
        connections: { sql },
        context: { discordIgnoredNotificationsBefore },
      } = this;
      const tasks = await sql<Task[]>`
        SELECT
          b.time,
          pi.title AS project_title,
          x.*
        FROM (
          SELECT
            DISTINCT ON (pd.project_id, pm.cid)
            pd.id,
            pd.project_id,
            pd.information_cid,
            pm.*
          FROM
            ai.project_moderation pm
          INNER JOIN
            chain.project_detail pd ON pd.information_cid = pm.cid OR pd.last_announcement_cid = pm.cid
          ORDER BY pd.project_id ASC, pm.cid ASC, pd.id DESC
        ) x
        INNER JOIN
          ipfs.project_info pi ON x.information_cid = pi.cid
        INNER JOIN
          chain.output o ON o.id = x.id
        INNER JOIN
          chain.block b ON b.slot = o.created_slot
        WHERE
          NOT EXISTS (
            SELECT FROM discord.project_moderation_alert pma
              WHERE (pma.project_id, pma.cid) = (x.project_id, x.cid)
          )
          AND ${
            discordIgnoredNotificationsBefore
              ? sql`${discordIgnoredNotificationsBefore} <= b.time`
              : sql`TRUE`
          }
        ORDER BY
          x.id
        LIMIT ${TASKS_PER_FETCH}
      `;
      return { tasks, continue: tasks.length >= TASKS_PER_FETCH };
    },

    handle: async function (task: WithId<Task, ProjectModerationAlertKey>) {
      const {
        connections: { sql, discord },
        context: { channelId, cexplorerUrl, kreateOrigin },
      } = this;
      try {
        // Limited at 256 characters
        const formattedProjectTitle = task.projectTitle.replace(
          /(.{150})..+/,
          "$1..."
        );

        // @sk-umiuma: we temporarily filter out these tags
        // as the returning verdict of these tags is unreliable
        const unstableLabels = ["political", "drug", "discrimination"];
        const moderatedLabels = MODERATION_LABELS.filter(
          (label) => !unstableLabels.includes(label) && !!task[label]
        ).map((item) => DISPLAYED_LABELS[item]);

        if (moderatedLabels.length) {
          const embed = new EmbedBuilder()
            .setColor(0x000)
            .setTitle(
              `${formattedProjectTitle}'s content has been detected as inappropriate`
            )
            .setDescription(moderatedLabels.join(", "))
            .setTimestamp(task.time);

          const links = new ActionRowBuilder()
            .addComponents(
              new ButtonBuilder()
                .setStyle(5)
                .setLabel("View project")
                .setURL(`${kreateOrigin}/kreator-by-id/${task.projectId}`)
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
        }

        // TODO: Error handling?
        await sql`
          INSERT INTO discord.project_moderation_alert ${sql({
            projectId: task.projectId,
            cid: task.cid,
          })}
            ON CONFLICT DO NOTHING
        `;
      } catch (error) {
        // TODO: Better log here
        console.error("ERROR:", task.id, error);
        this.retry();
      }
    },
  });
}
