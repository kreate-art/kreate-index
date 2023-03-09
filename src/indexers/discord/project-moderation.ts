import { ActionRowBuilder, ButtonBuilder, EmbedBuilder } from "discord.js";
import { UnixTime } from "lucid-cardano";

import { Hex } from "@teiki/protocol/types";
import { assert } from "@teiki/protocol/utils";

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
      tx_id varchar(64) NOT NULL,
      completed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      PRIMARY KEY (project_id, tx_id)
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

    $id: ({ projectId, txId }: Task): ProjectModerationAlertKey =>
      `${projectId}|${txId}`,

    fetch: async function () {
      const {
        connections: { sql },
      } = this;
      const tasks = await sql<Task[]>`
        WITH update_list AS (
          SELECT * FROM (
            SELECT
              pd.id,
              pd.project_id,
              pd.information_cid,
              pd.sponsorship_amount,
              pd.sponsorship_until,
              LAG(pd.information_cid) OVER w AS prev_information_cid
            FROM chain.project_detail pd
            WHERE
              EXISTS (SELECT FROM ipfs.project_info pi WHERE pi.cid = pd.information_cid)
            WINDOW w AS (PARTITION BY project_id ORDER BY id)
          ) AS _a
          WHERE
            information_cid IS DISTINCT FROM prev_information_cid
        )
        SELECT
          ul.project_id,
          o.tx_id,
          pi.title as project_title,
          b.time,
          pm.toxicity,
          pm.obscene,
          pm.identity_attack,
          pm.insult,
          pm.threat,
          pm.sexual_explicit,
          pm.political,
          pm.discrimination,
          pm.drug,
          pm.gun
        FROM update_list ul
        INNER JOIN ipfs.project_info pi ON ul.information_cid = pi.cid
        INNER JOIN ai.project_moderation pm ON ul.information_cid = pm.cid
        INNER JOIN chain.output o ON ul.id = o.id
        INNER JOIN chain.block b ON o.created_slot = b.slot
        WHERE
          NOT EXISTS (
            SELECT FROM discord.project_moderation_alert dpma
            WHERE
              (ul.project_id, o.tx_id) = (dpma.project_id, dpma.tx_id)
          )
        ORDER BY ul.id
        LIMIT ${TASKS_PER_FETCH}
      `;
      return { tasks, continue: tasks.length >= TASKS_PER_FETCH };
    },

    handle: async function (task: WithId<Task, ProjectModerationAlertKey>) {
      const {
        connections: { sql, discord },
        context: { channelId, cexplorerUrl, teikiHost },
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
                .setURL(`${teikiHost}/projects-by-id/${task.projectId}`)
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
            txId: task.txId,
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
