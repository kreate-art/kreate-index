import { ActionRowBuilder, ButtonBuilder, EmbedBuilder } from "discord.js";
import { UnixTime } from "lucid-cardano";

import { Hex } from "@teiki/protocol/types";
import { assert } from "@teiki/protocol/utils";

import { sqlNotIn } from "../../db/fragments";
import { $setup } from "../../framework/base";
import { createPollingIndexer, PollingIndexer } from "../../framework/polling";
import { WithId } from "../../types/typelevel";
import { DISPLAYED_SCOPE, ProjectUpdateScope } from "../../types/update";

import { ConnectionsWithDiscord, DiscordAlertContext } from ".";

type ProjectId = string;
type Task = {
  txId: Hex;
  projectId: ProjectId;
  scrope: ProjectUpdateScope[];
  time: UnixTime;
  projectTitle: string;
  // Project update scope
  roadmap: boolean;
  community: boolean;
  description: boolean;
  tags: boolean;
  title: boolean;
  slogan: boolean;
  summary: boolean;
  customUrl: boolean;
  logoImage: boolean;
  coverImages: boolean;
};
type ProjectUpdateAlertKey = string; // projectId|txId

const TASKS_PER_FETCH = 8;

discordProjectUpdateAlertIndexer.setup = $setup(async ({ sql }) => {
  await sql`
    CREATE TABLE IF NOT EXISTS discord.project_update_alert (
      tx_id TEXT NOT NULL,
      project_id TEXT NOT NULL,
      completed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      PRIMARY KEY (project_id, tx_id)
    )
  `;
});

export function discordProjectUpdateAlertIndexer(
  connections: ConnectionsWithDiscord
): PollingIndexer<DiscordAlertContext<ProjectUpdateAlertKey>> {
  return createPollingIndexer({
    name: "discord.project_update_alert",
    connections,
    triggers: { channels: ["discord.project_update_alert"] },
    concurrency: { workers: 1 },

    $id: ({ projectId, txId }: Task) => `${projectId}|${txId}`,

    fetch: async function () {
      const {
        connections: { sql },
        context: { ignored },
      } = this;
      const tasks = await sql<Task[]>`
        WITH update_list AS (
          SELECT * FROM (
            SELECT 
              pd.id,
              pd.project_id,
              pd.information_cid,
              LAG(information_cid) OVER (PARTITION BY pd.project_id ORDER BY pd.id) AS prev_information_cid
            FROM chain.project_detail pd
          ) AS _a
          WHERE information_cid IS DISTINCT FROM prev_information_cid 
        )
        SELECT 
          ru.project_id,
          ru.tx_id,
          ru.title AS project_title,
          ru.time,
          (ru.contents #> '{data, roadmap}' IS DISTINCT FROM ru.prev_contents #> '{data, roadmap}') AS roadmap, 
          (ru.contents #> '{data, community}' IS DISTINCT FROM ru.prev_contents #> '{data, community}') AS community, 
          (ru.contents #> '{data, description}' IS DISTINCT FROM ru.prev_contents #> '{data, description}') AS description, 
          (ru.contents #> '{data, basics, tags}' IS DISTINCT FROM ru.prev_contents #> '{data, basics, tags}') AS tags, 
          (ru.contents #> '{data, basics, title}' IS DISTINCT FROM ru.prev_contents #> '{data, basics, title}') AS title, 
          (ru.contents #> '{data, basics, slogan}' IS DISTINCT FROM ru.prev_contents #> '{data, basics, slogan}') AS slogan, 
          (ru.contents #> '{data, basics, summary}' IS DISTINCT FROM ru.prev_contents #> '{data, basics, summary}') AS summary, 
          (ru.contents #> '{data, basics, customUrl}' IS DISTINCT FROM ru.prev_contents #> '{data, basics, customUrl}') AS custom_url, 
          (ru.contents #> '{data, basics, logoImage}' IS DISTINCT FROM ru.prev_contents #> '{data, basics, logoImage}') AS logo_image, 
          (ru.contents #> '{data, basics, coverImages}' IS DISTINCT FROM ru.prev_contents #> '{data, basics, coverImages}') AS cover_images
        FROM (
          SELECT 
            ul.id,
            ul.project_id,
            b.time,
            o.tx_id,
            ul.information_cid AS cid,
            pi2.contents AS contents,
            pi2.title,
            LAG(contents) OVER (PARTITION BY ul.project_id ORDER BY ul.id) AS prev_contents
          FROM 
            update_list ul
            INNER JOIN ipfs.project_info pi2
              ON pi2.cid = ul.information_cid
            INNER JOIN chain.output o
              ON o.id = ul.id
            INNER JOIN chain.block b
              ON b.slot = o.created_slot
            LEFT JOIN discord.project_update_alert dpua
              ON (ul.project_id, o.tx_id) = (dpua.project_id, dpua.tx_id)
            WHERE
              dpua.tx_id IS NULL
              AND ${sqlNotIn(
                sql,
                "(ul.project_id, o.tx_id)",
                ignored.map((item) => {
                  const [projectId, txId] = item.split("|");
                  return sql([projectId, txId]);
                })
              )}
        ) AS ru
        WHERE
          ru.contents IS DISTINCT FROM ru.prev_contents
          AND ru.prev_contents IS NOT NULL
        LIMIT ${TASKS_PER_FETCH}
      `;
      return { tasks, continue: tasks.length >= TASKS_PER_FETCH };
    },

    handle: async function (task: WithId<Task, ProjectUpdateAlertKey>) {
      const {
        connections: { sql, discord },
        context: { ignored, cexplorerUrl, teikiHost },
      } = this;
      try {
        const { channelId } = this.context;
        // Limited at 256 characters
        const formattedProjectTitle = task.projectTitle.replace(
          /(.{120})..+/,
          "$1..."
        );
        const scope = ProjectUpdateScope.filter((item) => task[item]).map(
          (item) => DISPLAYED_SCOPE[item]
        );

        const embed = new EmbedBuilder()
          .setColor(0xf74055)
          .setTitle(
            `${formattedProjectTitle} has just updated ${scope.join(", ")}`
          )
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

        // TODO: Error handling?
        await sql`
          INSERT INTO discord.project_update_alert ${sql([
            { projectId: task.projectId, txId: task.txId },
          ])}
            ON CONFLICT DO NOTHING
        `;
      } catch (error) {
        // TODO: Better log here
        console.error("ERROR:", task.id, error);
        ignored.push(task.id);
      }
    },
  });
}
