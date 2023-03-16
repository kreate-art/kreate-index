import { ActionRowBuilder, ButtonBuilder, EmbedBuilder } from "discord.js";
import { UnixTime } from "lucid-cardano";

import { Hex } from "@kreate/protocol/types";
import { assert } from "@kreate/protocol/utils";

import { TASKS_PER_FETCH } from "../../constants";
import { $setup } from "../../framework/base";
import { createPollingIndexer, PollingIndexer } from "../../framework/polling";
import { Lovelace } from "../../types/chain";
import {
  DISPLAYED_SCOPE,
  ProjectUpdateScope,
} from "../../types/project/update";
import { WithId } from "../../types/typelevel";
import { shortenNumber } from "../../utils";

import { DiscordAlertContext, VitalDiscordConnections } from "./base";

type ProjectId = string;
type Task = {
  projectId: ProjectId;
  txId: Hex;
  time: UnixTime;
  projectTitle: string;
  sponsorshipAmount: Lovelace;
  // Project update scope
  benefits: boolean;
  community: boolean;
  description: boolean;
  tags: boolean;
  title: boolean;
  slogan: boolean;
  summary: boolean;
  customUrl: boolean;
  logoImage: boolean;
  coverImages: boolean;
  sponsorship: boolean;
};
type ProjectUpdateAlertKey = string; // projectId|txId

discordProjectUpdateAlertIndexer.setup = $setup(async ({ sql }) => {
  await sql`
    CREATE TABLE IF NOT EXISTS discord.project_update_alert (
      project_id varchar(64) NOT NULL,
      tx_id varchar(64) NOT NULL,
      completed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      PRIMARY KEY (project_id, tx_id)
    )
  `;
});

export function discordProjectUpdateAlertIndexer(
  connections: VitalDiscordConnections
): PollingIndexer<DiscordAlertContext> {
  return createPollingIndexer({
    name: "discord.project_update_alert",
    connections,
    triggers: { channels: ["discord.project_update_alert"] },
    concurrency: { workers: 1 },

    $id: ({ projectId, txId }: Task): ProjectUpdateAlertKey =>
      `${projectId}|${txId}`,

    fetch: async function () {
      const {
        connections: { sql },
        context: { discordIgnoredNotificationsBefore },
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
              LAG(pd.information_cid) OVER w AS prev_information_cid,
              LAG(pd.sponsorship_amount) OVER w AS prev_sponsorship_amount,
              LAG(pd.sponsorship_until) OVER w AS prev_sponsorship_until
            FROM chain.project_detail pd
            WHERE
              EXISTS (SELECT FROM ipfs.project_info pi WHERE pi.cid = pd.information_cid)
            WINDOW w AS (PARTITION BY project_id ORDER BY id)
          ) AS _a
          WHERE
            information_cid IS DISTINCT FROM prev_information_cid
            OR sponsorship_amount IS DISTINCT FROM prev_sponsorship_amount
            OR sponsorship_until IS DISTINCT FROM prev_sponsorship_until
        ),
        update_list_prev AS (
          SELECT
            ul.*,
            pi2.contents AS contents,
            pi2.title,
            LAG(contents) OVER (PARTITION BY ul.project_id ORDER BY ul.id) AS prev_contents
          FROM
            update_list ul
          INNER JOIN ipfs.project_info pi2
            ON pi2.cid = ul.information_cid
        ),
        x AS (
          SELECT
            ulp.*,
            o.tx_id,
            b.time
          FROM
            update_list_prev ulp
            INNER JOIN chain.output o
              ON o.id = ulp.id
            INNER JOIN chain.block b
              ON b.slot = o.created_slot
            WHERE
              NOT EXISTS (
                SELECT FROM discord.project_update_alert dpua
                WHERE
                  (ulp.project_id, o.tx_id) = (dpua.project_id, dpua.tx_id)
              )
        )
        SELECT
          x.project_id,
          x.tx_id,
          x.title AS project_title,
          x.time,
          x.sponsorship_amount,
          (x.contents #> '{data, benefits}' IS DISTINCT FROM x.prev_contents #> '{data, benefits}') AS benefits,
          (x.contents #> '{data, community}' IS DISTINCT FROM x.prev_contents #> '{data, community}') AS community,
          (x.contents #> '{data, description}' IS DISTINCT FROM x.prev_contents #> '{data, description}') AS description,
          (x.contents #> '{data, basics, tags}' IS DISTINCT FROM x.prev_contents #> '{data, basics, tags}') AS tags,
          (x.contents #> '{data, basics, title}' IS DISTINCT FROM x.prev_contents #> '{data, basics, title}') AS title,
          (x.contents #> '{data, basics, slogan}' IS DISTINCT FROM x.prev_contents #> '{data, basics, slogan}') AS slogan,
          (x.contents #> '{data, basics, summary}' IS DISTINCT FROM x.prev_contents #> '{data, basics, summary}') AS summary,
          (x.contents #> '{data, basics, customUrl}' IS DISTINCT FROM x.prev_contents #> '{data, basics, customUrl}') AS custom_url,
          (x.contents #> '{data, basics, logoImage}' IS DISTINCT FROM x.prev_contents #> '{data, basics, logoImage}') AS logo_image,
          (x.contents #> '{data, basics, coverImages}' IS DISTINCT FROM x.prev_contents #> '{data, basics, coverImages}') AS cover_images,
          (x.sponsorship_amount IS DISTINCT FROM x.prev_sponsorship_amount
            OR x.sponsorship_until IS DISTINCT FROM x.prev_sponsorship_until) AS sponsorship
        FROM x
        WHERE
          x.prev_contents IS NOT NULL
          AND ${
            discordIgnoredNotificationsBefore
              ? sql`${discordIgnoredNotificationsBefore} <= x.time `
              : sql`TRUE`
          }
        ORDER BY
          x.id
        LIMIT ${TASKS_PER_FETCH}
      `;
      return { tasks, continue: tasks.length >= TASKS_PER_FETCH };
    },

    handle: async function (task: WithId<Task, ProjectUpdateAlertKey>) {
      const {
        connections: { sql, discord },
        context: { channelId, cexplorerUrl, kreateOrigin },
      } = this;
      try {
        // Limited at 256 characters
        const formattedProjectTitle = task.projectTitle.replace(
          /(.{100})..+/,
          "$1..."
        );
        const scope = ProjectUpdateScope.filter((item) => !!task[item]).map(
          (item) => DISPLAYED_SCOPE[item]
        );
        if (task.sponsorship) {
          scope.push(
            `sponsorship to ${shortenNumber(task.sponsorshipAmount, {
              shift: -6,
            })} ₳`
          );
        }

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
        this.retry();
      }
    },
  });
}
