import { ActionRowBuilder, ButtonBuilder, EmbedBuilder } from "discord.js";
import { Lovelace, UnixTime } from "lucid-cardano";

import { Hex } from "@teiki/protocol/types";
import { assert } from "@teiki/protocol/utils";

import { TEIKI_HOST } from "../../config";
import { sqlNotIn } from "../../db/fragments";
import { $setup } from "../../framework/base";
import { createPollingIndexer, PollingIndexer } from "../../framework/polling";

import { shortenNumber } from "./utils";

import { ConnectionsWithDiscord, DiscordAlertContext } from ".";

type ProjectId = string;
type Task = {
  txId: Hex;
  amount: Lovelace;
  time: UnixTime;
  projectId: ProjectId;
  projectTitle: string;
};
type WithdrawFundsAlertKey = string; // txId|projectId
type DiscordAlertContext$WithdrawFunds = DiscordAlertContext & {
  ignored: WithdrawFundsAlertKey[];
};

const TASKS_PER_FETCH = 8;

discordWithdrawFundsAlertIndexer.setup = $setup(async ({ sql }) => {
  await sql`
    CREATE TABLE IF NOT EXISTS discord.withdraw_funds_alert (
      tx_id TEXT NOT NULL,
      project_id TEXT NOT NULL,
      completed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      PRIMARY KEY (tx_id, project_id)
    )
  `;
});

export function discordWithdrawFundsAlertIndexer(
  connections: ConnectionsWithDiscord
): PollingIndexer<DiscordAlertContext$WithdrawFunds> {
  return createPollingIndexer({
    name: "discord.withdraw_funds_alert",
    connections,
    triggers: { channels: ["discord.withdraw_funds_alert"] },
    concurrency: { workers: 1 },

    $id: ({ txId, projectId }: Task) => `${txId}|${projectId}`,

    fetch: async function () {
      const {
        connections: { sql },
        context: { ignored },
      } = this;
      const tasks = await sql<Task[]>`
        SELECT
          x.tx_id,
          x.project_id,
          x.amount,
          pi.title as project_title,
          b.time
        FROM (
          SELECT
            o.tx_id,
            o.created_slot,
            ppd.project_id,
            ppd.information_cid,
            (ppd.withdrawn_funds - ppd.prev_withdrawn_funds) AS amount
          FROM (
            SELECT
              pd.id,
              pd.project_id,
              pd.information_cid,
              pd.withdrawn_funds,
              LAG (pd.withdrawn_funds) OVER (
                PARTITION BY pd.project_id
                ORDER BY
                  id
              ) AS prev_withdrawn_funds
            FROM
              chain.project_detail pd
          ) ppd
          INNER JOIN chain.output o ON o.id = ppd.id
          WHERE
            ppd.prev_withdrawn_funds IS NOT NULL
            AND ppd.withdrawn_funds IS DISTINCT
          FROM
            ppd.prev_withdrawn_funds
        ) AS x
        LEFT JOIN discord.withdraw_funds_alert dwfa ON (dwfa.tx_id, dwfa.project_id) = (x.tx_id, x.project_id)
        INNER JOIN ipfs.project_info pi ON pi.cid = x.information_cid
        INNER JOIN chain.block b ON b.slot = x.created_slot
        WHERE
          dwfa.project_id IS NULL
          AND ${sqlNotIn(
            sql,
            "(x.tx_id, x.project_id)",
            ignored.map((item) => {
              const [txId, projectId] = item.split("|");
              return sql([txId, projectId]);
            })
          )}
        LIMIT ${TASKS_PER_FETCH}
      `;
      return { tasks, continue: tasks.length >= TASKS_PER_FETCH };
    },

    handle: async function ({
      id,
      txId,
      projectId,
      projectTitle,
      amount,
      time,
    }) {
      const {
        connections: { sql, discord },
        context: { cexplorerUrl, ignored },
      } = this;
      try {
        const { notificationChannelId: channelId } = this.context;
        // Limited at 256 characters
        const formattedProjectTitle = projectTitle.replace(
          /(.{200})..+/,
          "$1..."
        );
        const embed = new EmbedBuilder()
          .setColor(0xd200e4)
          .setTitle(`${formattedProjectTitle} has just withdrawn funds`)
          .addFields({
            name: "Amount",
            value: `${shortenNumber(amount, { shift: -6 })} ₳`,
            inline: true,
          })
          .setTimestamp(time);

        const links = new ActionRowBuilder()
          .addComponents(
            new ButtonBuilder()
              .setStyle(5)
              .setLabel("View project")
              .setURL(`${TEIKI_HOST}/projects-by-id/${projectId}`)
          )
          .addComponents(
            new ButtonBuilder()
              .setStyle(5)
              .setLabel("View transaction")
              .setURL(`${cexplorerUrl}/tx/${txId}`)
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
          INSERT INTO discord.withdraw_funds_alert ${sql([{ txId, projectId }])}
            ON CONFLICT DO NOTHING
        `;
      } catch (error) {
        // TODO: Better log here
        console.error("ERROR:", id, error);
        ignored.push(id);
      }
    },
  });
}
