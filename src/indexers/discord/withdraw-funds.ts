import { ActionRowBuilder, ButtonBuilder, EmbedBuilder } from "discord.js";
import { Lovelace, UnixTime } from "lucid-cardano";

import { Hex } from "@teiki/protocol/types";
import { assert } from "@teiki/protocol/utils";

import { $setup } from "../../framework/base";
import { createPollingIndexer, PollingIndexer } from "../../framework/polling";
import { shortenNumber } from "../../utils";

import { ConnectionsWithDiscord, DiscordAlertContext } from ".";

type ProjectId = string;
type Task = {
  txId: Hex;
  amount: Lovelace;
  withdrawnFunds: Lovelace;
  time: UnixTime;
  projectId: ProjectId;
  projectTitle: string;
};
type WithdrawFundsAlertKey = string; // projectId|txId

const TASKS_PER_FETCH = 8;

discordWithdrawFundsAlertIndexer.setup = $setup(async ({ sql }) => {
  await sql`
    CREATE TABLE IF NOT EXISTS discord.withdraw_funds_alert (
      tx_id varchar(64) NOT NULL,
      project_id varchar(64) NOT NULL,
      completed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      PRIMARY KEY (project_id, tx_id)
    )
  `;
});

export function discordWithdrawFundsAlertIndexer(
  connections: ConnectionsWithDiscord
): PollingIndexer<DiscordAlertContext> {
  return createPollingIndexer({
    name: "discord.withdraw_funds_alert",
    connections,
    triggers: {
      channels: ["staking:withdraw"],
    },
    concurrency: { workers: 1 },

    $id: ({ projectId, txId }: Task): WithdrawFundsAlertKey =>
      `${projectId}|${txId}`,

    fetch: async function () {
      const {
        connections: { sql },
      } = this;
      const tasks = await sql<Task[]>`
        SELECT
          x.tx_id,
          x.project_id,
          x.time,
          x.amount,
          pd.withdrawn_funds,
          pi.title AS project_title
        FROM (
          SELECT
            s.tx_id,
            COALESCE((s.payload #> '{amount}')::bigint, 0) AS amount,
            b.time,
            ps.project_id
          FROM
            chain.staking s
          INNER JOIN
            chain.project_script ps ON s.hash = ps.staking_script_hash
          INNER JOIN
            chain.block b ON b.slot = s.slot
        ) AS x
        LEFT JOIN
          discord.withdraw_funds_alert dwfa ON (dwfa.tx_id, dwfa.project_id) = (x.tx_id, x.project_id)
        INNER JOIN (
          SELECT
            *
          FROM
            chain.project_detail pd
            INNER JOIN chain.output o ON pd.id = o.id
          WHERE
            o.spent_slot IS NULL
        ) AS pd ON pd.project_id = x.project_id
        INNER JOIN ipfs.project_info pi ON pi.cid = pd.information_cid
        WHERE
          dwfa.tx_id IS NULL
          AND amount > 0
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
      withdrawnFunds,
      time,
    }) {
      const {
        connections: { sql, discord },
        context: { cexplorerUrl, teikiHost },
      } = this;
      try {
        const { channelId } = this.context;
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
          .addFields({
            name: "Total withdrawn funds",
            value: `${shortenNumber(withdrawnFunds, { shift: -6 })} ₳`,
            inline: true,
          })
          .setTimestamp(time);

        const links = new ActionRowBuilder()
          .addComponents(
            new ButtonBuilder()
              .setStyle(5)
              .setLabel("View project")
              .setURL(`${teikiHost}/projects-by-id/${projectId}`)
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
          INSERT INTO discord.withdraw_funds_alert ${sql([{ projectId, txId }])}
            ON CONFLICT DO NOTHING
        `;
      } catch (error) {
        // TODO: Better log here
        console.error("ERROR:", id, error);
        this.retry();
      }
    },
  });
}
