import { ActionRowBuilder, ButtonBuilder, EmbedBuilder } from "discord.js";
import { Address, Lovelace, UnixTime } from "lucid-cardano";

import { Hex } from "@teiki/protocol/types";
import { assert } from "@teiki/protocol/utils";

import { $setup } from "../../framework/base";
import { createPollingIndexer, PollingIndexer } from "../../framework/polling";
import { BackingActionType } from "../../types/backing";
import { shortenNumber } from "../../utils";

import { VitalDiscordConnections, DiscordAlertContext } from "./base";

type ProjectId = string;
type Task = {
  projectId: ProjectId;
  actorAddress: Address;
  txId: Hex;
  amount: Lovelace;
  action: BackingActionType;
  message: string;
  time: UnixTime;
  projectTitle: string;
};
type BackingAlertKey = string; // txId|projectId|actorAddress

const TASKS_PER_FETCH = 20;

discordBackingAlertIndexer.setup = $setup(async ({ sql }) => {
  await sql`
    CREATE TABLE IF NOT EXISTS discord.backing_alert (
      project_id varchar(64) NOT NULL,
      actor_address text NOT NULL,
      tx_id varchar(64) NOT NULL,
      completed_at timestamptz NOT NULL DEFAULT NOW(),
      PRIMARY KEY (project_id, actor_address, tx_id)
    )
  `;
});

export function discordBackingAlertIndexer(
  connections: VitalDiscordConnections
): PollingIndexer<DiscordAlertContext> {
  return createPollingIndexer({
    name: "discord.backing_alert",
    connections,
    triggers: { channels: ["discord.backing_alert"] },
    concurrency: { workers: 1 },

    $id: ({ projectId, actorAddress, txId }: Task): BackingAlertKey =>
      `${projectId}|${actorAddress}|${txId}`,

    fetch: async function () {
      const {
        connections: { sql },
        context: { discordIgnoredNotificationsBefore },
      } = this;
      const tasks = await sql<Task[]>`
        SELECT
          ba.tx_id,
          ba.project_id,
          ba.actor_address,
          ba.action,
          ba.amount,
          ba.message,
          ba.time,
          pi.title as project_title
        FROM
          chain.backing_action ba
        INNER JOIN (
          SELECT
            *
          FROM
            chain.project_detail pd
            INNER JOIN chain.output o ON pd.id = o.id
          WHERE
            o.spent_slot IS NULL
        ) AS pd ON pd.project_id = ba.project_id
        INNER JOIN ipfs.project_info pi ON pi.cid = pd.information_cid
        WHERE
          NOT EXISTS (
            SELECT FROM discord.backing_alert dba
            WHERE
              (dba.project_id, dba.actor_address, dba.tx_id)
              = (ba.project_id, ba.actor_address, ba.tx_id)
          )
          AND ${
            discordIgnoredNotificationsBefore == null
              ? sql`TRUE`
              : sql`${discordIgnoredNotificationsBefore} <= ba.time`
          }
        ORDER BY ba.id
        LIMIT ${TASKS_PER_FETCH}
      `;
      return { tasks, continue: tasks.length >= TASKS_PER_FETCH };
    },

    handle: async function ({
      id,
      txId,
      projectId,
      actorAddress,
      amount,
      action,
      message,
      time,
      projectTitle,
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
          .setColor(action === "back" ? 0x006e46 : 0xff7a00)
          .setTitle(`${formattedProjectTitle} has just been ${action}ed`)
          .addFields({
            name: "Amount",
            value: `${shortenNumber(amount, { shift: -6 })} ₳`,
            inline: true,
          })
          .addFields({
            name: "Message",
            value: message || "-",
            inline: true,
          })
          .addFields({ name: "Backer", value: actorAddress, inline: false })
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
          INSERT INTO discord.backing_alert ${sql([
            { projectId, actorAddress, txId },
          ])}
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
