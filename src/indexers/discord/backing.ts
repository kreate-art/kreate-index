import { ActionRowBuilder, ButtonBuilder, EmbedBuilder } from "discord.js";
import { Address, Lovelace, UnixTime } from "lucid-cardano";

import { Hex } from "@teiki/protocol/types";
import { assert } from "@teiki/protocol/utils";

import { sqlNotIn } from "../../db/fragments";
import { $setup } from "../../framework/base";
import { createPollingIndexer, PollingIndexer } from "../../framework/polling";
import { BackingActionType } from "../../types/backing";

import { shortenNumber } from "./utils";

import { ConnectionsWithDiscord, DiscordAlertContext } from ".";

type ProjectId = string;
type Task = {
  txId: Hex;
  projectId: ProjectId;
  actorAddress: Address;
  amount: Lovelace;
  action: BackingActionType;
  message: string;
  time: UnixTime;
  projectTitle: string;
};
type BackingAlertKey = string; // txId|projectId|actorAddress

const TASKS_PER_FETCH = 8;

discordBackingAlertIndexer.setup = $setup(async ({ sql }) => {
  await sql`
    CREATE TABLE IF NOT EXISTS discord.backing_alert (
      tx_id TEXT NOT NULL,
      project_id TEXT NOT NULL,
      actor_address TEXT NOT NULL,
      completed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      PRIMARY KEY (tx_id, project_id, actor_address)
      -- TODO: Update PK so that project_id is first
    )
  `;
});

export function discordBackingAlertIndexer(
  connections: ConnectionsWithDiscord
): PollingIndexer<DiscordAlertContext<BackingAlertKey>> {
  return createPollingIndexer({
    name: "discord.backing_alert",
    connections,
    triggers: { channels: ["discord.backing_alert"] },
    concurrency: { workers: 1 },

    $id: ({ txId, projectId, actorAddress }: Task) =>
      `${txId}|${projectId}|${actorAddress}`,

    fetch: async function () {
      const {
        connections: { sql },
        context: { ignored },
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
        LEFT JOIN
          discord.backing_alert dba
          ON (dba.tx_id, dba.project_id, dba.actor_address)
               = (ba.tx_id, ba.project_id, ba.actor_address)
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
          dba.tx_id IS NULL
          AND ${sqlNotIn(
            sql,
            "(ba.tx_id, ba.project_id, ba.actor_address)",
            ignored.map((item) => {
              const [txId, projectId, actorAddress] = item.split("|");
              return sql([txId, projectId, actorAddress]);
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
      actorAddress,
      amount,
      action,
      message,
      time,
      projectTitle,
    }) {
      const {
        connections: { sql, discord },
        context: { ignored, cexplorerUrl, teikiHost },
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
            { txId, projectId, actorAddress },
          ])}
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
