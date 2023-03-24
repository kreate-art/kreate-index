import { EmbedBuilder, Message } from "discord.js";
import { Address, Lovelace, UnixTime } from "lucid-cardano";

import { Hex } from "@kreate/protocol/types";
import { assert } from "@kreate/protocol/utils";

import { $setup } from "../../framework/base";
import { createPollingIndexer, PollingIndexer } from "../../framework/polling";
import { shortenNumber } from "../../utils";
import { KolourStatuses } from "../chain/kolours";

import { VitalDiscordConnections, DiscordAlertContext } from "./base";

type Task = {
  bookId: bigint;
  kreation: string;
  status: (typeof KolourStatuses)[number];
  txId: Hex;
  listedFee: Lovelace;
  userAddress: Address;
  name: string;
  description: string;
  time: UnixTime;
  imageCid: string;
  messageId: string;
};
type GenesisKreationNftAlertKey = string; // id|status

const TASKS_PER_FETCH = 20;

discordGenesisKreationNftAlertIndexer.setup = $setup(async ({ sql }) => {
  await sql`
    CREATE TABLE IF NOT EXISTS discord.genesis_kreation_nft_alert (
      book_id bigint NOT NULL,
      status kolours.status NOT NULL,
      message_id text,
      PRIMARY KEY (book_id, status)
    )
  `;
});

export function discordGenesisKreationNftAlertIndexer(
  connections: VitalDiscordConnections
): PollingIndexer<DiscordAlertContext> {
  return createPollingIndexer({
    name: "discord.genesis_kreation_nft_alert",
    connections,
    triggers: { channels: ["discord.genesis_kreation_nft_alert"] },
    concurrency: { workers: 1 },

    $id: ({ bookId, status }: Task): GenesisKreationNftAlertKey =>
      `${bookId}|${status}`,

    fetch: async function () {
      const {
        connections: { sql },
        context: { discordIgnoredNotificationsBefore },
      } = this;
      const tasks = await sql<Task[]>`
        SELECT
          gkb.id as book_id,
          gkb.kreation,
          gkb.status,
          gkb.tx_id,
          gkb.listed_fee,
          gkb.user_address,
          gkb.name,
          gkb.description,
          gkl.final_image_cid as image_cid,
          x.message_id,
          b.time
        FROM
          kolours.genesis_kreation_book gkb
        INNER JOIN
          kolours.genesis_kreation_mint gkm ON gkm.tx_id = gkb.tx_id
        INNER JOIN
          chain.block b ON gkm.slot = b.slot
        INNER JOIN
          kolours.genesis_kreation_list gkl ON gkl.kreation = gkb.kreation
        LEFT JOIN (
          SELECT DISTINCT ON (book_id)
            book_id,
            message_id
          FROM
            discord.genesis_kreation_nft_alert
        ) AS x ON x.book_id = gkb.id
        WHERE
          NOT EXISTS (
            SELECT FROM discord.genesis_kreation_nft_alert kna
            WHERE
              (kna.book_id, kna.status) = (gkb.id, gkb.status)
          )
          AND ${
            discordIgnoredNotificationsBefore
              ? sql`${discordIgnoredNotificationsBefore} <= b.time`
              : sql`TRUE`
          }
        ORDER BY gkb.id
        LIMIT ${TASKS_PER_FETCH}
      `;
      return { tasks, continue: tasks.length >= TASKS_PER_FETCH };
    },

    handle: async function ({
      id,
      bookId,
      kreation,
      status,
      listedFee,
      userAddress,
      imageCid,
      name,
      description,
      time,
      messageId,
    }) {
      const {
        connections: { sql, discord },
        context: { channelId, ipfsGatewayUrl },
      } = this;

      const channel = await discord.channels.fetch(channelId);
      let initialMessageId = null;

      const embed = new EmbedBuilder()
        .setTitle(`${kreation} was ${status}!`)
        .addFields({ name: "Owner", value: `${userAddress}` })
        .addFields({ name: "Name", value: `${name}` })
        .addFields({ name: "Description", value: `${description}` })
        .addFields({
          name: "Price",
          value: `${shortenNumber(listedFee, { shift: -6 })} ₳`,
        })
        .setColor("#ffff00")
        .setImage(`${ipfsGatewayUrl}/ipfs/${imageCid}`)
        .setTimestamp(time);

      try {
        if (messageId == null) {
          const channel = await discord.channels.fetch(channelId);
          assert(channel, `Channel ${channelId} not found`);
          assert("send" in channel, `Channel ${channelId} is not sendable`);
          const message = await channel.send({ embeds: [embed] });
          initialMessageId = message.id;
        } else {
          const message: Message<false> | Message<true> =
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            await (channel as any).messages.fetch(messageId);
          assert("edit" in message, `Message ${messageId} is not editable`);
          message.edit({ embeds: [embed] });
        }

        // TODO: Error handling?
        await sql`
          INSERT INTO discord.genesis_kreation_nft_alert ${sql([
            { bookId, status, messageId: messageId ?? initialMessageId },
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
