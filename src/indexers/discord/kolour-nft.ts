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
  kolour: Hex;
  status: (typeof KolourStatuses)[number];
  txId: Hex;
  listedFee: Lovelace;
  userAddress: Address;
  time: UnixTime;
  imageCid: string;
  messageId: string;
};
type KolourNftAlertKey = string; // id|status

const TASKS_PER_FETCH = 20;

discordKolourNftAlertIndexer.setup = $setup(async ({ sql }) => {
  await sql`
    CREATE TABLE IF NOT EXISTS discord.kolour_nft_alert (
      book_id bigint NOT NULL,
      status kolours.status NOT NULL,
      message_id text,
      PRIMARY KEY (book_id, status)
    )
  `;
});

export function discordKolourNftAlertIndexer(
  connections: VitalDiscordConnections
): PollingIndexer<DiscordAlertContext> {
  return createPollingIndexer({
    name: "discord.kolour_nft_alert",
    connections,
    triggers: { channels: ["discord.kolour_nft_alert"] },
    concurrency: { workers: 1 },

    $id: ({ bookId, status }: Task): KolourNftAlertKey => `${bookId}|${status}`,

    fetch: async function () {
      const {
        connections: { sql },
        context: { discordIgnoredNotificationsBefore },
      } = this;
      const tasks = await sql<Task[]>`
        SELECT
          kb.id as book_id,
          kb.kolour,
          kb.status,
          kb.tx_id,
          kb.listed_fee,
          kb.user_address,
          kb.image_cid,
          x.message_id,
          b.time
        FROM
          kolours.kolour_book kb
        INNER JOIN
          chain.output o ON kb.tx_id = o.tx_id
        INNER JOIN
          chain.block b ON o.created_slot = b.slot
        LEFT JOIN (
          SELECT DISTINCT ON (book_id)
            book_id,
            message_id
          FROM
            discord.kolour_nft_alert
        ) AS x ON x.book_id = kb.id
        WHERE
          NOT EXISTS (
            SELECT FROM discord.kolour_nft_alert kna
            WHERE
              (kna.book_id, kna.status) = (kb.id, kb.status)
          )
          AND ${
            discordIgnoredNotificationsBefore
              ? sql`${discordIgnoredNotificationsBefore} <= b.time`
              : sql`TRUE`
          }
        ORDER BY o.id
        LIMIT ${TASKS_PER_FETCH}
      `;
      return { tasks, continue: tasks.length >= TASKS_PER_FETCH };
    },

    handle: async function ({
      id,
      bookId,
      kolour,
      status,
      listedFee,
      userAddress,
      imageCid,
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
        .setTitle(`#${kolour} was ${status}!`)
        .addFields({ name: "Owner", value: `${userAddress}` })
        .addFields({
          name: "Price",
          value: `${shortenNumber(listedFee, { shift: -6 })} ₳`,
        })
        .setColor(`#${kolour}`)
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
          INSERT INTO discord.kolour_nft_alert ${sql([
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
