import { ActionRowBuilder, ButtonBuilder, EmbedBuilder } from "discord.js";
import { PoolId, UnixTime } from "lucid-cardano";

import { Hex } from "@teiki/protocol/types";
import { assert } from "@teiki/protocol/utils";

import { $setup } from "../../framework/base";
import { createPollingIndexer, PollingIndexer } from "../../framework/polling";

import { VitalDiscordConnections, DiscordAlertContext } from "./base";

type ProjectId = string;
type Task = {
  projectId: ProjectId;
  txId: Hex;
  poolId: PoolId;
  projectTitle: string;
  time: UnixTime;
};
type DelegationAlertKey = string; // projectId|txId

const TASKS_PER_FETCH = 20;

discordDelegationAlertIndexer.setup = $setup(async ({ sql }) => {
  await sql`
    CREATE TABLE IF NOT EXISTS discord.delegation_alert (
      tx_id varchar(64) NOT NULL,
      project_id varchar(64) NOT NULL,
      completed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      PRIMARY KEY (project_id, tx_id)
    )
  `;
});

export function discordDelegationAlertIndexer(
  connections: VitalDiscordConnections
): PollingIndexer<DiscordAlertContext> {
  return createPollingIndexer({
    name: "discord.delegation_alert",
    connections,
    triggers: { channels: ["staking:delegate"] },
    concurrency: { workers: 1 },

    $id: ({ projectId, txId }: Task): DelegationAlertKey =>
      `${projectId}|${txId}`,

    fetch: async function () {
      const {
        connections: { sql },
      } = this;
      const tasks = await sql<Task[]>`
        SELECT
          x.project_id,
          x.tx_id,
          x.pool_id,
          pi.title AS project_title,
          x.time
        FROM (
          SELECT
            s.id,
            ps.project_id,
            s.tx_id,
            (s.payload ->> 'poolId') AS pool_id,
            b.time
          FROM
            chain.staking s
          INNER JOIN
            chain.project_script ps ON s.hash = ps.staking_script_hash
          INNER JOIN
            chain.block b ON b.slot = s.slot
          WHERE s.action = 'delegate'
          ORDER BY s.id
        ) AS x
        INNER JOIN (
          SELECT
            *
          FROM
            chain.project_detail pd
          INNER JOIN
            chain.output o ON pd.id = o.id
          WHERE
            o.spent_slot IS NULL
        ) AS pd ON pd.project_id = x.project_id
        INNER JOIN
          ipfs.project_info pi ON pi.cid = pd.information_cid
        WHERE
          NOT EXISTS (
            SELECT FROM discord.delegation_alert dda
            WHERE (dda.project_id, dda.tx_id) = (x.project_id, x.tx_id)
          )
        ORDER BY x.id
        LIMIT ${TASKS_PER_FETCH}
      `;
      return { tasks, continue: tasks.length >= TASKS_PER_FETCH };
    },

    handle: async function ({
      id,
      projectId,
      txId,
      poolId,
      projectTitle,
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
          /(.{150})..+/,
          "$1..."
        );
        const embed = new EmbedBuilder()
          .setColor(0x00362c)
          .setTitle(
            `${formattedProjectTitle} has just been delegated to pool ${poolId}`
          )
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
          )
          .addComponents(
            new ButtonBuilder()
              .setStyle(5)
              .setLabel("View pool")
              .setURL(`${cexplorerUrl}/pool/${poolId}`)
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
          INSERT INTO discord.delegation_alert ${sql({ projectId, txId })}
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
