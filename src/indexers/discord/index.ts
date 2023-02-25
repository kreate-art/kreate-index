import { Events } from "discord.js";

import * as config from "../../config";
import { Connections } from "../../connections";
import { VitalConnections } from "../../framework/polling";

// TODO: Proper failures handling
export type ConnectionsWithDiscord = VitalConnections & Connections<"discord">;

export type DiscordAlertContext = {
  channelId: string;
  shinkaRoleId: string;
  cexplorerUrl: string;
  teikiHost: string;
};

// TODO: This function shouldn't be called directly by any indexer.
export function startDiscordBotInteractionListener(
  { sql, discord }: ConnectionsWithDiscord,
  { channelId }: DiscordAlertContext
) {
  discord.once(Events.ClientReady, async (c) => {
    console.log(`Logged in as ${c.user.tag}!`);
  });

  discord.on(Events.InteractionCreate, async (i) => {
    try {
      if (i.isButton()) {
        if (i.channelId !== channelId) return;

        const [action, projectId] = i.customId.split("-");
        const actionUser = `${i.member?.user.username}#${i.member?.user.discriminator}`;

        switch (action) {
          case "unblock":
            await sql`
            DELETE FROM
              ADMIN.blocked_project
            WHERE
              project_id = ${projectId};
          `;
            await i.update({
              content: `${i.message.content} \n\n Unblocked by ${actionUser}`,
              components: i.message.components,
            });
            return;
          case "block":
            await sql`
            INSERT INTO
              admin.blocked_project ${sql({ projectId })}
            ON CONFLICT DO NOTHING
          `;
            await i.update({
              content: `${i.message.content} \n\n Blocked by ${actionUser}`,
              components: i.message.components,
            });
            return;
          default:
            return;
        }
      }
    } catch (e) {
      // FIXME: Proper error handling!
      // We are catching to keep the bot alive.
      console.log(e);
    }
  });
}

export function createDiscordAlertContext(
  channelId: string
): DiscordAlertContext {
  return {
    channelId,
    shinkaRoleId: config.discord().DISCORD_SHINKA_ROLE_ID,
    cexplorerUrl: config.cardano().CEXPLORER_URL,
    teikiHost: config.teiki().TEIKI_HOST,
  };
}
