import { Events } from "discord.js";

import { Connections } from "../../connections";
import { VitalConnections } from "../../framework/polling";

// TODO: Proper failures handling
export type ConnectionsWithDiscord = VitalConnections & Connections<"discord">;

export type DiscordAlertContext = {
  contentModerationChannelId: string;
  shinkaRoleId: string;
};

export function startDiscordBotInteractionListener(
  { sql, discord }: ConnectionsWithDiscord,
  { contentModerationChannelId }: DiscordAlertContext
) {
  discord.once(Events.ClientReady, async (c) => {
    console.log(`Logged in as ${c.user.tag}!`);
  });

  discord.on(Events.InteractionCreate, async (i) => {
    try {
      if (i.isButton()) {
        if (i.channelId !== contentModerationChannelId) return;

        const [action, projectId] = i.customId.split("-");
        const actionUser = `${i.member?.user.username}#${i.member?.user.discriminator}`;

        if (action === "unblock") {
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
        } else if (action === "block") {
          await sql`
            INSERT INTO
              admin.blocked_project ${sql({ projectId })}
            ON CONFLICT DO NOTHING
          `;
          await i.update({
            content: `${i.message.content} \n\n Blocked by ${actionUser}`,
            components: i.message.components,
          });
        }
      }
    } catch (e) {
      // FIXME: Proper error handling!
      // We are catching to keep the bot alive.
      console.log(e);
    }
  });
}
