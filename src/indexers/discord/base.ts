import * as config from "../../config";
import { Connections } from "../../connections";
import { VitalConnections } from "../../framework/polling";

// TODO: Proper failures handling
export type VitalDiscordConnections = VitalConnections & Connections<"discord">;

export type DiscordAlertContext = {
  channelId: string;
  shinkaRoleId: string;
  cexplorerUrl: string;
  teikiHost: string;
  discordIgnoredNotificationsBefore?: Date;
};

export function createDiscordAlertContext(
  channelId: string
): DiscordAlertContext {
  return {
    channelId,
    shinkaRoleId: config.discord().DISCORD_SHINKA_ROLE_ID,
    cexplorerUrl: config.cardano().CEXPLORER_URL,
    teikiHost: config.teiki().TEIKI_HOST,
    discordIgnoredNotificationsBefore:
      config.discord().DISCORD_IGNORE_NOTIFICATIONS_BEFORE,
  };
}
