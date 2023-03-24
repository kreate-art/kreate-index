import * as config from "../../config";
import { Connections } from "../../connections";
import { VitalConnections } from "../../framework/polling";

// TODO: Proper failures handling
export type VitalDiscordConnections = VitalConnections & Connections<"discord">;

export type DiscordAlertContext = {
  channelId: string;
  shinkaRoleId: string;
  cexplorerUrl: string;
  kreateOrigin: string;
  discordIgnoredNotificationsBefore?: Date;
  ipfsGatewayUrl: string;
};

export function createDiscordAlertContext(
  channelId: string
): DiscordAlertContext {
  return {
    channelId,
    shinkaRoleId: config.discord().DISCORD_SHINKA_ROLE_ID,
    cexplorerUrl: config.cardano().CEXPLORER_URL,
    kreateOrigin: config.kreate().KREATE_ORIGIN,
    discordIgnoredNotificationsBefore:
      config.discord().DISCORD_IGNORE_NOTIFICATIONS_BEFORE,
    ipfsGatewayUrl: config.ai().IPFS_GATEWAY_URL,
  };
}
