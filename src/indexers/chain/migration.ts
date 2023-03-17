import { fromText } from "lucid-cardano";

import { $handlers } from "../../framework/chain";

import { KreateChainIndexContext } from "./context";

export type Event = { type: "migration" };
const $ = $handlers<KreateChainIndexContext, Event>();

const MIGRATION_ASSET_SUFFIX = "." + fromText("migration");

export const filter = $.filter(({ tx }) => {
  // TODO: Just a hotfix...
  const minted = tx.body.mint.assets;
  if (minted)
    for (const asset of Object.keys(minted))
      if (asset.endsWith(MIGRATION_ASSET_SUFFIX))
        return [{ type: "migration" }];
  return null;
});
