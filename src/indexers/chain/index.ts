import { $setup, Setup } from "../../framework/base";
import {
  $handlers,
  BaseChainIndexConnections,
  ChainIndexer,
  setupGenesis,
} from "../../framework/chain";
import * as Staking from "../../framework/chain/staking";

import * as backing from "./backing";
import { KreateChainIndexContext } from "./context";
import * as deployed_scripts from "./deployed-scripts";
import * as kolours from "./kolours";
import * as migration from "./migration";
import * as project from "./project";
import * as protocol_params from "./protocol-params";
import * as teiki_plant from "./teiki-plant";
import * as treasury from "./treasury";

type KreateChainIndexEvent =
  | project.Event
  | backing.Event
  | treasury.Event
  | protocol_params.Event
  | deployed_scripts.Event
  | migration.Event
  | teiki_plant.Event
  | kolours.Event;

const creator = ChainIndexer.new<
  KreateChainIndexContext,
  KreateChainIndexEvent
>;

getChainIndexer.setup = $setup(async (resources) => {
  await setupGenesis(resources);
  await Promise.all(setups.map((setup) => setup(resources)));
});

const setups: Setup[] = [
  Staking.setup,
  project.setup,
  backing.setup,
  treasury.setup,
  protocol_params.setup,
  teiki_plant.setup,
  kolours.setup,
];

const $ = $handlers<KreateChainIndexContext>();

export async function getChainIndexer(connections: BaseChainIndexConnections) {
  const staking = Staking.createStakingIndexer({
    connections,
    onReloaded: ({ connections: { views } }) => {
      views.refresh("views.project_summary");
    },
  });
  const chainIndexer = await creator({
    connections,
    handlers: {
      initializers: [treasury.initialize],
      filters: [
        project.filter,
        backing.filter,
        treasury.filter,
        protocol_params.filter,
        deployed_scripts.filter,
        migration.filter,
        teiki_plant.filter,
        kolours.filter,
      ],
      events: {
        project: [project.projectEvent],
        project_detail: [project.projectDetailEvent],
        project_script: [project.projectScriptEvent],
        project_script$ceased: [],
        backing: [backing.event],
        dedicated_treasury: [treasury.dedicatedTreasuryEvent],
        shared_treasury: [treasury.sharedTreasuryEvent],
        open_treasury: [treasury.openTreasuryEvent],
        protocol_params: [protocol_params.event],
        deployed_scripts: [deployed_scripts.event],
        migration: [],
        teiki_plant: [teiki_plant.event],
        kolour$mint: [kolours.kolourEvent$Mint],
        kolour$transfer: [kolours.kolourEvent$Transfer],
        genesis_kreation$mint: [kolours.genesisKreationEvent$Mint],
        genesis_kreation$transfer: [kolours.genesisKreationEvent$Transfer],
      },
      rollbacks: [
        $.rollback(
          async ({
            driver,
            context: { staking },
            connections: { sql, views },
            action,
            point,
          }) => {
            staking.reboot();
            action != "begin" && staking.reload(null);
            views.refresh("views.project_custom_url");
            views.refresh("views.project_summary");
            action != "end" &&
              point !== "origin" &&
              (await kolours.confirm(sql, point.slot, driver));
          }
        ),
      ],
      onceInSync: async ({ context: { staking } }) => {
        // Staking.reboot(staking, sql);
        staking.toggleReloadDynamically(true);
        staking.reload(null);
      },
      afterBlock: async (params) => {
        await (params.inSync
          ? Promise.all([
              Staking.afterBlock(params),
              kolours.confirm(
                params.connections.sql,
                params.point.slot,
                params.driver
              ),
            ])
          : Staking.afterBlock(params));
      },
    },
  });
  return Object.assign(chainIndexer, { staking });
}
