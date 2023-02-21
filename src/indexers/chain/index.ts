import { Sql } from "../../db";
import { $setup, Setup } from "../../framework/base";
import {
  $handlers,
  BaseChainIndexConnections,
  ChainIndexer,
  setupGenesis,
} from "../../framework/chain";
import * as staking from "../../framework/chain/staking";
import { StakingIndexer, StakingHash } from "../../framework/chain/staking";

import * as backing from "./backing";
import { TeikiChainIndexContext } from "./context";
import * as deployed_scripts from "./deployed-scripts";
import * as migration from "./migration";
import * as project from "./project";
import * as protocol_params from "./protocol-params";
import * as teiki_plant from "./teiki-plant";
import * as treasury from "./treasury";

type TeikiChainIndexEvent =
  | project.Event
  | backing.Event
  | treasury.Event
  | staking.Event
  | protocol_params.Event
  | deployed_scripts.Event
  | migration.Event
  | teiki_plant.Event;

const creator = ChainIndexer.new<TeikiChainIndexContext, TeikiChainIndexEvent>;

getChainIndexer.setup = $setup(async (resources) => {
  await setupGenesis(resources);
  await Promise.all(setups.map((setup) => setup(resources)));
});

const setups: Setup[] = [
  project.setup,
  backing.setup,
  treasury.setup,
  staking.setup,
  protocol_params.setup,
  teiki_plant.setup,
];

const $ = $handlers<TeikiChainIndexContext>();

export async function getChainIndexer(connections: BaseChainIndexConnections) {
  const stakingIndexer = staking.createStakingIndexer({
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
        staking.filter,
        protocol_params.filter,
        deployed_scripts.filter,
        migration.filter,
        teiki_plant.filter,
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
        staking: [staking.event],
        protocol_params: [protocol_params.event],
        deployed_scripts: [deployed_scripts.event],
        migration: [],
        teiki_plant: [teiki_plant.event],
      },
      rollbacks: [
        $.rollback(
          async ({
            context: { staking },
            connections: { sql, views },
            action,
          }) => {
            await resetStaking(staking, sql);
            action != "begin" && staking.reload(null);
            views.refresh("views.project_custom_url");
            views.refresh("views.project_summary");
          }
        ),
      ],
      onceInSync: [
        async ({ context: { staking }, connections: { sql } }) => {
          resetStaking(staking, sql);
          staking.toggleReloadDynamically(true);
          staking.reload(null);
        },
      ],
    },
  });
  return Object.assign(chainIndexer, { staking: stakingIndexer });
}

async function resetStaking(staking: StakingIndexer, sql: Sql) {
  staking.reset();
  const rows = await sql<{ hash: StakingHash }[]>`
    SELECT DISTINCT
      datum_json #>> '{registry, protocolStakingValidator, script, hash}' AS hash
    FROM
      chain.protocol_params pp
    UNION ALL SELECT DISTINCT
      staking_script_hash AS hash
    FROM
      chain.project_script ps
  `;
  const hashes = rows.map(({ hash }) => hash);
  staking.batchRegister(hashes, "Script");
}
