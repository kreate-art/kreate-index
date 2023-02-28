import * as O from "@cardano-ogmios/schema";
import { ScriptHash } from "lucid-cardano";

import {
  PROJECT_AT_TOKEN_NAMES,
  PROOF_OF_BACKING_TOKEN_NAMES,
  PROTOCOL_NFT_TOKEN_NAMES,
  TEIKI_PLANT_NFT_TOKEN_NAME,
  TEIKI_TOKEN_NAME,
} from "@teiki/protocol/contracts/common/constants";

import { StakingIndexer } from "../../framework/chain/staking";

type TeikiChainIndexYamlConfig = {
  bootstrap: O.PointOrOrigin[];
  deployment: ScriptHash[];
  scripts: {
    mpTeiki: ScriptHash;
    nftTeikiPlant: ScriptHash;
    nftProtocol: ScriptHash[];
    atProject: ScriptHash[];
    mpProofOfBacking: ScriptHash[];
    vDedicatedTreasury: ScriptHash[];
    vSharedTreasury: ScriptHash[];
    vOpenTreasury: ScriptHash[];
  };
};

export type TeikiChainIndexConfig = ReturnType<typeof loadConfig>;

export function loadConfig(rawConfig: TeikiChainIndexYamlConfig) {
  const { deployment, scripts, ...config } = rawConfig;
  return {
    ...config,
    deployment: new Set(deployment),
    authTeikiPlant: `${scripts.nftTeikiPlant}.${TEIKI_PLANT_NFT_TOKEN_NAME}`,
    assetTeiki: `${scripts.mpTeiki}.${TEIKI_TOKEN_NAME}`,
    authProtocolParams: `${scripts.nftProtocol}.${PROTOCOL_NFT_TOKEN_NAMES.PARAMS}`,
    authsProject: {
      project: scripts.atProject.map(
        (mph) => `${mph}.${PROJECT_AT_TOKEN_NAMES.PROJECT}`
      ),
      projectDetail: scripts.atProject.map(
        (mph) => `${mph}.${PROJECT_AT_TOKEN_NAMES.PROJECT_DETAIL}`
      ),
      projectScript: scripts.atProject.map(
        (mph) => `${mph}.${PROJECT_AT_TOKEN_NAMES.PROJECT_SCRIPT}`
      ),
    },
    mphProofOfBackings: scripts.mpProofOfBacking,
    assetsProofOfBacking: {
      seed: scripts.mpProofOfBacking.map(
        (mph) => `${mph}.${PROOF_OF_BACKING_TOKEN_NAMES.SEED}`
      ),
      wilted: scripts.mpProofOfBacking.map(
        (mph) => `${mph}.${PROOF_OF_BACKING_TOKEN_NAMES.WILTED_FLOWER}`
      ),
    },
    hashesTreasury: {
      dedicated: scripts.vDedicatedTreasury,
      shared: scripts.vSharedTreasury,
      open: scripts.vOpenTreasury,
    },
  };
}

export type TeikiChainIndexContext = {
  readonly staking: StakingIndexer;
  // Customize this, generally it consists of both immutable (e.g, config)
  // and mutable (e.g, caching) state for effiency.
  readonly config: Readonly<TeikiChainIndexConfig>;
  protocolVersion: number;
  scriptHashes: {
    dedicatedTreasury: Set<ScriptHash>;
    sharedTreasury: Set<ScriptHash>;
    openTreasury: Set<ScriptHash>;
  };
};
