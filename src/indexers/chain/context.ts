import { ScriptHash } from "lucid-cardano";

import { Hex } from "@teiki/protocol/types";

import { StakingIndexer } from "../../framework/chain/staking";

export interface TeikiChainIndexConfig {
  readonly TEIKI_PLANT_NFT_MPH: Hex;
  readonly ALWAYS_FAIL_SCRIPT_HASH: Hex;
  readonly PROJECT_AT_MPH: Hex;
  readonly PROTOCOL_NFT_MPH: Hex;
  readonly PROOF_OF_BACKING_MPH: Hex;
  readonly PROTOCOL_SCRIPT_V_SCRIPT_HASH: Hex;
}

export interface TeikiChainIndexContext {
  readonly staking: StakingIndexer;
  // Customize this, generally it consists of both immutable (e.g, config)
  // and mutable (e.g, caching) state for effiency.
  readonly config: TeikiChainIndexConfig;
  scriptHashes: {
    dedicatedTreasuryV: Set<ScriptHash>;
    sharedTreasuryV: Set<ScriptHash>;
    openTreasuryV: Set<ScriptHash>;
  };
}
