// This module will be imported by handlers/*.ts

import { ScriptHash } from "lucid-cardano";

import { Hex } from "@teiki/protocol/types";

export interface TeikiChainIndexConfig {
  readonly TEIKI_PLANT_NFT_MPH: Hex;
  readonly ALWAYS_FAIL_SCRIPT_HASH: Hex;
  readonly PROJECT_AT_MPH: Hex;
  readonly PROTOCOL_NFT_MPH: Hex;
  readonly PROOF_OF_BACKING_MPH: Hex;
  readonly PROTOCOL_SCRIPT_V_SCRIPT_HASH: Hex;
}

export interface TeikiChainIndexContext {
  // Customize this, generally it consists of both immutable (e.g, config)
  // and mutable (e.g, caching) state for effiency.
  readonly config: TeikiChainIndexConfig;
  dedicatedTreasuryVScriptHashes: Set<ScriptHash>;
  sharedTreasuryVScriptHashes: Set<ScriptHash>;
  openTreasuryVScriptHashes: Set<ScriptHash>;
}
