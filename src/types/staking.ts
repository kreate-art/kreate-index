import * as L from "lucid-cardano";
import { RewardAddress } from "lucid-cardano";

export type StakingHash = L.KeyHash | L.ScriptHash;
export type StakingType = "Key" | "Script";

export interface StakingController {
  reset(): void;
  register(hash: StakingHash, type: StakingType): void;
  batchRegister(hashes: StakingHash[], type: StakingType): void;
  deregister(hash: StakingHash): boolean;
  fromHash(hash: StakingHash): RewardAddress | undefined;
  fromAddress(address: RewardAddress): StakingHash | undefined;
  isHashRegistered(hash: StakingHash): boolean;
  isAddressRegistered(address: RewardAddress): boolean;
  reload(hashes: StakingHash[] | null): void;
  toggleReloadDynamically(state: boolean): void;
}
