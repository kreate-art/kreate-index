// SHARED: This module should be shared with `kreate-web`.
import * as O from "@cardano-ogmios/schema";
import * as L from "lucid-cardano";

import { Hex, UnixTime } from "@kreate/protocol/types";

export type Lovelace = bigint;

// chain.block
export type ChainBlock = {
  slot: O.Slot;
  hash: O.DigestBlake2BBlockBody; // Hex
  height: O.BlockNo;
  time: UnixTime;
};

// chain.output
export type ChainOutput = {
  tag: string | null;
  txId: L.TxHash;
  txIx: number;
  address: L.Address;
  value: L.Assets;
  datum: L.Datum | null;
  datumHash: L.DatumHash | null;
  scriptHash: L.ScriptHash | null;
  createdSlot: O.Slot;
  spentSlot: O.Slot | null;
};

// chain.script
export type ChainScript = {
  scriptHash: L.ScriptHash;
  scriptType: L.ScriptType;
  script: Hex;
};

type WithScript = Omit<ChainScript, "scriptHash">;
export type ChainOutputWithScript = ChainOutput &
  (WithScript | { [_ in keyof WithScript]?: null });

export type EnrichedUtxo = L.UTxO & { scriptHash: L.ScriptHash | null };

export function toLucidUtxo(output: ChainOutputWithScript): EnrichedUtxo {
  return {
    txHash: output.txId,
    outputIndex: output.txIx,
    address: output.address,
    assets: output.value,
    datum: output.datum,
    datumHash: output.datumHash,
    scriptHash: output.scriptHash,
    scriptRef: output.scriptType
      ? { type: output.scriptType, script: output.script }
      : null,
  };
}
