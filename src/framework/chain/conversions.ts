import * as O from "@cardano-ogmios/schema";
import * as L from "lucid-cardano";

import { Hex } from "@kreate/protocol/types";

import { ChainOutput } from "../../types/chain";

export function prettyOutRef(output: ChainOutput) {
  return `${output.txId}/${output.txIx}`;
}

export function slotFrom(slotted: O.Point | O.Tip | O.Origin) {
  if (slotted === "origin") return 0;
  else return slotted.slot;
}

export function ogmiosValueToLucidAssets(value: O.Value): L.Assets {
  const nonAdaAssets = value.assets;
  const assets: L.Assets = { lovelace: value.coins };
  if (nonAdaAssets)
    Object.entries(nonAdaAssets).forEach(([unit, amount]) => {
      assets[unit.replace(".", "")] = amount;
    });
  return assets;
}

export function ogmiosScriptToLucidScript(script: O.Script): L.Script {
  if ("plutus:v1" in script)
    return {
      type: "PlutusV1",
      script: doubleCborEncodePlutusScript(script["plutus:v1"]),
    };
  else if ("plutus:v2" in script)
    return {
      type: "PlutusV2",
      script: doubleCborEncodePlutusScript(script["plutus:v2"]),
    };
  // FIXME: Support native scripts...
  else throw new Error(`Native scripts are not supported yet`);
}

function doubleCborEncodePlutusScript(scriptPlutus: O.ScriptPlutus): Hex {
  return L.toHex(L.C.PlutusScript.new(L.fromHex(scriptPlutus)).to_bytes());
}
