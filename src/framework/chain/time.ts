import * as O from "@cardano-ogmios/schema";

import { UnixTime } from "@teiki/protocol/types";

import { slotFrom } from "./conversions";

export type SlotTimeInterpreter = ReturnType<typeof createSlotTimeInterpreter>;

export function createSlotTimeInterpreter(
  ledgerTip: O.PointOrOrigin,
  eraSummaries: O.EraSummary[],
  systemStart: UnixTime
) {
  if (!eraSummaries.length) throw new Error("No era");
  const reversedEras = [...eraSummaries].reverse();
  const ledgerTipSlot = slotFrom(ledgerTip);
  // TODO: Confirm how safeZone works...
  const safeZone = reversedEras[0].parameters.safeZone;
  // It's fine to refetch every single block
  const staleSlot = slotFrom(ledgerTip) + (safeZone ?? 0);

  function slotToRelativeTime(slot: O.Slot): O.RelativeTime {
    for (const era of reversedEras) {
      const start = era.start;
      if (slot >= start.slot)
        return start.time + (slot - start.slot) * era.parameters.slotLength;
    }
    throw new Error("Not fit into any era");
  }

  function slotToAbsoluteTime(slot: O.Slot): UnixTime {
    return systemStart + slotToRelativeTime(slot) * 1_000;
  }

  return {
    ledgerTipSlot,
    staleSlot,
    slotToRelativeTime,
    slotToAbsoluteTime,
  };
}
