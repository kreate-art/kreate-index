export const BackingActionTypes = [
  "back",
  "unback",
  "claim_rewards",
  "migrate",
] as const;
export type BackingActionType = (typeof BackingActionTypes)[number];
