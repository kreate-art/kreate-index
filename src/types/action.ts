export const ActionTypes = [
  "back",
  "unback",
  "claim_rewards",
  "migrate",
] as const;
export type BackingActionType = (typeof ActionTypes)[number];
