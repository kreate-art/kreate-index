export const ModerationLabels = [
  "toxicity",
  "obscene",
  "identityAttack",
  "insult",
  "threat",
  "sexualExplicit",
  "political",
  "discrimination",
  "drug",
  "gun",
  "pornographic",
] as const;

export type ModerationLabel = (typeof ModerationLabels)[number];

// Note: Adjust weights if needed
export const ModerationWeights = {
  title: 5,
  slogan: 4,
  summary: 3,
  tags: 2,
  description: 1,
  benefits: 1,
  announcement: 3,
  faq: 2,
  media: 3,
};

export const DISPLAYED_LABELS: Record<ModerationLabel, string> = {
  toxicity: "toxicity",
  obscene: "obscene",
  identityAttack: "identity attack",
  insult: "insult",
  threat: "threat",
  sexualExplicit: "sexual explicit",
  political: "political",
  discrimination: "discrimination",
  drug: "drug",
  gun: "gun",
  pornographic: "pornographic",
};
