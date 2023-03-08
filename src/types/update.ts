export const ProjectUpdateScope = [
  "description",
  "title",
  "slogan",
  "customUrl",
  "tags",
  "summary",
  "coverImages",
  "logoImage",
  "roadmap",
  "community",
] as const;
export type ProjectUpdateScope = (typeof ProjectUpdateScope)[number];
export const DISPLAYED_SCOPE: Record<ProjectUpdateScope, string> = {
  description: "description",
  roadmap: "roadmap",
  community: "community info",
  title: "title",
  slogan: "tagline",
  customUrl: "custom URL",
  tags: "tags",
  summary: "summary",
  coverImages: "banners",
  logoImage: "logo",
};
