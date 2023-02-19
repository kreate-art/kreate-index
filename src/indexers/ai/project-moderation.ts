import { toJson } from "@teiki/protocol/json";
import { Cid } from "@teiki/protocol/types";

import { $setup } from "../../framework/base";
import {
  createPollingIndexer,
  PollingIndexer,
  VitalConnections,
} from "../../framework/polling";
import { objectEntries } from "../../types/typelevel";

export type AiProjectModerationContext = {
  aiServerUrl: string;
};

const LABELS = [
  "toxicity",
  "obscene",
  "identity_attack",
  "insult",
  "threat",
  "sexual_explicit",
  "political",
  "discrimination",
  "drug",
  "gun",
];

const WEIGHTS = {
  title: 5,
  slogan: 4,
  summary: 3,
  tags: 2,
  description: 1,
  announcement: 3,
  roadmap: 2,
  faq: 2,
};

// We might need to add more fields, depends on our moderation scope
type Task<T> = { id: Cid } & T;

// TODO: Proper type
// eslint-disable-next-line @typescript-eslint/no-explicit-any
type AnyBufs = any;

type ProjectInfo = {
  title: string;
  slogan: string;
  tags: string;
  summary: string;
  description: AnyBufs;
  roadmap: string;
  faq: string;
};

type ProjectAnnouncement = {
  announcement: AnyBufs;
};

// Contains sections being moderated
type Sections<T> = { [K in keyof T]: string };

// Moderation result from AI
type ModerationResult = {
  labels: Map<string, number>;
  error: string | null;
};

const TASKS_PER_FETCH = 20;

aiProjectModerationIndexer.setup = $setup(async ({ sql }) => {
  await sql`
    CREATE TABLE IF NOT EXISTS ai.project_moderation (
      cid TEXT NOT NULL PRIMARY KEY,
      toxicity smallint,
      obscene smallint,
      identity_attack smallint,
      insult smallint,
      threat smallint,
      sexual_explicit smallint,
      political smallint,
      discrimination smallint,
      drug smallint,
      gun smallint,
      error text
    )
  `;
});

export function aiProjectModerationIndexer(
  connections: VitalConnections
): PollingIndexer<AiProjectModerationContext> {
  return createPollingIndexer({
    name: "ai.project_moderation",
    connections,
    triggers: { channels: ["ai.project_moderation"] },
    concurrency: {
      tasks: TASKS_PER_FETCH * 2,
      workers: 1,
    },

    fetch: async function () {
      const sql = this.connections.sql;

      const tasksProjectInfo = await sql<Task<ProjectInfo>[]>`
        SELECT * FROM
          (
            SELECT
              pi.cid AS id,
              pi.title AS title,
              pi.slogan AS slogan,
              pi.summary AS summary,
              array_to_string(pi.tags, ' ') AS tags,
              pi.description AS description,
              string_agg(coalesce(r ->> 'name', ''), ' ') || ' ' || string_agg(coalesce(r ->> 'description', ''), ' ') AS roadmap,
              string_agg(coalesce(f ->> 'question', ''), ' ') || ' ' || string_agg(coalesce(f ->> 'answer', ''), ' ') AS faq
            FROM
              ipfs.project_info pi
              LEFT JOIN LATERAL jsonb_array_elements(pi.contents #> '{data, roadmap}') r ON TRUE
              LEFT JOIN LATERAL jsonb_array_elements(pi.contents #> '{data, community, frequentlyAskedQuestions}') f ON TRUE
            GROUP BY
              pi.cid
          ) pj
          LEFT JOIN
            ai.project_moderation pm
          ON pj.id = pm.cid
          WHERE pm.cid IS NULL
          LIMIT ${TASKS_PER_FETCH}
      `;

      const tasksProjectAnnouncement = await sql<Task<ProjectAnnouncement>[]>`
        SELECT
          pa.cid AS id,
          (pa.data -> 'data') AS announcement
        FROM
          ipfs.project_announcement pa
          LEFT JOIN ai.project_moderation pm ON pa.cid = pm.cid
        WHERE
          pm.cid IS NULL
        LIMIT ${TASKS_PER_FETCH}
      `;

      return {
        tasks: [...tasksProjectInfo, ...tasksProjectAnnouncement],
        continue:
          tasksProjectInfo.length >= TASKS_PER_FETCH ||
          tasksProjectAnnouncement.length >= TASKS_PER_FETCH,
      };
    },

    handle: async function ({
      id,
      ...data
    }: Task<ProjectInfo | ProjectAnnouncement>) {
      const {
        connections: { sql },
        context: { aiServerUrl },
      } = this;

      let description = undefined;
      if ("description" in data) {
        try {
          description = extractDescriptionTexts(data.description, []).join(
            "\n"
          );
        } catch (error) {
          console.error(
            `[ai.content_moderation] Failed to extract text from description: ${id}`,
            error
          );
        }
      }

      let announcement = undefined;
      if ("announcement" in data) {
        try {
          const _an = data.announcement;
          announcement = [
            extractDescriptionTexts(_an?.body, []),
            _an?.title ?? "",
            _an?.summary ?? "",
          ].join("\n");
        } catch (error) {
          console.error(
            `[ai.content_moderation] Failed to extract text from announcement: ${id}`,
            error
          );
        }
      }

      const { labels, error } = await callContentModeration(
        id,
        { ...data, description, announcement },
        aiServerUrl
      );

      if (error == null) {
        await sql`
          INSERT INTO ai.project_moderation ${sql({
            cid: id,
            error: null,
            ...Object.fromEntries(
              LABELS.map((label) => [label, labels.get(label) ?? 0])
            ),
          })}
        `;
      } else {
        this.retry();
        // TODO: We will re-enable storing errors later
        // await sql`
        //   INSERT INTO ai.project_moderation ${sql({
        //     id,
        //     error: res.error ?? "tags are unexpectedly empty",
        //   })}
        // `;
      }
    },
  });
}

async function callContentModeration(
  id: Cid,
  sections: Partial<Sections<ProjectInfo> & Sections<ProjectAnnouncement>>,
  aiServerUrl: string
): Promise<ModerationResult> {
  const labels = new Map<string, number>();
  let error: string | null = null;
  try {
    for (const [key, value] of objectEntries(sections)) {
      if (!value) continue;
      const res = await fetch(`${aiServerUrl}/ai-content-moderation`, {
        method: "POST",
        headers: { "Content-Type": "application/x-www-form-urlencoded" },
        body: new URLSearchParams({ text: value }),
      });
      if (res.ok) {
        const data = await res.json();
        if (data == null) throw new Error(`Response invalid: ${toJson(data)}`);
        for (const rlabel of data.tags) {
          const label = rlabel.replace(" ", "_");
          labels.set(label, (labels.get(label) ?? 0) + WEIGHTS[key]);
        }
      } else {
        error = `Response: ${res.status} - ${
          res.statusText
        }: ${await res.text()}`;
      }
    }
    console.log(`[ai.project_moderation] OK: ${id}`);
  } catch (e) {
    console.error(`[ai.project_moderation] Error ${id}`, e);
    error = e instanceof Error ? e.message : e ? toJson(e) : "ERROR";
  }
  return { labels, error };
}

function extractDescriptionTexts(
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  jsonDescription: any,
  result: string[]
): string[] {
  if (typeof jsonDescription?.text === "string")
    result.push(jsonDescription?.text);
  if (Array.isArray(jsonDescription?.content))
    for (const c of jsonDescription.content) extractDescriptionTexts(c, result);
  return result;
}
