import { toJson } from "@teiki/protocol/json";
import { Cid } from "@teiki/protocol/types";
import { assert } from "@teiki/protocol/utils";

import { $setup } from "../../framework/base";
import {
  createPollingIndexer,
  PollingIndexer,
  VitalConnections,
} from "../../framework/polling";
import { objectEntries } from "../../utils";

export type AiProjectModerationContext = {
  aiServerUrl: string;
  ipfsGatewayUrl: string;
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
  "pornographic",
];

// Note: Adjust weights if needed
const WEIGHTS = {
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

// We might need to add more fields, depends on our moderation scope
type Task<T> = { cid: Cid } & T;

// TODO: Proper type
// eslint-disable-next-line @typescript-eslint/no-explicit-any
type AnyBufs = any;

type ProjectInfo = {
  title: string;
  slogan: string;
  tags: string;
  summary: string;
  description: AnyBufs;
  benefits: AnyBufs;
  faq: string;
  media: string[]; // list of CIDs of project media
};

type ProjectAnnouncement = {
  announcement: AnyBufs;
};

// Contains sections being moderated
type Sections<T> = { [K in keyof T]: string | string[] };

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
      pornographic smallint,
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

    $id: ({ cid }: Task<ProjectInfo | ProjectAnnouncement>) => cid,

    fetch: async function () {
      const sql = this.connections.sql;

      const tasksProjectInfo = await sql<Task<ProjectInfo>[]>`
        SELECT pj.* FROM
          (
            SELECT
              pi.cid AS cid,
              pi.title AS title,
              pi.slogan AS slogan,
              pi.summary AS summary,
              array_to_string(pi.tags, ' ') AS tags,
              pi.description AS description,
              pi.benefits AS benefits,
              string_agg(coalesce(f ->> 'question', ''), ' ') || ' ' || string_agg(coalesce(f ->> 'answer', ''), ' ') AS faq
            FROM
              ipfs.project_info pi
              LEFT JOIN LATERAL jsonb_array_elements(pi.contents #> '{data, community, frequentlyAskedQuestions}') f ON TRUE
            GROUP BY
              pi.cid
          ) pj
          LEFT JOIN
            ai.project_moderation pm
          ON pj.cid = pm.cid
          WHERE pm.cid IS NULL
          LIMIT ${TASKS_PER_FETCH}
      `;

      const tasksProjectAnnouncement = await sql<Task<ProjectAnnouncement>[]>`
        SELECT
          pa.cid AS cid,
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

    handle: async function ({ id: _, cid, ...data }) {
      const {
        connections: { sql },
        context: { aiServerUrl, ipfsGatewayUrl },
      } = this;

      let description = undefined;
      if ("description" in data) {
        try {
          description = extractFromRichText(data.description, []).join("\n");
        } catch (error) {
          console.error(
            `[ai.content_moderation] Failed to extract text from description: ${cid}`,
            error
          );
        }
      }

      let benefits = undefined;
      if ("benefits" in data) {
        try {
          benefits = extractFromRichText(data.benefits, []).join("\n");
        } catch (error) {
          console.log(
            `[ai.content_moderation] Failed to extract text from benefits: ${cid}`,
            error
          );
        }
      }

      let announcement = undefined;
      if ("announcement" in data) {
        try {
          const _an = data.announcement;
          announcement = [
            extractFromRichText(_an?.body, []),
            _an?.title ?? "",
            _an?.summary ?? "",
          ].join("\n");
        } catch (error) {
          console.error(
            `[ai.content_moderation] Failed to extract text from announcement: ${cid}`,
            error
          );
        }
      }

      const { labels, error } = await callContentModeration(
        cid,
        { ...data, description, announcement, benefits },
        aiServerUrl,
        ipfsGatewayUrl
      );

      if (error == null) {
        await sql`
          INSERT INTO ai.project_moderation ${sql({
            cid,
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
        //     cid,
        //     error: res.error ?? "tags are unexpectedly empty",
        //   })}
        // `;
      }
    },
  });
}

async function callContentModeration(
  cid: Cid,
  sections: Partial<Sections<ProjectInfo> & Sections<ProjectAnnouncement>>,
  aiServerUrl: string,
  ipfsGatewayUrl: string
): Promise<ModerationResult> {
  const labels = new Map<string, number>();
  let error: string | null = null;
  try {
    for (const [key, value] of objectEntries(sections)) {
      if (key === "media") {
        if (!value || !Array.isArray(value)) continue;
        for (const cid of value) {
          const res = await fetch(`${aiServerUrl}/image-content-moderation`, {
            method: "POST",
            headers: {
              "Content-Type": "application/x-www-form-urlencoded",
            },
            body: new URLSearchParams({
              url: `${ipfsGatewayUrl}/ipfs/${cid}`,
            }),
          });
          if (res.ok) {
            const data = await res.json();
            assert(data != null, `Data must not be empty (${cid} | media)`);
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
      } else {
        if (!value || Array.isArray(value)) continue;
        const res = await fetch(`${aiServerUrl}/ai-content-moderation`, {
          method: "POST",
          headers: { "Content-Type": "application/x-www-form-urlencoded" },
          body: new URLSearchParams({ text: value }),
        });

        // Duplicated with the above code, refine if needed.
        if (res.ok) {
          const data = await res.json();
          assert(data != null, `Data must not be empty (${cid} | text)`);
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
    }
    console.log(`[ai.project_moderation] OK: ${cid}`);
  } catch (e) {
    console.error(`[ai.project_moderation] Error ${cid}`, e);
    error = e instanceof Error ? e.message : e ? toJson(e) : "ERROR";
  }
  return { labels, error };
}

function extractFromRichText(
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  jsonRichText: any,
  result: string[]
): string[] {
  if (jsonRichText != null) {
    if (typeof jsonRichText?.text === "string") result.push(jsonRichText?.text);
    else if (Array.isArray(jsonRichText?.content))
      for (const c of jsonRichText.content) extractFromRichText(c, result);
  }
  return result;
}
