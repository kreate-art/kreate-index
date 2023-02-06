import { toJson } from "@teiki/protocol/json";
import { Cid } from "@teiki/protocol/types";

import { $setup } from "../../framework/base";
import {
  createPollingIndexer,
  PollingIndexer,
  VitalConnections,
} from "../../framework/polling";

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

// We might need to add more fields, depends on our moderation scope
type Task = {
  id: Cid;
  title: string;
  slogan: string;
  tags: string[];
  summary: string;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  description: any;
};

// Contains sections being moderated
type Section = Omit<Task, "id" | "tags"> & {
  tags: string;
};

const WEIGHTS$SECTION = {
  title: 5,
  slogan: 4,
  summary: 3,
  tags: 2,
  description: 1,
};

// Moderation result from AI
type ModerationResult = {
  tags: Map<string, number> | null;
  error: string | null;
};

const TASKS_PER_FETCH = 40;

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
      tasks: TASKS_PER_FETCH,
      workers: 1,
    },

    fetch: async function () {
      const sql = this.connections.sql;
      const tasks = await sql<Task[]>`
        SELECT
          pc.cid as id,
          pc.title as title,
          pc.slogan as slogan,
          pc.summary as summary,
          pc.tags as tags,
          pc.description as description
        FROM
          ipfs.project_content pc
        LEFT JOIN
          ai.project_moderation pm
        ON pc.cid = pm.cid
        WHERE pm.cid IS NULL
        LIMIT ${TASKS_PER_FETCH};
      `;
      return { tasks, continue: tasks.length >= TASKS_PER_FETCH };
    },

    handle: async function ({
      id,
      title,
      tags,
      slogan,
      summary,
      description,
    }: Task) {
      const {
        connections: { sql },
        context: { aiServerUrl },
      } = this;

      let extractedText = "";
      try {
        extractedText = extractTextFromDescription(description, []).join("\n");
      } catch (error) {
        console.error("Failed to extract text from description:", id, error);
      }
      const res = await callContentModeration(
        {
          title,
          tags: tags.join(" "),
          slogan,
          summary,
          description: extractedText,
        },
        aiServerUrl
      );

      if (res.error == null && res.tags != null) {
        const moderatedTags: Map<string, number> = res.tags;
        await sql`
          INSERT INTO ai.project_moderation ${sql({
            cid: id,
            error: null,
            ...Object.fromEntries(
              LABELS.map((label) => [label, moderatedTags.get(label) ?? 0])
            ),
          })}
        `;
      } else {
        console.error(
          "[ai.project_moderation]",
          res.error ?? "tags are unexpectedly empty"
        );
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

async function callContentModeration(sections: Section, aiServerUrl: string) {
  const result: ModerationResult = { tags: new Map(), error: null };
  for (const [key, value] of Object.entries(sections))
    try {
      const weight = WEIGHTS$SECTION[key as keyof typeof WEIGHTS$SECTION];

      const res = await fetch(`${aiServerUrl}/ai-content-moderation`, {
        method: "POST",
        headers: { "Content-Type": "application/x-www-form-urlencoded" },
        body: new URLSearchParams({ text: value }),
      });

      if (res.ok) {
        const data = await res.json();
        if (data == null) throw new Error(`Response invalid: ${toJson(data)}`);
        data.tags.forEach((tag: string) => {
          const oldValue = result.tags?.get(tag) ?? 0;
          result.tags?.set(tag, oldValue + weight);
        });
      } else {
        const error = `Response: ${res.status} - ${
          res.statusText
        }: ${await res.text()}`;
        result.error = error;
      }
    } catch (e) {
      console.error(`[ai.project_moderation]`, e);
      result.error = e instanceof Error ? e.message : toJson(e);
    }
  return result;
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function extractTextFromDescription(jsonDescription: any, result: string[]) {
  if (typeof jsonDescription?.text === "string") {
    result.push(jsonDescription?.text);
  }
  if (Array.isArray(jsonDescription?.content)) {
    jsonDescription.content.forEach((c: string) => {
      extractTextFromDescription(c, result);
    });
  }
  return result;
}
