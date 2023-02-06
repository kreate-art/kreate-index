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

// We might need to add more fields, depends on our moderation scope
type Task = {
  id: Cid;
  title: string;
  slogan: string;
  tags: string;
  summary: string;
  description: any;
};

// Contains sections being moderated
type Section = Omit<Task, "id">;

// Moderation result from AI
type ModerationResult = {
  tags: string[] | null;
  error: string | null;
};

const TASKS_PER_FETCH = 40;

aiProjectModerationIndexer.setup = $setup(async ({ sql }) => {
  await sql`
    CREATE TABLE IF NOT EXISTS ai.project_moderation (
      id TEXT NOT NULL PRIMARY KEY,
      toxicity boolean,
      obscene boolean,
      identity_attack boolean,
      insult boolean,
      threat boolean,
      sexual_explicit boolean,
      political boolean,
      discrimination boolean,
      drug boolean,
      gun boolean,
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
          (array_to_string(pc.tags, ' ')) as tags,
          pc.description as description
        FROM
          ipfs.project_content pc
        LEFT JOIN
          ai.project_moderation pm
        ON pc.cid = pm.id
        WHERE pm.id IS NULL
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
        extractedText = extractTextFromDescription(description, []).join(" ");
      } catch (error) {
        console.error("Failed to extract text from description:", id, error);
      }
      const res = await moderate(
        { title, tags, slogan, summary, description: extractedText },
        aiServerUrl
      );

      if (res.error === null && res.tags !== null) {
        await sql`
          INSERT INTO ai.project_moderation ${sql({
            id,
            toxicity: res.tags.includes("toxicity"),
            obscene: res.tags.includes("obscene"),
            identity_attack: res.tags.includes("identity_attack"),
            insult: res.tags.includes("insult"),
            threat: res.tags.includes("threat"),
            sexual_explicit: res.tags.includes("sexual_explicit"),
            political: res.tags.includes("political"),
            discrimination: res.tags.includes("discrimination"),
            drug: res.tags.includes("drug"),
            gun: res.tags.includes("gun"),
            error: null,
          })}
        `;
      } else {
        await sql`
          INSERT INTO ai.project_moderation ${sql({
            id,
            error: res.error,
          })}
        `;
      }
    },
  });
}

async function moderate(sections: Section, aiServerUrl: string) {
  const result: ModerationResult = { tags: [], error: null };
  for (const [_key, value] of Object.entries(sections))
    try {
      const res = await fetch(`${aiServerUrl}/ai-content-moderation`, {
        method: "POST",
        headers: { "Content-Type": "application/x-www-form-urlencoded" },
        body: new URLSearchParams({ text: value }),
      });

      if (res.ok) {
        const data = await res.json();
        if (data == null) throw new Error(`Response invalid: ${toJson(data)}`);
        data.tags.forEach((tag: string) => result.tags?.push(tag));
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
