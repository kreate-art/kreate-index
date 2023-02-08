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
type Task = {
  id: Cid;
  title: string | null;
  slogan: string | null;
  tags: string;
  summary: string | null;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  description: any;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  announcement: any;
  roadmap: string | null;
  faq: string | null;
};

type TaskProjectInfo = Omit<Task, "announcement">;
type TaskProjectAnnouncement = {
  id: Cid;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  announcement: any;
};

// Contains sections being moderated
type Section = Omit<Task, "id">;

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
      tasks: TASKS_PER_FETCH,
      workers: 1,
    },

    fetch: async function () {
      const sql = this.connections.sql;

      const tasksProjectInfo = await sql<TaskProjectInfo[]>`
      SELECT * FROM
        (
          SELECT
            pc.cid as id,
            pc.title as title,
            pc.slogan as slogan,
            pc.summary as summary,
            array_to_string(pc.tags, ' ') as tags,
            pc.description as description,
            string_agg(r->>'name', ' ') || ' ' || string_agg(r->>'description', ' ') as roadmap,
            string_agg(f->>'question', ' ') || ' ' || string_agg(f->>'answer', ' ') as faq
          FROM
            ipfs.project_content pc,
            jsonb_array_elements(pc.contents -> 'data' -> 'roadmap') r,
            jsonb_array_elements(pc.contents -> 'data' -> 'community' -> 'frequentlyAskedQuestions') f
          GROUP BY pc.cid
        ) pi
        LEFT JOIN
          ai.project_moderation pm
        ON pi.id = pm.cid
        WHERE pm.cid IS NULL
        LIMIT ${TASKS_PER_FETCH}
      `;

      const tasksProjectAnnouncement = await sql<TaskProjectAnnouncement[]>`
        SELECT
          pcu.cid as id,
          (pcu.data -> 'data') as announcement
        FROM
          ipfs.project_community_update pcu
        LEFT JOIN
          ai.project_moderation pm
        ON pcu.cid = pm.cid
        WHERE pm.cid IS NULL
        LIMIT ${TASKS_PER_FETCH}
      `;

      let tasks = tasksProjectInfo.map((task) => fromTaskProjectInfo(task));
      tasks = tasks.concat(
        tasksProjectAnnouncement.map((task) =>
          fromTaskProjectAnnouncement(task)
        )
      );
      return {
        tasks,
        continue:
          tasksProjectInfo.length >= TASKS_PER_FETCH ||
          tasksProjectAnnouncement.length >= TASKS_PER_FETCH,
      };
    },

    handle: async function ({ id, ...data }: Task) {
      const {
        connections: { sql },
        context: { aiServerUrl },
      } = this;

      let description;
      try {
        description = extractDescriptionTexts(data.description, []).join("\n");
      } catch (error) {
        description = "";
        console.error(
          `[ai.content_moderation] Failed to extract text from description: ${id}`,
          error
        );
      }

      let announcement;
      try {
        const _announcement = extractDescriptionTexts(
          data.announcement?.body,
          []
        );
        _announcement.push(
          data.announcement?.title ?? "",
          data.announcement?.summary ?? ""
        );
        announcement = _announcement.join("\n");
      } catch (error) {
        announcement = "";
        console.error(
          `[ai.content_moderation] Failed to extract text from announcement: ${id}`,
          error
        );
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
  sections: Section,
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
        for (let label of data.tags) {
          label = label.replace(" ", "_");
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
    error = e instanceof Error ? e.message : toJson(e);
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

function fromTaskProjectInfo(task: TaskProjectInfo): Task {
  return {
    ...task,
    announcement: null,
  };
}

function fromTaskProjectAnnouncement(task: TaskProjectAnnouncement): Task {
  return {
    ...task,
    title: null,
    slogan: null,
    tags: "",
    summary: null,
    description: null,
    roadmap: null,
    faq: null,
  };
}
