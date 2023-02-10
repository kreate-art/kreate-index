import { Readable } from "node:stream";

import { waitUntilObjectExists } from "@aws-sdk/client-s3";
import { Upload } from "@aws-sdk/lib-storage";
import { checkExceptions } from "@aws-sdk/util-waiter";
import fetch from "node-fetch";

import { toJson } from "@teiki/protocol/json";
import { Cid } from "@teiki/protocol/types";
import { assert } from "@teiki/protocol/utils";

import * as config from "../../config";
import { Connections } from "../../connections";
import { $setup } from "../../framework/base";
import {
  createPollingIndexer,
  PollingIndexer,
  VitalConnections,
} from "../../framework/polling";

export type AiPodcastContext = {
  aiServerUrl: string;
  s3Bucket: string;
  s3Prefix: string;
};
type Task = { id: Cid; title: string; summary: string };

const TASKS_PER_FETCH = 40;

const STAGING_CONFIG = {
  interval: 60_000,
  workers: 8,
};

const NORMAL_CONFIG = {
  interval: undefined,
  // Our TTS server can only handle 1 request at a time...
  workers: 1,
};

const CURRENT_CONFIG =
  config.ENV === "staging" ? STAGING_CONFIG : NORMAL_CONFIG;

aiPodcastIndexer.setup = $setup(async ({ sql }) => {
  await sql`
    CREATE TABLE IF NOT EXISTS ai.podcast (
      cid TEXT PRIMARY KEY,
      error TEXT,
      completed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `;
});

export function aiPodcastIndexer(
  connections: VitalConnections & Connections<"s3">
): PollingIndexer<AiPodcastContext> {
  return createPollingIndexer({
    name: "ai.podcast",
    connections,
    triggers: {
      channels: ["ai.podcast"],
      interval: CURRENT_CONFIG.interval,
    },
    concurrency: {
      tasks: TASKS_PER_FETCH,
      workers: CURRENT_CONFIG.workers,
    },

    fetch: async function () {
      const sql = this.connections.sql;
      const tasks = await sql<Task[]>`
        SELECT
          cid AS id,
          title,
          summary
        FROM (
          SELECT
            DISTINCT pa.cid,
            pi.title,
            NULLIF (pa.data -> 'data' ->> 'summary', '') AS summary
          FROM
            ipfs.project_announcement pa
          INNER JOIN chain.project_detail pd ON pa.cid = pd.last_announcement_cid
          INNER JOIN ipfs.project_info pi ON pi.cid = pd.information_cid
          LEFT JOIN ai.podcast pod ON pa.cid = pod.cid
          WHERE
            pod.cid IS NULL) u
        WHERE
          summary IS NOT NULL
        LIMIT ${TASKS_PER_FETCH};
      `;
      return { tasks, continue: tasks.length >= TASKS_PER_FETCH };
    },

    handle: async function ({ id, title, summary }: Task) {
      const {
        connections: { sql, s3 },
        context: { aiServerUrl, s3Bucket, s3Prefix },
      } = this;
      let error: string | null = null;
      let willRetry = false;
      const s3Key = `${s3Prefix}${id}.wav`;
      try {
        summary = normalizeSummary(summary);
        title = title
          .split(/\s+/)
          .filter((w) => !!w)
          .slice(0, 10)
          .join(" ");
        assert(summary.length >= 100, `update summary is too short (< 100)`);

        // TODO: The TTS server is really fragile at the moment
        // Therefore we won't try to "skip" failed tasks for now.
        willRetry = true;

        if (config.ENV === "staging") {
          console.log(`[ai.podcast] Waiting for ${id}.wav to be uploaded.`);
          checkExceptions(
            await waitUntilObjectExists(
              { client: s3, maxWaitTime: 60 },
              { Bucket: "teiki-ai", Key: s3Key }
            )
          );
          console.log(`[ai.podcast] OK: ${id}.wav`);
        } else {
          console.log(`[ai.podcast] TTS: ${id}`);
          const res = await fetch(`${aiServerUrl}/text2speech`, {
            method: "POST",
            headers: {
              "Content-Type": "application/x-www-form-urlencoded",
            },
            body: new URLSearchParams({ text: [title, summary].join("\n") }),
          });
          if (!res.ok)
            throw new Error(
              `Response: ${res.status} - ${res.statusText}: ${await res.text()}`
            );
          const body = res.body;
          assert(body != null, "no response body");
          assert(body instanceof Readable, "response body must be Readable");
          const upload = new Upload({
            client: s3,
            params: {
              Bucket: s3Bucket,
              Key: s3Key,
              Body: body,
              ContentType: "audio/wav",
            },
          });
          upload.done();
          console.log(`[ai.podcast] Uploaded: ${id}.wav`);
        }
      } catch (e) {
        console.error(`[ai.podcast] Error ${id}`, e);
        error = e instanceof Error ? e.message : toJson(e);
      }
      if (!error || !willRetry)
        await sql`INSERT INTO ai.podcast ${sql({ cid: id, error })}`;
    },
  });
}

function normalizeSummary(
  summary: string,
  wordsLimit = 100,
  outro = "For more information, please read the full announcement on Teiki."
) {
  let remaining = wordsLimit;
  const lines: string[] = [];
  for (const line of summary.split(/(?:\r?\n)+/)) {
    const words = line.split(/\s+/).filter((w) => !!w);
    if (words.length <= remaining) {
      remaining -= words.length;
      lines.push(line);
    } else {
      if (remaining > 0) lines.push(words.slice(0, remaining).join(" "));
      lines.push(outro);
      remaining = 0;
      break;
    }
  }
  return lines.join("\n");
}
