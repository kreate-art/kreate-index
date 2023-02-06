import { toJson } from "@teiki/protocol/json";
import { Cid } from "@teiki/protocol/types";

import { $setup } from "../../framework/base";
import {
  createPollingIndexer,
  PollingIndexer,
  VitalConnections,
} from "../../framework/polling";

export type AiOcrContext = {
  aiServerUrl: string;
  ipfsGatewayUrl: string;
};
type Task = { id: Cid };

const TASKS_PER_FETCH = 40;

aiOcrIndexer.setup = $setup(async ({ sql }) => {
  await sql`
    CREATE TABLE IF NOT EXISTS ai.ocr (
      media_cid TEXT NOT NULL PRIMARY KEY,
      error TEXT,
      text TEXT
    )
  `;
});

export function aiOcrIndexer(
  connections: VitalConnections
): PollingIndexer<AiOcrContext> {
  return createPollingIndexer({
    name: "ai.ocr",
    connections,
    triggers: { channels: ["ai.ocr"] },
    concurrency: {
      tasks: TASKS_PER_FETCH,
      workers: 1,
    },

    fetch: async function () {
      const sql = this.connections.sql;
      const tasks = await sql<Task[]>`
        SELECT DISTINCT
          pm.media_cid AS id
        FROM
          ipfs.project_media pm
        LEFT JOIN ai.ocr ocr
          ON pm.media_cid = ocr.media_cid
        WHERE
          ocr.media_cid IS NULL
        LIMIT ${TASKS_PER_FETCH};
      `;
      return { tasks, continue: tasks.length >= TASKS_PER_FETCH };
    },

    handle: async function ({ id }: Task) {
      const {
        connections: { sql },
        context: { aiServerUrl, ipfsGatewayUrl },
      } = this;
      try {
        const res = await fetch(`${aiServerUrl}/text-recognition`, {
          method: "POST",
          headers: { "Content-Type": "application/x-www-form-urlencoded" },
          body: new URLSearchParams({
            url: `${ipfsGatewayUrl}/ipfs/${id}`,
          }),
        });
        if (res.ok) {
          const data = await res.json();
          if (data == null)
            throw new Error(`Response invalid (${id}): ${toJson(data)}`);
          await sql`
            INSERT INTO ai.ocr ${sql({
              mediaCid: id,
              error: null,
              text: data.ocr,
            })}
          `;
          console.log(`[ai.ocr] OK: ${id}`);
        } else {
          const error = `Response (${id}): ${res.status} - ${
            res.statusText
          }: ${await res.text()}`;
          if (res.status >= 400 && res.status < 500) {
            await sql`
              INSERT INTO ai.ocr ${sql({
                mediaCid: id,
                error,
                text: null,
              })}
            `;
          } else throw new Error(error);
        }
      } catch (e) {
        console.error(`[ai.ocr] Error ${id}`, e);
      }
    },
  });
}
