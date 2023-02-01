import * as S3 from "@aws-sdk/client-s3";

import { assert } from "@teiki/protocol/utils";

import { AllConnections, Connections } from "../../connections";
import { $setup } from "../../framework/base";
import {
  createPollingIndexer,
  PollingIndexer,
  VitalConnections,
} from "../../framework/polling";

type S3Key = string;
type ETag = string;

const LOGO_S3_PREFIX = "logos/";
const S3_KEY_PATTERN = new RegExp(`^${LOGO_S3_PREFIX}\\w+/([a-zA-Z])/`);

export type AiLogoContext = { s3Bucket: string; fetched: Set<ETag> };
type Task = {
  id: S3Key;
  etag?: ETag;
};

aiLogoIndexer.setup = $setup(async ({ sql }) => {
  await sql`
    CREATE TABLE IF NOT EXISTS ai.logo (
      cid TEXT PRIMARY KEY,
      letter varchar(1) NOT NULL,
      etag TEXT
    )
  `;
  await sql`
    CREATE INDEX IF NOT EXISTS logo_letter_index
      ON ai.logo(letter)
  `;
});

export function aiLogoIndexer(
  connections: VitalConnections & Connections<"ipfs" | "s3">
): PollingIndexer<AiLogoContext> {
  return createPollingIndexer({
    name: "ai.logo",
    connections,
    triggers: {
      channels: ["ai.logo"],
      interval: 21_600_000, // 6 hours
    },
    concurrency: { workers: 16 },

    initialize: async function () {
      const sql = this.connections.sql;
      const fetched = this.context.fetched;
      await sql<{ etag: string | null }[]>`
        SELECT etag FROM ai.logo
      `.cursor(100, (rows) => {
        for (const { etag } of rows) etag != null && fetched.add(etag);
      });
      console.log(`[ai.logo] Already fetched ${fetched.size} S3 objects`);
    },

    fetch: async function () {
      const s3 = this.connections.s3;
      const { s3Bucket, fetched } = this.context;
      const { skipped, tasks } = await fetchTasksFromS3(s3, s3Bucket, fetched);
      console.log(`[ai.logo] Skipped ${skipped} S3 objects`);
      return { tasks, continue: false };
    },

    handle: async function ({ id, etag }: Task) {
      const {
        connections: { sql, s3, ipfs },
      } = this;
      const letter = id.match(S3_KEY_PATTERN)?.[1]?.toUpperCase();
      if (!letter) throw new Error(`Malformed logo S3 key: ${id}`);

      const res = await s3.send(
        new S3.GetObjectCommand({
          Bucket: this.context.s3Bucket,
          Key: id,
          IfMatch: etag,
        })
      );
      etag = res.ETag;
      const body = res.Body;

      assert(body != null, `S3 ${id} has no body`);
      const { cid } = await ipfs.add(body, { pin: true });

      if (etag == null) console.warn(`[ai.logo] S3 ${id} has no ETag`);
      else this.context.fetched.add(etag);
      await sql`
        INSERT INTO ai.logo ${sql({ cid, letter, etag })}
          ON CONFLICT (cid) DO UPDATE set etag = ${etag}
      `;

      console.log(`[ai.logo] Streamed ${id} => ${cid}`);
    },
  });
}

async function fetchTasksFromS3(
  s3: AllConnections["s3"],
  bucket: string,
  fetched: Set<ETag>
): Promise<{ skipped: number; tasks: Task[] }> {
  const tasks: Task[] = [];
  let skipped = 0;

  async function fetchSinglePage(continuationToken?: string) {
    const res = await s3.send(
      new S3.ListObjectsV2Command({
        Bucket: bucket,
        Prefix: LOGO_S3_PREFIX,
        ContinuationToken: continuationToken,
      })
    );
    const contents = res.Contents;
    assert(contents != null, "S3 Contents must be defined");
    for (const obj of contents) {
      assert(obj.Key, "S3 Object Key must be defined");
      const etag = obj.ETag;
      if (etag == null || !fetched.has(etag)) tasks.push({ id: obj.Key, etag });
      else skipped++;
    }
    return res.NextContinuationToken;
  }

  let continuationToken: string | undefined = undefined;
  do continuationToken = await fetchSinglePage(continuationToken);
  while (continuationToken != null);

  return { skipped, tasks };
}
