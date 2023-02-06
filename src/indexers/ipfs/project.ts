import { fromJson } from "@teiki/protocol/json";
import { Cid } from "@teiki/protocol/types";
import { nullIfFalsy } from "@teiki/protocol/utils";

import { AllConnections, Connections } from "../../connections";
import { sqlNotIn } from "../../db/fragments";
import { $setup } from "../../framework/base";
import {
  createPollingIndexer,
  PollingIndexer,
  VitalConnections,
} from "../../framework/polling";
import { NonEmpty } from "../../types/typelevel";

// TODO: Proper failures handling
export type IpfsProjectContext = { ignored: Cid[] };
type Task = { id: Cid };

const TASKS_PER_FETCH = 40;
const SHA256_BUF_PATTERN = /^sha256:([a-f0-9]{64})$/;

ipfsProjectContentIndexer.setup = $setup(async ({ sql }) => {
  await sql`
    CREATE TABLE IF NOT EXISTS ipfs.project_content (
      cid text PRIMARY KEY,
      contents jsonb NOT NULL,
      title text,
      slogan text,
      custom_url text,
      tags text[],
      summary text,
      description jsonb
    )
  `;
  await sql`
    CREATE INDEX IF NOT EXISTS project_content_custom_url_index
      ON ipfs.project_content(custom_url)
  `;

  await sql`
    CREATE TABLE IF NOT EXISTS ipfs.project_media (
      project_cid TEXT,
      media_cid TEXT,
      -- media_cid is often used in queries than project_cid
      PRIMARY KEY (media_cid, project_cid)
    )
  `;

  await sql`
    CREATE TABLE IF NOT EXISTS ipfs.logo_used (
      cid TEXT PRIMARY KEY
    )
  `;
});

ipfsProjectCommunityUpdateIndexer.setup = $setup(async ({ sql }) => {
  await sql`
    -- TODO: Add index for 'custom_url'
    CREATE TABLE IF NOT EXISTS ipfs.project_community_update (
      cid text PRIMARY KEY,
      data jsonb NOT NULL
    )
  `;
});

export function ipfsProjectContentIndexer(
  connections: VitalConnections & Connections<"ipfs" | "views">
): PollingIndexer<IpfsProjectContext> {
  return createPollingIndexer({
    name: "ipfs.project_content",
    connections,
    triggers: { channels: ["ipfs.project_content"] },

    fetch: async function () {
      const {
        connections: { sql },
        context: { ignored },
      } = this;
      const tasks = await sql<Task[]>`
        SELECT DISTINCT
          pd.information_cid AS id
        FROM
          chain.output out
        INNER JOIN chain.project_detail pd
          ON out.id = pd.id
        LEFT JOIN ipfs.project_content pc
          ON pd.information_cid = pc.cid
        WHERE
          pc.cid IS NULL
          AND ${sqlNotIn(sql, "pd.information_cid", ignored)}
        LIMIT ${TASKS_PER_FETCH};
      `;
      return { tasks, continue: tasks.length >= TASKS_PER_FETCH };
    },

    handle: async function ({ id }: Task) {
      const {
        connections: { sql, ipfs, notifications, views },
        context: { ignored },
      } = this;
      // TODO: Proper type
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      let projectContent: any;
      // TODO: Error handling?
      try {
        // NOTE: This function is copied from teiki-backend/src/indexer/project-info.ts
        projectContent = fromJson(await fetchFromIpfs(ipfs, id));
      } catch (error) {
        // TODO: Better log here
        console.error("ERROR:", id, error);
        ignored.push(id);
        return;
      }

      const customUrl = nullIfFalsy(projectContent?.data?.basics?.customUrl);
      const record = {
        cid: id,
        contents: projectContent,
        customUrl: customUrl,
        title: nullIfFalsy(projectContent?.data?.basics?.title),
        slogan: nullIfFalsy(projectContent?.data?.basics?.slogan),
        tags: nullIfFalsy(projectContent?.data?.basics?.tags),
        summary: nullIfFalsy(projectContent?.data?.basics?.summary),
        description: nullIfFalsy(projectContent?.data?.description?.body),
      };

      if (projectContent.bufs != null) {
        const medias = [];
        for (const mediaCid of Object.values(projectContent.bufs)) {
          if (!mediaCid) continue;
          medias.push({ projectCid: id, mediaCid: mediaCid as string });
        }
        if (medias.length) {
          await sql`
            INSERT INTO ipfs.project_media ${sql(medias)}
              ON CONFLICT DO NOTHING
          `;
          notifications.notify("ai.ocr");
        }
      }

      await sql`
        INSERT INTO ipfs.project_content ${sql(record)}
          ON CONFLICT DO NOTHING
      `;

      // TODO: Better warnings and stuff
      const logoUrl: string | undefined =
        projectContent?.data?.basics?.logoImage?.url;
      const logoSha256 = logoUrl?.match(SHA256_BUF_PATTERN)?.[1];
      const logoCid = logoSha256
        ? projectContent?.bufs?.[logoSha256]
        : undefined;
      if (logoCid)
        await sql`
          INSERT INTO ipfs.logo_used ${sql({ cid: logoCid })}
            ON CONFLICT DO NOTHING
        `;

      notifications.notify("discord.project_alert");

      if (customUrl != null) views.refresh("views.project_custom_url");
    },
  });
}

export function ipfsProjectCommunityUpdateIndexer(
  connections: VitalConnections & Connections<"ipfs">
): PollingIndexer<IpfsProjectContext> {
  return createPollingIndexer({
    name: "ipfs.project_community_update",
    connections,
    triggers: { channels: ["ipfs.project_community_update"] },

    fetch: async function () {
      const {
        connections: { sql },
        context: { ignored },
      } = this;
      const tasks = await sql<Task[]>`
        SELECT DISTINCT
          pd.last_community_update_cid AS id
        FROM
          chain.output out
        INNER JOIN chain.project_detail pd
          ON out.id = pd.id
        LEFT JOIN ipfs.project_community_update pcu
          ON pd.last_community_update_cid = pcu.cid
        WHERE
          pcu.cid IS NULL
          AND pd.last_community_update_cid IS NOT NULL
          AND ${sqlNotIn(sql, "pd.last_community_update_cid", ignored)}
        LIMIT ${TASKS_PER_FETCH};
      `;
      return { tasks, continue: tasks.length >= TASKS_PER_FETCH };
    },

    handle: async function ({ id }: Task) {
      const {
        connections: { sql, ipfs },
        context: { ignored },
      } = this;
      try {
        const data = fromJson(await fetchFromIpfs(ipfs, id));
        const record = { cid: id, data };
        // TODO: Error handling?
        await sql`
          INSERT INTO ipfs.project_community_update ${sql(record)}
            ON CONFLICT DO NOTHING
        `;
      } catch (error) {
        console.error("ERROR:", id, error); // TODO: Better log here
        ignored.push(id);
      }
    },

    batch: function (_tasks: NonEmpty<Task[]>) {
      this.connections.notifications.notify("ai.podcast");
    },
  });
}

async function fetchFromIpfs(
  ipfs: AllConnections["ipfs"],
  cid: Cid
): Promise<string> {
  const path = `/ipfs/${cid}`;
  console.log(`Fetching ${path}`);
  // Check whether given CID exist
  const chunks: Uint8Array[] = [];
  const response = ipfs.cat(path);
  for await (const chunk of response) chunks.push(chunk);
  return Buffer.concat(chunks).toString("utf8");
}
