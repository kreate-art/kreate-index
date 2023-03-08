import { fromJson } from "@teiki/protocol/json";
import { Cid } from "@teiki/protocol/types";
import { nullIfFalsy } from "@teiki/protocol/utils";

import { AllConnections, Connections } from "../../connections";
import { $setup } from "../../framework/base";
import {
  createPollingIndexer,
  PollingIndexer,
  VitalConnections,
} from "../../framework/polling";
import { NonEmpty } from "../../types/typelevel";

// TODO: Proper failures handling
type Task = { cid: Cid };

const TASKS_PER_FETCH = 40;
const SHA256_BUF_PATTERN = /^sha256:([a-f0-9]{64})$/;

ipfsProjectInfoIndexer.setup = $setup(async ({ sql }) => {
  await sql`
    -- TODO: Add index for 'custom_url'
    CREATE TABLE IF NOT EXISTS ipfs.project_info (
      cid text PRIMARY KEY,
      contents jsonb NOT NULL,
      title text,
      slogan text,
      custom_url text,
      tags text[] NOT NULL,
      summary text,
      description jsonb,
      benefits jsonb
    )
  `;
  await sql`
    CREATE INDEX IF NOT EXISTS project_info_custom_url_index
      ON ipfs.project_info(custom_url)
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

ipfsProjectAnnouncementIndexer.setup = $setup(async ({ sql }) => {
  await sql`
    CREATE TABLE IF NOT EXISTS ipfs.project_announcement (
      cid text PRIMARY KEY,
      data jsonb NOT NULL
    )
  `;
});

export function ipfsProjectInfoIndexer(
  connections: VitalConnections & Connections<"ipfs" | "views">
): PollingIndexer<null> {
  return createPollingIndexer({
    name: "ipfs.project_info",
    connections,
    triggers: { channels: ["ipfs.project_info"] },

    $id: ({ cid }: Task) => cid,

    fetch: async function () {
      const {
        connections: { sql },
      } = this;
      const tasks = await sql<Task[]>`
        SELECT DISTINCT
          pd.information_cid AS cid
        FROM
          chain.output out
        INNER JOIN chain.project_detail pd
          ON out.id = pd.id
        LEFT JOIN ipfs.project_info pi
          ON pd.information_cid = pi.cid
        WHERE
          pi.cid IS NULL
        LIMIT ${TASKS_PER_FETCH};
      `;
      return { tasks, continue: tasks.length >= TASKS_PER_FETCH };
    },

    handle: async function ({ cid }) {
      const {
        connections: { sql, ipfs, notifications, views },
      } = this;
      // TODO: Proper type
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      let projectInfo: any;
      // TODO: Error handling?
      try {
        // NOTE: This function is copied from teiki-backend/src/indexer/project-info.ts
        projectInfo = fromJson(await fetchFromIpfs(ipfs, cid));
      } catch (error) {
        // TODO: Better log here
        console.error("ERROR:", cid, error);
        this.retry();
        return;
      }

      const customUrl = nullIfFalsy(projectInfo?.data?.basics?.customUrl);
      const record = {
        cid,
        contents: projectInfo,
        customUrl: customUrl,
        title: nullIfFalsy(projectInfo?.data?.basics?.title),
        slogan: nullIfFalsy(projectInfo?.data?.basics?.slogan),
        tags: projectInfo?.data?.basics?.tags ?? [],
        summary: nullIfFalsy(projectInfo?.data?.basics?.summary),
        description: nullIfFalsy(projectInfo?.data?.description?.body),
        benefits: nullIfFalsy(projectInfo?.data?.benefits?.perks),
      };

      if (projectInfo.bufs != null) {
        const medias = [];
        for (const mediaCid of Object.values(projectInfo.bufs)) {
          if (!mediaCid) continue;
          medias.push({ projectCid: cid, mediaCid: mediaCid as string });
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
        INSERT INTO ipfs.project_info ${sql(record)}
          ON CONFLICT DO NOTHING
      `;
      notifications.notify("ai.project_moderation");

      // TODO: Better warnings and stuff
      const logoUrl: string | undefined =
        projectInfo?.data?.basics?.logoImage?.url;
      const logoSha256 = logoUrl?.match(SHA256_BUF_PATTERN)?.[1];
      const logoCid = logoSha256 ? projectInfo?.bufs?.[logoSha256] : undefined;
      if (logoCid)
        await sql`
          INSERT INTO ipfs.logo_used ${sql({ cid: logoCid })}
            ON CONFLICT DO NOTHING
        `;

      notifications.notify("discord.project_alert");
      notifications.notify("discord.project_moderation_alert");

      if (customUrl != null) views.refresh("views.project_custom_url");
    },
  });
}

export function ipfsProjectAnnouncementIndexer(
  connections: VitalConnections & Connections<"ipfs">
): PollingIndexer<null> {
  return createPollingIndexer({
    name: "ipfs.project_announcement",
    connections,
    triggers: { channels: ["ipfs.project_announcement"] },

    $id: ({ cid }: Task) => cid,

    fetch: async function () {
      const {
        connections: { sql },
      } = this;
      const tasks = await sql<Task[]>`
        SELECT DISTINCT
          pd.last_announcement_cid AS cid
        FROM
          chain.output out
        INNER JOIN chain.project_detail pd
          ON out.id = pd.id
        LEFT JOIN ipfs.project_announcement pa
          ON pd.last_announcement_cid = pa.cid
        WHERE
          pa.cid IS NULL
          AND pd.last_announcement_cid IS NOT NULL
        LIMIT ${TASKS_PER_FETCH};
      `;
      return { tasks, continue: tasks.length >= TASKS_PER_FETCH };
    },

    handle: async function ({ cid }) {
      const {
        connections: { sql, ipfs, notifications },
      } = this;
      try {
        const data = fromJson(await fetchFromIpfs(ipfs, cid));
        const record = { cid, data };
        // TODO: Error handling?
        await sql`
          INSERT INTO ipfs.project_announcement ${sql(record)}
            ON CONFLICT DO NOTHING
        `;
        notifications.notify("ai.project_moderation");
      } catch (error) {
        console.error("ERROR:", cid, error); // TODO: Better log here
        this.retry();
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
