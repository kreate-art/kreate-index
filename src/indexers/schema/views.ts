import { TransactionSql } from "postgres";

import { Hex } from "@teiki/protocol/types";

import { Sql, SqlQuery } from "../../db";
import { $setup } from "../../framework/base";

export const setupViews = $setup(async ({ sql }) => {
  await setupDefinitionsTable(sql);
  await setupOpenProjectView(sql);
  await setupProjectCustomUrlMatView(sql);
  await setupProjectSummaryMatView(sql);
});

async function setupDefinitionsTable(sql: Sql) {
  await sql`
    CREATE TABLE IF NOT EXISTS views.definitions (
      name text PRIMARY KEY,
      definition text NOT NULL
    )
  `;
}

function createMaterializedView(
  db: Sql,
  name: string,
  define: (sql: TransactionSql) => SqlQuery
): Promise<boolean> {
  return db.begin(async (sql) => {
    const query = define(sql);
    const definition = (await query.describe()).string;
    const res = await sql<{ existed: boolean; definition: string | null }[]>`
      SELECT
        (
          SELECT
            TRUE
          FROM
            pg_catalog.pg_matviews
          WHERE
            schemaname = 'views'
            AND matviewname = ${name}) AS existed,
        (
          SELECT
            definition
          FROM
            views.definitions
          WHERE
            name = ${name}) AS definition;
    `;
    if (!res.length || !res[0].existed || res[0].definition !== definition) {
      if (res[0].existed) {
        await sql`DROP MATERIALIZED VIEW ${sql(`views.${name}`)} CASCADE`;
        console.warn(`(?) Materialized View "${name}" is oudated. Recreate.`);
      } else {
        console.log(
          `(?) Materialized View "${name}" does not exist. Create New.`
        );
      }
      await sql.unsafe(definition);
      await sql`
        INSERT INTO views.definitions ${sql({ name, definition })}
        ON CONFLICT (name)
          DO UPDATE SET
            definition = EXCLUDED.definition
      `;
      return true;
    } else {
      console.log(`(!) Materialized View "${name}" is already up to date!`);
      return false;
    }
  });
}

async function setupOpenProjectView(sql: Sql) {
  // TODO: Drop this view? As it slightly affects query performance.
  await sql`
    CREATE OR REPLACE VIEW views.open_project AS
    SELECT
      p.*
    FROM
      chain.project p
    WHERE
      NOT EXISTS (
        SELECT
        FROM
          admin.blocked_project bp
        WHERE
          p.project_id = bp.project_id
      )
  `;
}

export type ViewProjectCustomUrl = {
  projectId: Hex;
  customUrl: string;
};

async function setupProjectCustomUrlMatView(sql: Sql) {
  // TODO: This isn't 100% accurate
  await createMaterializedView(
    sql,
    "project_custom_url",
    (sql) => sql`
      CREATE MATERIALIZED VIEW IF NOT EXISTS views.project_custom_url AS
      SELECT
        t4.custom_url,
        t4.project_id
      FROM ( SELECT DISTINCT ON (t3.custom_url)
          custom_url,
          t3.project_id
        FROM ( SELECT DISTINCT ON (t2.project_id)
            project_id,
            t2.custom_url,
            t2.min_slot
          FROM (
            SELECT
              t1.project_id,
              t1.custom_url,
              min(t1.created_slot) AS min_slot,
              max(t1.created_slot) AS max_slot
            FROM (
              SELECT
                pd.project_id,
                pi.custom_url,
                o.created_slot
              FROM
                chain.project_detail pd
                INNER JOIN ipfs.project_info pi ON pd.information_cid = pi.cid
                INNER JOIN chain.output o ON pd.id = o.id
                INNER JOIN (
                  SELECT _op.* FROM views.open_project _op
                  INNER JOIN chain.output _o ON _op.id = _o.id
                  WHERE _o.spent_slot IS NULL AND _op.status NOT IN ('delisted', 'closed')
                ) op ON pd.project_id = op.project_id
              WHERE
                pi.custom_url IS NOT NULL) t1
            GROUP BY
              t1.project_id, t1.custom_url) t2
            ORDER BY
              t2.project_id, t2.max_slot DESC) t3
            ORDER BY
              t3.custom_url, t3.min_slot ASC) t4
    `
  );
  await sql`
    CREATE UNIQUE INDEX IF NOT EXISTS project_custom_url_pid_index
      ON views.project_custom_url (project_id)
  `;
  await sql`
    CREATE UNIQUE INDEX IF NOT EXISTS project_custom_url_url_index
      ON views.project_custom_url (custom_url)
  `;
}

async function setupProjectSummaryMatView(sql: Sql) {
  await createMaterializedView(
    sql,
    "project_summary",
    (sql) => sql`
      CREATE MATERIALIZED VIEW IF NOT EXISTS views.project_summary AS
      WITH
      x_project AS (
        SELECT
          p.project_id AS pid,
          p.status,
          p.owner_address
        FROM
          views.open_project p
          INNER JOIN chain.output o ON p.id = o.id
        WHERE
          o.spent_slot IS NULL
      ),
      x_project_detail AS (
        SELECT
          pd.project_id AS pid,
          pd.withdrawn_funds,
          pd.sponsorship_amount,
          pd.sponsorship_until
        FROM
          chain.project_detail pd
          INNER JOIN chain.output o ON pd.id = o.id
        WHERE
          o.spent_slot IS NULL
      ),
      x_project_time AS (
        SELECT
          upd.pid,
          b1.time AS created_time,
          b2.time AS last_updated_time
        FROM (
          SELECT
            pd.project_id AS pid,
            min(o.created_slot) AS created_slot,
            ( SELECT
                max(created_slot)
              FROM (
                SELECT
                  o.created_slot,
                  d.last_announcement_cid AS announcement_cid,
                  LAG(d.last_announcement_cid) OVER (ORDER BY d.id) AS prev_announcement_cid,
                  d.information_cid,
                  LAG(d.information_cid) OVER (ORDER BY d.id) AS prev_information_cid
                FROM
                  chain.project_detail d
                INNER JOIN
                  chain.output o ON d.id = o.id
                WHERE
                  d.project_id = pd.project_id
              ) AS _a
              WHERE
                announcement_cid IS DISTINCT FROM prev_announcement_cid
                OR information_cid IS DISTINCT FROM prev_information_cid
            ) AS last_updated_slot
        FROM
          chain.project_detail pd
          INNER JOIN chain.output o ON pd.id = o.id
        GROUP BY
          pd.project_id) upd
        INNER JOIN chain.block b1 ON b1.slot = upd.created_slot
        LEFT JOIN chain.block b2 ON b2.slot = upd.last_updated_slot
      ),
      x_backing AS (
        SELECT
          b.project_id AS pid,
          count(DISTINCT b.backer_address) AS backer_count,
          sum(b.backing_amount)::bigint AS total_backing_amount
        FROM
          chain.backing b
          INNER JOIN chain.output o ON b.id = o.id
        WHERE
          o.spent_slot IS NULL
        GROUP BY
          b.project_id
      ),
      x_staking AS (
        SELECT
          ps.project_id AS pid,
          sum(ss.rewards)::bigint AS available_funds
        FROM
          chain.project_script ps
          INNER JOIN chain.output o ON ps.id = o.id
          INNER JOIN chain.staking_state ss ON ps.staking_script_hash = ss.hash
        WHERE
          o.spent_slot IS NULL
        GROUP BY
          ps.project_id
      ),
      x_project_related_outputs AS (
        SELECT
          po.project_id as pid,
          sum(amount)::bigint AS total_amount
        FROM (
          SELECT
            uap.id,
            uap.project_id,
            coalesce((o.value -> 'lovelace')::bigint, 0) AS amount
          FROM (
            SELECT
              p.id,
              p.project_id
            FROM
              chain.project p
            UNION ALL
            SELECT
              pd.id,
              pd.project_id
            FROM
              chain.project_detail pd
            UNION ALL
            SELECT
              ps.id,
              ps.project_id
            FROM
              chain.project_script ps
          ) uap
          INNER JOIN
            chain.output o ON o.id = uap.id
          WHERE
            o.spent_slot IS NULL
        ) po
        GROUP BY
          po.project_id
      )
      SELECT
        x_project.pid AS project_id,
        x_project.status,
        x_project.owner_address,
        x_project_time.created_time,
        x_project_time.last_updated_time,
        coalesce(x_backing.backer_count, 0) AS backer_count,
        coalesce(x_backing.total_backing_amount, 0) AS total_backing_amount,
        coalesce(x_backing.total_backing_amount, 0) + (CASE
          WHEN (status IN ('closed', 'delisted'))
            THEN 0
            ELSE coalesce(x_project_related_outputs.total_amount, 0)
        END) AS total_staking_amount,
        x_project_detail.withdrawn_funds,
        x_project_detail.sponsorship_amount,
        x_project_detail.sponsorship_until,
        coalesce(x_staking.available_funds, 0) AS available_funds,
        (x_project_detail.withdrawn_funds + coalesce(x_staking.available_funds, 0)) AS total_raised_funds
      FROM
        x_project
        LEFT JOIN x_project_detail ON x_project.pid = x_project_detail.pid
        LEFT JOIN x_project_time ON x_project.pid = x_project_time.pid
        LEFT JOIN x_project_related_outputs ON x_project.pid = x_project_related_outputs.pid
        LEFT JOIN x_backing ON x_project.pid = x_backing.pid
        LEFT JOIN x_staking ON x_project.pid = x_staking.pid
    `
  );
  await sql`
    CREATE UNIQUE INDEX IF NOT EXISTS project_summary_pid_index
      ON views.project_summary (project_id)
  `;
  await sql`
    CREATE INDEX IF NOT EXISTS project_summary_status_index
      ON views.project_summary (status)
  `;
}
