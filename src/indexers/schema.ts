import { Hex } from "@teiki/protocol/types";

import { Sql } from "../db";
import { $setup } from "../framework/base";

export const setupSchemas = $setup(async ({ sql }) => {
  await sql`CREATE SCHEMA IF NOT EXISTS chain`;
  await sql`CREATE SCHEMA IF NOT EXISTS ipfs`;
  await sql`CREATE SCHEMA IF NOT EXISTS discord`;
  await sql`CREATE SCHEMA IF NOT EXISTS ai`;
  await sql`CREATE SCHEMA IF NOT EXISTS admin`;
  await sql`CREATE SCHEMA IF NOT EXISTS views`;
  // TODO: Remove this `migration` schema later.
  await sql`CREATE SCHEMA IF NOT EXISTS migration`;
});

export const setupAdminTables = $setup(async ({ sql }) => {
  await setupFeaturedProjectTable(sql);
  await setupBlockedProjectTable(sql);
});

export const setupMigrationTables = $setup(async ({ sql }) => {
  await setupProjectMigrationTable(sql);
});

export const setupViews = $setup(async ({ sql }) => {
  await setupOpenProjectView(sql);
  await setupProjectCustomUrlView(sql);
  await setupProjectSummaryView(sql);
});

async function setupFeaturedProjectTable(sql: Sql) {
  await sql`
    CREATE TABLE IF NOT EXISTS admin.featured_project (
      project_id varchar(64) PRIMARY KEY
    )
  `;
  await sql`
    CREATE OR REPLACE FUNCTION featured_project_refresh ()
      RETURNS TRIGGER
      LANGUAGE PLPGSQL
      AS
    $$
    BEGIN
      REFRESH MATERIALIZED VIEW CONCURRENTLY views.project_summary;
      RETURN NULL;
    END;
    $$
  `;
  await sql`
    CREATE OR REPLACE TRIGGER featured_project_refresh_trigger
      AFTER INSERT OR UPDATE OR DELETE OR TRUNCATE ON admin.featured_project
      FOR EACH statement
      EXECUTE FUNCTION featured_project_refresh ()
  `;
}

async function setupBlockedProjectTable(sql: Sql) {
  await sql`
    CREATE TABLE IF NOT EXISTS admin.blocked_project (
      project_id text PRIMARY KEY
    )
  `;
  await sql`
    CREATE OR REPLACE FUNCTION blocked_project_refresh ()
      RETURNS TRIGGER
      LANGUAGE PLPGSQL
      AS
    $$
    BEGIN
      REFRESH MATERIALIZED VIEW CONCURRENTLY views.project_custom_url;
      REFRESH MATERIALIZED VIEW CONCURRENTLY views.project_summary;
      RETURN NULL;
    END;
    $$
  `;
  await sql`
    CREATE OR REPLACE TRIGGER blocked_project_refresh_trigger
      AFTER INSERT OR UPDATE OR DELETE OR TRUNCATE ON admin.blocked_project
      FOR EACH statement
      EXECUTE FUNCTION blocked_project_refresh ()
  `;
}

async function setupProjectMigrationTable(sql: Sql) {
  await sql`
    CREATE TABLE IF NOT EXISTS migration.project (
      id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
      legacy_project_id varchar(64) UNIQUE,
      project_id varchar(64) UNIQUE
    )
  `;
}

export type ViewProjectCustomUrl = {
  projectId: Hex;
  customUrl: string;
};

async function setupOpenProjectView(sql: Sql) {
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

async function setupProjectCustomUrlView(sql: Sql) {
  // TODO: This isn't 100% accurate
  await sql`
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
              pc.custom_url,
              o.created_slot
            FROM
              chain.project_detail pd
              INNER JOIN ipfs.project_content pc ON pd.information_cid = pc.cid
              INNER JOIN chain.output o ON pd.id = o.id
              INNER JOIN (
                SELECT _op.* FROM views.open_project _op
                INNER JOIN chain.output _o ON _op.id = _o.id
                WHERE _o.spent_slot IS NULL AND _op.status NOT IN ('delisted', 'closed')
              ) op ON pd.project_id = op.project_id
            WHERE
              pc.custom_url IS NOT NULL) t1
          GROUP BY
            t1.project_id, t1.custom_url) t2
          ORDER BY
            t2.project_id, t2.max_slot DESC) t3
          ORDER BY
            t3.custom_url, t3.min_slot ASC) t4
  `;
  await sql`
    CREATE UNIQUE INDEX IF NOT EXISTS project_custom_url_pid_index
      ON views.project_custom_url (project_id)
  `;
  await sql`
    CREATE UNIQUE INDEX IF NOT EXISTS project_custom_url_url_index
      ON views.project_custom_url (custom_url)
  `;
}

async function setupProjectSummaryView(sql: Sql) {
  await sql`
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
    x_project_time AS (
      SELECT
        upd.pid,
        b1.time AS created_time,
        b2.time AS last_announcement_time
      FROM (
        SELECT
          pd.project_id AS pid,
          min(o.created_slot) AS created_slot,
          max(
            CASE WHEN pd.last_community_update_cid IS NOT NULL THEN
              o.created_slot
            END) AS last_announcement_slot
      FROM
        chain.project_detail pd
        INNER JOIN chain.output o ON pd.id = o.id
      GROUP BY
        pd.project_id) upd
      INNER JOIN chain.block b1 ON b1.slot = upd.created_slot
      LEFT JOIN chain.block b2 ON b2.slot = upd.last_announcement_slot
    ),
    x_backing AS (
      SELECT
        b.project_id AS pid,
        count(DISTINCT b.backer_address) AS backer_count,
        sum(b.backing_amount) AS total_backing_amount
      FROM
        chain.backing b
        INNER JOIN chain.output o ON b.id = o.id
      WHERE
        o.spent_slot IS NULL
      GROUP BY
        b.project_id
    ),
    x_funds_w AS (
      SELECT
        pd.project_id AS pid,
        pd.withdrawn_funds
      FROM
        chain.project_detail pd
        INNER JOIN chain.output o ON pd.id = o.id
      WHERE
        o.spent_slot IS NULL
    ),
    x_funds_a AS (
      SELECT
        ps.project_id AS pid,
        sum(s.rewards) AS available_funds
      FROM
        chain.project_script ps
        INNER JOIN chain.output o ON ps.id = o.id
        INNER JOIN chain.staking s ON ps.staking_script_hash = s.hash
      WHERE
        o.spent_slot IS NULL
      GROUP BY
        ps.project_id
    )
    SELECT
      x_project.pid AS project_id,
      x_project.status,
      x_project.owner_address,
      x_project_time.created_time,
      x_project_time.last_announcement_time,
      coalesce(x_backing.backer_count, 0) AS backer_count,
      coalesce(x_backing.total_backing_amount, 0) AS total_backing_amount,
      x_funds_w.withdrawn_funds,
      coalesce(x_funds_a.available_funds, 0) AS available_funds,
      (x_funds_w.withdrawn_funds + coalesce(x_funds_a.available_funds, 0)) AS total_raised_funds
    FROM
      x_project
      LEFT JOIN x_project_time ON x_project.pid = x_project_time.pid
      LEFT JOIN x_backing ON x_project.pid = x_backing.pid
      LEFT JOIN x_funds_w ON x_project.pid = x_funds_w.pid
      LEFT JOIN x_funds_a ON x_project.pid = x_funds_a.pid;
  `;
  await sql`
    CREATE UNIQUE INDEX IF NOT EXISTS project_summary_pid_index
      ON views.project_summary (project_id)
  `;
  await sql`
    CREATE INDEX IF NOT EXISTS project_summary_status_index
      ON views.project_summary (status)
  `;
}
