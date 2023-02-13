import { Sql } from "../../db";
import { $setup } from "../../framework/base";

export const setupAdminTables = $setup(async ({ sql }) => {
  await setupFeaturedProjectTable(sql);
  await setupBlockedProjectTable(sql);
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
