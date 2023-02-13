import { Sql } from "../../db";
import { $setup } from "../../framework/base";

export const setupMigrationTables = $setup(async ({ sql }) => {
  await setupProjectMigrationTable(sql);
});

async function setupProjectMigrationTable(sql: Sql) {
  // TODO: Remove this table when we go mainnet
  await sql`
    CREATE TABLE IF NOT EXISTS migration.project (
      id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
      legacy_project_id varchar(64) UNIQUE,
      project_id varchar(64) UNIQUE
    )
  `;
}
