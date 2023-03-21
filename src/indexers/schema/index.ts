import { $setup } from "../../framework/base";

export { setupAdminTables } from "./admin";
export { setupMigrationTables } from "./migration";
export { setupViews } from "./views";

export const setupSchemas = $setup(async ({ sql }) => {
  await sql`CREATE SCHEMA IF NOT EXISTS chain`;
  await sql`CREATE SCHEMA IF NOT EXISTS ipfs`;
  await sql`CREATE SCHEMA IF NOT EXISTS discord`;
  await sql`CREATE SCHEMA IF NOT EXISTS ai`;
  await sql`CREATE SCHEMA IF NOT EXISTS kolours`;
  await sql`CREATE SCHEMA IF NOT EXISTS admin`;
  await sql`CREATE SCHEMA IF NOT EXISTS views`;
  // TODO: Remove this `migration` schema later.
  await sql`CREATE SCHEMA IF NOT EXISTS migration`;
});
