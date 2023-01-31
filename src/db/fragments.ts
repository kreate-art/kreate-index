import { Sql } from ".";

export function sqlNotIn(sql: Sql, column: string, values: unknown[]) {
  return values.length ? sql`${sql(column)} NOT IN ${sql(values)}` : sql`TRUE`;
}
