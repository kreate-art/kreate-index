// TODO: Move SQL definitions here, to share between projects
import postgres from "postgres";

import { toJson, fromJson } from "@teiki/protocol/json";

export { postgres };

export type SqlTypes = Record<string, postgres.PostgresType>;

const POSTGRES_BASE_OPTIONS: postgres.Options<never> = {
  connection: {
    application_name: "teiki/index",
  },
  // eslint-disable-next-line import/no-named-as-default-member
  transform: postgres.camel,
  onnotice: (notice) => {
    if (
      ("code" in notice && notice.code === "42P06") ||
      notice.code === "42P07"
    )
      // relation/schema ... already exists, skipping
      notice.message && console.info(`(!) ${notice.message}`);
    else console.warn(notice);
  },
  onparameter: (key, value) => {
    console.warn(`(:) ${key} = ${value}`);
  },
  // onclose: (id) => {
  //   console.info(`(.) Connection Closed :: ${id}`);
  // },
};

const POSTGRES_BASE_TYPES = {
  // eslint-disable-next-line import/no-named-as-default-member
  bigint: postgres.BigInt,
  json: {
    // Override postgres'
    to: 114, // json
    from: [114, 3802], // json, jsonb
    serialize: toJson,
    parse: fromJson,
  } as postgres.PostgresType<unknown>,
};

export function options<T extends SqlTypes = Record<string, never>>(
  options: postgres.Options<T> = {}
): postgres.Options<typeof POSTGRES_BASE_TYPES & T> {
  const { types: userTypes, ...userOptions } = options;
  return {
    types: {
      ...POSTGRES_BASE_TYPES,
      ...userTypes,
    } as typeof POSTGRES_BASE_TYPES & T,
    ...POSTGRES_BASE_OPTIONS,
    ...userOptions,
  };
}

export type Sql<T extends SqlTypes = typeof POSTGRES_BASE_TYPES> = ReturnType<
  typeof postgres<T>
>;
