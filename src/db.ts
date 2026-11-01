import { open, type RootDatabase, type RootDatabaseOptions } from "lmdb";

import { Table } from "./table.js";
import type { SchemaProvider } from "./schema-provider.js";
import { Index } from "./table-index.js";

export type Db<DB_SCHEMA extends DbSchema, TX extends boolean = false> = {
  env: RootDatabase;
  tables: Tables<DB_SCHEMA, TX>;
} & (TX extends false
  ? { transaction<T>(fn: (db: Db<DB_SCHEMA, true>) => T): Promise<T> }
  : {});

export type DbSchema = { [K: string]: SchemaProvider<any, any> };

export type Tables<DB_SCHEMA extends DbSchema, TX extends boolean = false> = {
  [K in keyof DB_SCHEMA]: Table<
    DB_SCHEMA[K],
    SchemaProvider.InferOutput<DB_SCHEMA[K]>,
    Exclude<keyof SchemaProvider.InferOutput<DB_SCHEMA[K]>, "id">[][],
    TX
  >;
};

export type Indexes<DB_SCHEMA extends DbSchema> = {
  [K in keyof DB_SCHEMA]?: Exclude<
    keyof SchemaProvider.InferOutput<DB_SCHEMA[K]>,
    "id"
  >[][];
};

export type Config<DB_SCHEMA extends DbSchema> = {
  path: `${string}.mdb`;
  dbSchema: DB_SCHEMA;
  indexes?: Indexes<DB_SCHEMA>;
  options?: Omit<RootDatabaseOptions, "maxDbs" | "dupSort" | "useVersions">;
};
export function create<DB_SCHEMA extends DbSchema>(
  config: Config<DB_SCHEMA>,
): Db<DB_SCHEMA> {
  const env = open(config.path, {
    ...config.options,
    maxDbs:
      Object.keys(config.dbSchema).length +
      Object.values(config.indexes ?? {}).flat().length,
  });
  env.readerCheck();

  const db: Db<DB_SCHEMA> = {
    env,
    tables: Object.fromEntries(
      Object.entries(config.dbSchema).map(([tableName, tableSchema]) => {
        return [
          tableName,
          new Table(
            tableName,
            env,
            tableSchema,
            config.indexes?.[tableName] ?? [],
          ),
        ];
      }),
    ) as Tables<DB_SCHEMA>,

    transaction(fn) {
      const { transaction, ...rest } = db;
      return env.transaction(() => fn(rest as never));
    },
  };

  return db;
}
