import { open, type RootDatabase, type RootDatabaseOptions } from "lmdb";

import { Table } from "./table.js";
import type { SchemaProvider } from "./schema-provider.js";
import { Index } from "./table-index.js";

export type Db<
  TABLES_SCHEMA extends TablesSchema,
  INDEXES extends Indexes<TABLES_SCHEMA>,
  TX extends boolean = false,
> = {
  env: RootDatabase;
  tables: Tables<TABLES_SCHEMA, INDEXES, TX>;
} & (TX extends false
  ? {
      transaction<T>(
        fn: (db: Db<TABLES_SCHEMA, INDEXES, true>) => T,
      ): Promise<T>;
    }
  : {});

export type TablesSchema = { [K: string]: SchemaProvider<any, any> };

export type Tables<
  TABLES_SCHEMA extends TablesSchema,
  INDEXES extends Indexes<TABLES_SCHEMA>,
  TX extends boolean = false,
> = {
  [K in keyof TABLES_SCHEMA]: Table<
    TABLES_SCHEMA[K],
    SchemaProvider.InferOutput<TABLES_SCHEMA[K]>,
    INDEXES[K] extends string[][] ? INDEXES[K] : string[][],
    TX
  >;
};

export type Indexes<TABLES_SCHEMA extends TablesSchema> = {
  [K in keyof TABLES_SCHEMA]?: Exclude<
    keyof SchemaProvider.InferOutput<TABLES_SCHEMA[K]>,
    "id"
  >[][];
};

export type Config<
  TABLES_SCHEMA extends TablesSchema,
  INDEXES extends Indexes<TABLES_SCHEMA>,
> = {
  path: `${string}.mdb`;
  tables: TABLES_SCHEMA;
  indexes?: INDEXES;
  options?: Omit<RootDatabaseOptions, "maxDbs" | "dupSort" | "useVersions">;
};
export function create<
  TABLES_SCHEMA extends TablesSchema,
  const INDEXES extends Indexes<TABLES_SCHEMA>,
>(config: Config<TABLES_SCHEMA, INDEXES>): Db<TABLES_SCHEMA, INDEXES> {
  const env = open(config.path, {
    compression: true,
    ...Object.fromEntries(
      Object.entries(config.options ?? {}).filter(([_, v]) => v != null),
    ),
    maxDbs:
      Object.keys(config.tables).length +
      Object.values(config.indexes ?? {}).flat().length,
  });
  env.readerCheck();

  const db: Db<TABLES_SCHEMA, INDEXES> = {
    env,
    tables: Object.fromEntries(
      Object.entries(config.tables).map(([tableName, tableSchema]) => {
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
    ) as Tables<TABLES_SCHEMA, INDEXES>,

    transaction(fn) {
      const { transaction, ...rest } = db;
      return env.transaction(() => fn(rest as never));
    },
  };

  return db;
}
