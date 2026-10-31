import {
  open,
  Database,
  RootDatabase,
  type Key,
  type DatabaseOptions,
  type RootDatabaseOptions,
} from "lmdb";
import { z } from "zod";
import { Table } from "./table";
import type { SchemaProvider } from "./schema-provider";
import { Index } from "./index";
import { ZodSchema } from "./zod-schema";
import { toConsole } from "@watchmanlab/stream";

export type DbSchema = { [K: string]: SchemaProvider<any, any> };

export type Tables<DB_SCHEMA extends DbSchema, TX extends boolean = false> = {
  [K in keyof DB_SCHEMA]: Table<
    DB_SCHEMA[K],
    SchemaProvider.InferOutput<DB_SCHEMA[K]>,
    TX
  >;
};

export type Indexes<DB_SCHEMA extends DbSchema> = {
  [K in keyof DB_SCHEMA]?: Exclude<
    keyof SchemaProvider.InferOutput<DB_SCHEMA[K]>,
    "id"
  >[][];
};
export type Db<DB_SCHEMA extends DbSchema, TX extends boolean = false> = {
  tables: Tables<DB_SCHEMA, TX>;
} & (TX extends false
  ? { transaction<T>(fn: (db: Db<DB_SCHEMA, true>) => T): Promise<T> }
  : {});

export function create<DB_SCHEMA extends DbSchema>(config: {
  path: `${string}.mdb`;
  dbSchema: DB_SCHEMA;
  indexes?: Indexes<DB_SCHEMA>;
  options?: Omit<RootDatabaseOptions, "maxDbs">;
}): Db<DB_SCHEMA> {
  const env = open(config.path, {
    ...config.options,
    maxDbs:
      Object.keys(config.dbSchema).length +
      Object.values(config.indexes ?? {}).flat().length,
  });
  env.readerCheck();

  const db: Db<DB_SCHEMA> = {
    tables: Object.fromEntries(
      Object.entries(config.dbSchema).map(([tableName, tableSchema]) => [
        tableName,
        new Table(
          tableName,
          env.openDB({ name: tableName }),
          tableSchema,
          config.indexes?.[tableName]?.map(
            (cols) =>
              new Index(
                env.openDB({
                  name: ["index", ...cols, tableName].join("\x00"),
                }),
                cols,
              ),
          ),
        ),
      ]),
    ) as Tables<DB_SCHEMA>,

    transaction(fn) {
      const { transaction, ...rest } = db;
      return env.transaction(() => fn(rest as never));
    },
  };

  return db;
}
