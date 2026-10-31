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
}): Db<DB_SCHEMA> {
  const env = open(config.path, {});
  env.readerCheck();
  const db: Db<DB_SCHEMA> = {
    tables: Object.fromEntries(
      Object.entries(config.dbSchema).map(([tableName, tableSchema]) => [
        tableName,
        new Table(
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
try {
  await Bun.file("test.mdb").unlink();
  await Bun.file("test.mdb-lock").unlink();
} catch (error) {}

const db = create({
  path: "test.mdb",
  dbSchema: {
    user: new ZodSchema({
      id: z.string(),
      name: z.string(),
      age: z.number(),
    }),
    apply: new ZodSchema({
      id: z.number(),
      userId: z.string(),
      price: z.number(),
    }),
  },
  indexes: {
    // user: [["age"], ["age", "name"]],
    // apply: [["price"], ["userId", "price"]],
  },
});

await db.transaction((db) => {
  db.tables.user.insert({ id: "a", name: "sof", age: 44 });
  db.tables.user.insert({ id: "b", name: "dahmounth", age: 33 });
  // db.tables.apply.insert({ id: 123, userId: "a", price: 200 });
});

db.tables.user.find({ name: "dahmounth" }).pipe(toConsole());
