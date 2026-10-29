import {
  open,
  Database,
  RootDatabase,
  type Key,
  type DatabaseOptions,
  type RootDatabaseOptions,
} from "lmdb";
import { z } from "zod";

export type TableSchema = {
  readonly id: z.core.$ZodType<
    Key,
    unknown,
    z.core.$ZodTypeInternals<Key, unknown>
  >;
  readonly [x: string]: z.core.$ZodType<
    unknown,
    unknown,
    z.core.$ZodTypeInternals<unknown, unknown>
  >;
};

export type Row<T extends z.ZodObject, OUT = z.output<T>> = OUT extends {
  id: any;
}
  ? OUT
  : OUT extends Record<string, never>
    ? { id: string }
    : OUT & { id: string };

export type IndexCombine<T extends string, U extends string = T> = [T] extends [
  never,
]
  ? ""
  : T extends any
    ?
        | T
        | (IndexCombine<Exclude<U, T>> extends ""
            ? never
            : `${T}:${IndexCombine<Exclude<U, T>>}`)
    : never;

export type Index<ROW extends Record<string, any>, K extends string> = {
  includes?: Exclude<keyof ROW, K>[];
  filter?: (row: ROW) => boolean;
};
export type Indexes<ROW extends Record<string, any>> = {
  [K in keyof ROW extends string ? IndexCombine<keyof ROW> : never]?: Index<
    ROW,
    K
  >;
};

export type Triggers<
  DB extends Db<any, true>,
  ROW extends Record<string, any>,
> = {
  beforeInsert?: (db: DB, data: ROW) => void;
  beforeUpdate?: (
    db: DB,
    data: "id" extends keyof ROW
      ? Partial<Omit<ROW, "id">> & Pick<ROW, "id">
      : never,
    old: ROW,
  ) => void;
  beforeDelete?: (db: DB, old: ROW) => void;
};

export type TableOptions<
  DB extends Db<any, true>,
  ROW_SCHEMA extends z.ZodObject,
  ROW extends Row<ROW_SCHEMA> = Row<ROW_SCHEMA>,
> = Omit<DatabaseOptions, "name"> & {
  noValidation?: boolean;
  indexes?: Indexes<Omit<ROW, "id">>;
  triggers?: Triggers<DB, ROW>;
};

export type Tables<DB_SCHEMA extends DbSchema, TX extends true = never> = {
  [K in keyof DB_SCHEMA]: Table<
    Db<DB_SCHEMA, TX>,
    z.ZodObject<DB_SCHEMA[K]>,
    TX
  >;
};
export type DbSchema = { [K: string]: TableSchema };
export type Db<DB_SCHEMA extends DbSchema, TX extends true = never> = {
  tables: Tables<DB_SCHEMA, TX>;
} & ([TX] extends [never]
  ? { transaction<T>(fn: (db: Db<DB_SCHEMA, true>) => T): Promise<T> }
  : {});
export type TablesOptions<DB_SCHEMA extends DbSchema> = {
  [K in keyof DB_SCHEMA]?: TableOptions<
    Db<DB_SCHEMA, true>,
    z.ZodObject<DB_SCHEMA[K]>
  >;
};
export type DbOptions<DB_SCHEMA extends DbSchema> = {
  tables?: TablesOptions<DB_SCHEMA>;
  noValidation?: boolean;
  advance?: RootDatabaseOptions;
};
export function create<DB_SCHEMA extends DbSchema>(
  path: string,
  dbSchema: DB_SCHEMA,
  options?: DbOptions<DB_SCHEMA>,
): Db<DB_SCHEMA> {
  const env = open(path, {});
  env.readerCheck();

  const db: Db<DB_SCHEMA> = {
    tables: Object.fromEntries(
      Object.entries(dbSchema).map(([tableName, tableSchema]) => [
        tableName,
        new Table(
          db,
          tableName,
          env,
          z.object(tableSchema),
          options?.tables[tableName],
        ),
      ]),
    ),
    transaction(fn) {
      return env.transaction(() => fn(db));
    },
  };

  return db;
}

const db = create(
  "./db.mdb",
  {
    user: {
      id: z.string(),
      name: z.string(),
      age: z.number().min(4).max(200),
    },
    apply: {
      id: z.number(),
      userId: z.string(),
      price: z.number().optional(),
    },
  },
  {
    tables: {
      user: {
        indexes: {
          "name:age": {
            includes: ["name"],
            filter: (row) => row.age > 18,
          },
        },
        triggers: {
          beforeInsert(db, data) {},
        },
      },
    },
  },
);

db.transaction((tx) => {
  tx.tables.user.update({ id: "" });
});
