import {
  delay,
  filter,
  fromIterable,
  map,
  Source,
  Stream,
  terminate,
} from "@watchmanlab/stream";
import type { Database, GetOptions, Key, RootDatabase } from "lmdb";
import type { SchemaProvider } from "./schema-provider";
import { Index } from "./table-index";

export class Table<
  SCHEMA,
  ROW extends Table.Row,
  IDX_COLS extends any[][],
  TX extends boolean = false,
> {
  private _name: string;
  private _db: Database<Readonly<ROW>>;
  private _schemaProvider: SchemaProvider<SCHEMA, ROW>;
  private _indexes_cols: IDX_COLS;
  private _indexes?: Index[];
  private _$beforeInsert?: Stream<Readonly<ROW>>;
  private _$afterInsert?: Stream<Readonly<ROW>>;
  private _$beforeUpdate?: Stream<{ New: Readonly<ROW>; Old: Readonly<ROW> }>;
  private _$afterUpdate?: Stream<{ New: Readonly<ROW>; Old: Readonly<ROW> }>;
  private _$beforeDelete?: Stream<Readonly<ROW>>;
  private _$afterDelete?: Stream<Readonly<ROW>>;

  constructor(
    name: string,
    env: RootDatabase,
    schemaProvider: SchemaProvider<SCHEMA, ROW>,
    indexesCols: IDX_COLS,
  ) {
    this._name = name;
    this._db = env.openDB({ name, sharedStructuresKey: Symbol.for(name) });
    this._schemaProvider = schemaProvider;
    this._indexes_cols = indexesCols;
    this._indexes = indexesCols
      .sort((a, b) => b.length - a.length)
      .map((cols) => new Index(env, this, cols));
  }
  get name() {
    return this._name;
  }
  get $afterInsert(): Source<Readonly<ROW>> {
    return Source.from((this._$afterInsert ??= new Stream()));
  }
  get $beforeInsert(): Source<Readonly<ROW>> {
    return Source.from((this._$beforeInsert ??= new Stream()));
  }
  get $beforeUpdate(): Source<{ New: Readonly<ROW>; Old: Readonly<ROW> }> {
    return Source.from((this._$beforeUpdate ??= new Stream()));
  }
  get $afterUpdate(): Source<{ New: Readonly<ROW>; Old: Readonly<ROW> }> {
    return Source.from((this._$afterUpdate ??= new Stream()));
  }
  get $beforeDelete(): Source<Readonly<ROW>> {
    return Source.from((this._$beforeDelete ??= new Stream()));
  }
  get $afterDelete(): Source<Readonly<ROW>> {
    return Source.from((this._$afterDelete ??= new Stream()));
  }

  get(id: ROW["id"], options?: GetOptions): ROW | undefined {
    return this._db.get(id, options);
  }

  find<Q extends Partial<Omit<ROW, "id">>>(
    query = {} as Q,
  ): Table.TrackedSource<
    ROW,
    Table.DeduceIndexSelection<IDX_COLS, Q>["index"],
    Table.DeduceIndexSelection<IDX_COLS, Q>["strategy"]
  > {
    const queryKeys = Object.keys(query) as string[];

    if (queryKeys.length === 0)
      return fromIterable(this._db.getRange()).pipe(
        map((e) => e.value),
      ) as never;

    if (this._indexes) {
      for (const index of this._indexes) {
        const source = index.find(query, queryKeys.length);
        if (!source) continue;

        return source as never;
      }
    }

    return fromIterable(this._db.getRange({}))
      .pipe(map((e) => e.value))
      .pipe(
        filter((row) => queryKeys.every((k) => query[k] === row[k])),
      ) as never;
  }

  private _isInsideTransaction(): boolean {
    try {
      return this._db.getWriteTxnId() > 0;
    } catch (error) {
      return false;
    }
  }
  insert(row: ROW): TX extends true ? boolean : Promise<boolean> {
    if (this._isInsideTransaction()) {
      return this._insert(row) as never;
    } else {
      return this._db.transaction(() => this._insert(row)) as never;
    }
  }
  update(
    row: Partial<Omit<ROW, "id">> & Pick<ROW, "id">,
  ): TX extends true ? boolean : Promise<boolean> {
    if (this._isInsideTransaction()) {
      return this._update(row) as never;
    } else {
      return this._db.transaction(() => this._update(row)) as never;
    }
  }
  upsert(row: ROW): TX extends true ? boolean : Promise<boolean> {
    if (this._db.doesExist(row.id)) {
      return this.update(row) as never;
    } else {
      return this.insert(row) as never;
    }
  }
  delete(id: ROW["id"]): TX extends true ? boolean : Promise<boolean> {
    if (this._isInsideTransaction()) {
      return this._delete(id) as never;
    } else {
      return this._db.transaction(() => this._delete(id)) as never;
    }
  }
  private _insert(row: ROW) {
    if (this._db.doesExist(row.id)) return false;

    const parsed = this._schemaProvider.parse(row);

    this._$beforeInsert?.push(parsed);

    for (let i = 0; i < (this._indexes?.length ?? 0); i++) {
      this._indexes?.[i]?.insert(parsed);
    }

    this._db.put(parsed.id, parsed).then((r) => {
      if (r) this._$afterInsert?.push(parsed);
    });

    return true;
  }
  private _update(row: Partial<Omit<ROW, "id">> & Pick<ROW, "id">) {
    const Old = this._db.get(row.id);
    if (!Old) return false;

    const parsed = this._schemaProvider.parse({ ...Old, ...row });

    this._$beforeUpdate?.push({ New: parsed, Old });

    for (let i = 0; i < (this._indexes?.length ?? 0); i++) {
      this._indexes?.[i]?.update(parsed, Old);
    }

    this._db.put(parsed.id, parsed).then((r) => {
      if (r) this._$afterUpdate?.push({ New: parsed, Old });
    });

    return true;
  }
  private _delete(id: ROW["id"]) {
    const old = this._db.get(id);
    if (!old) return false;

    this._$beforeDelete?.push(old);

    for (let i = 0; i < (this._indexes?.length ?? 0); i++) {
      this._indexes?.[i]?.insert(old);
    }

    this._db.remove(old.id).then((r) => {
      if (r) this._$afterDelete?.push(old);
    });
    return true;
  }
  async drop(): Promise<void> {
    await Promise.all([
      this._db.clearAsync(),
      this._indexes?.map((idx) => idx.drop()),
    ]);
    return;
  }
}

export namespace Table {
  export type Row = Record<string, any> & { id: Key };
  export type InferRow<T extends Table<any, any, any>> =
    T extends Table<any, infer R, any> ? R : never;

  export interface QueryDiagnostics<Index, Strategy> {
    __debug_index_hit: Index;
    __debug_strategy: Strategy;
  }

  // We wrap your original Source stream with the diagnostics metadata
  export type TrackedSource<T, Index, Strategy> = Source<T> &
    QueryDiagnostics<Index, Strategy>;

  // Helper type to check if a specific Index tuple covers a Query object type
  export type EvaluateIndexMatch<
    Index extends readonly string[],
    Query,
  > = Index extends readonly []
    ? "NO_COVER"
    : {
          [K in Index[number]]: K extends keyof Query ? true : false;
        }[Index[number]] extends true
      ? EqualLen<Index, Query> extends true
        ? "FULL_COVER"
        : "RESIDUAL_FILTER"
      : "NO_COVER";

  // Helper to determine if the query key length matches the index tuple length exactly
  export type EqualLen<
    Index extends readonly string[],
    Query,
    Keys extends any[] = [],
  > = Keys["length"] extends Index["length"]
    ? keyof Query extends Index[number]
      ? true
      : false
    : EqualLen<Index, Query, [...Keys, any]>;

  // The recursive compiler loop that mimics your runtime loop matching order
  export type DeduceIndexSelection<
    Indexes extends readonly (readonly string[])[],
    Query,
  > = Indexes extends readonly [
    infer First extends readonly string[],
    ...infer Rest extends readonly (readonly string[])[],
  ]
    ? EvaluateIndexMatch<First, Query> extends "NO_COVER"
      ? DeduceIndexSelection<Rest, Query> // Try next index in priority order
      : { index: First; strategy: EvaluateIndexMatch<First, Query> } // Found it!
    : { index: never; strategy: "FULL_SCAN" }; // No index matched
}
