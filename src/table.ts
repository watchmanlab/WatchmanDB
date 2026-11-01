import { filter, fromIterable, map, Source, Stream } from "@watchmanlab/stream";
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
  _indexes_cols: IDX_COLS;
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

  find<
    QYERY extends Partial<Omit<ROW, "id">>,
    OPTIONS extends Table.FindOptions,
  >(
    query = {} as QYERY & Table.ValidateQuery<IDX_COLS, QYERY, OPTIONS>,
    options?: OPTIONS,
  ): Table.WatchmanSource<
    ROW,
    OPTIONS["forceFullScan"] extends true
      ? Table.Diagnostics<never, "FULL_SCAN">
      : Table.DeduceStrategy<IDX_COLS, QYERY>
  > {
    const queryKeys = Object.keys(query) as string[];

    if (queryKeys.length === 0) {
      const source = fromIterable(this._db.getRange()).pipe(
        map((e) => e.value),
      );
      return this._attachDiagnostics(source, undefined, "FULL_SCAN") as never;
    }

    if (this._indexes && !options?.forceFullScan) {
      for (const index of this._indexes) {
        const result = index.find(query, queryKeys.length);
        if (!result) continue;

        return this._attachDiagnostics(
          result.source,
          index.columns,
          result.strategy,
        ) as never;
      }
    }

    const source = fromIterable(this._db.getRange({}))
      .pipe(map((e) => e.value))
      .pipe(
        filter((row) => queryKeys.every((k) => query[k] === row[k])),
      ) as never;

    return this._attachDiagnostics(source, undefined, "FULL_SCAN") as never;
  }
  private _attachDiagnostics(
    source: Source<ROW>,
    index: string[] | undefined,
    strategy: Table.IndexStrategy,
  ) {
    return Object.assign(source, {
      __diagnostics: {
        index,
        strategy,
      },
    });
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
  async clear(): Promise<void> {
    await Promise.all([
      this._db.clearAsync(),
      this._indexes?.map((idx) => idx.clear()),
    ]);
    return;
  }
}

export namespace Table {
  export type Row = Record<string, any> & { id: Key };

  export type InferRow<TABLE extends Table<any, any, any>> =
    TABLE extends Table<any, infer R, any> ? R : never;

  // export type InferIndex<T extends string, U extends string = T> = [T] extends [
  //   never,
  // ]
  //   ? ""
  //   : T extends any
  //     ?
  //         | T
  //         | (InferIndex<Exclude<U, T>> extends ""
  //             ? never
  //             : `${T}:${InferIndex<Exclude<U, T>>}`)
  //     : never;

  export type IndexMatch<
    INDEX extends string[],
    QUERY extends Record<string, any>,
  > = keyof QUERY extends INDEX[number]
    ? INDEX[0] extends keyof QUERY
      ? "FULL_COVER"
      : "NO_COVER"
    : INDEX[number] extends keyof QUERY
      ? "RESIDUAL_FILTER"
      : INDEX[0] extends keyof QUERY
        ? "RESIDUAL_FILTER"
        : "NO_COVER";

  export type DeduceStrategy<
    INDEXES extends string[][],
    QUERY extends Record<string, any>,
  > = INDEXES extends [
    infer FIRST extends string[],
    ...infer REST extends string[][],
  ]
    ? IndexMatch<FIRST, QUERY> extends "NO_COVER"
      ? DeduceStrategy<REST, QUERY>
      : Diagnostics<FIRST, IndexMatch<FIRST, QUERY>>
    : Diagnostics<never, "FULL_SCAN">;

  export type IndexStrategy =
    | "FULL_COVER"
    | "NRESIDUAL_FILTER"
    | "FULL_SCAN"
    | (string & {});
  export type Diagnostics<
    INDEX extends string[] | undefined,
    STRATEGY extends IndexStrategy,
  > = { index: INDEX; strategy: STRATEGY };
  export type WatchmanSource<
    ROW,
    DIAGNOSTICS extends Diagnostics<any, any>,
  > = Source<ROW> & {
    __diagnostics: DIAGNOSTICS;
  };
  export interface FindOptions {
    forceFullScan?: boolean;
  }

  export type ValidateQuery<
    INDEXES extends string[][],
    QUERY extends Record<string, any>,
    OPTIONS extends FindOptions,
  > = OPTIONS["forceFullScan"] extends true
    ? QUERY // Escape hatch triggered! Bypass all indexing errors.
    : DeduceStrategy<INDEXES, QUERY>["strategy"] extends "FULL_SCAN"
      ? { [K in keyof QUERY]: QUERY[K] } & {
          readonly __unindexed_query_error: "❌ WatchmanDB Error: This query results in a FULL_SCAN! Please add an appropriate index or pass { forceFullScan: true }.";
        }
      : QUERY; // Valid index found
}
