import { fromIterable, Source, Stream } from "@watchmanlab/stream";

export class Table<
  DB extends Db<any, true>,
  ROW_SCHEMA extends z.ZodObject,
  TX extends true = never,
  ROW extends Row<ROW_SCHEMA> = Row<ROW_SCHEMA>,
> {
  private _db: DB;
  private _lmdb: Database<ROW>;
  private _$insert?: Stream<ROW>;
  private _$update?: Stream<ROW>;
  private _$delete?: Stream<ROW>;
  private _indexes?: {
    columns: Exclude<keyof ROW, "id">[];
    indexdb: Database;
  }[];

  private _rowSchema?: ROW_SCHEMA;

  constructor(
    db: DB,
    name: string,
    env: RootDatabase,
    rowSchema: ROW_SCHEMA,
    private options?: TableOptions<DB, ROW_SCHEMA>,
  ) {
    this._db = db;
    this._lmdb = env.openDB({ name, ...options });

    this._rowSchema = options?.noValidation ? undefined : rowSchema;

    if (options?.indexes) {
      this._indexes = [];

      for (const index of Object.values(options.indexes)) {
        const indexdb = env.openDB({ name: `${name}_${index.join("_")}_idx` });
        this._indexes.push({
          columns: [...new Set(index)].sort(),
          indexdb,
        });

        const mainIsEmpty = this._lmdb
          .getKeys({ limit: 1 })
          [Symbol.iterator]()
          .next().done;
        const indexIsEmpty = indexdb
          .getKeys({ limit: 1 })
          [Symbol.iterator]()
          .next().done;

        if (!mainIsEmpty && indexIsEmpty) {
          env.childTransaction(() => {
            for (const entry of this._lmdb.getRange({})) {
              const indexKey = this._makeIndexKey(entry.value, index);
              indexdb.put(indexKey, String(entry.key));
            }
          });
        }
      }
    }
  }
  get $insert() {
    return Source.from((this._$insert ??= new Stream()));
  }
  get $update() {
    return Source.from((this._$update ??= new Stream()));
  }
  get $delete() {
    return Source.from((this._$delete ??= new Stream()));
  }

  get(id: ROW["id"]): ROW | undefined {
    return this._lmdb.get(id);
  }

  find(query: Partial<Omit<ROW, "id">>): Source<ROW> {
    const queryKeys = Object.keys(query) as Exclude<keyof ROW, "id">[];
    if (queryKeys.length === 0)
      return fromIterable(this._lmdb.getRange({}).map((e) => e.value));

    if (this._indexes) {
      for (const { columns, indexdb } of this._indexes) {
        const matchesAll = columns.every((col) => queryKeys.includes(col));
        if (matchesAll) {
          const searchValues = columns
            .map((col) => String(query[col]))
            .join("\x00");
          const indexRange = indexdb.getRange({
            start: searchValues,
            end: searchValues + "\xff",
          });

          const rowStream = new Stream<ROW>();
          const entries = [...indexRange];

          queueMicrotask(() => {
            for (const entry of entries) {
              const row = this._lmdb.get(entry.value as any);
              if (row) {
                const passesRemainingFilters = queryKeys.every(
                  (k) => String(row[k]) === String(query[k]),
                );
                if (passesRemainingFilters) rowStream.push(row);
              }
            }
          });
          return Source.from(rowStream);
        }
      }
    }

    const scanStream = new Stream<ROW>();
    const allEntries = [...this._lmdb.getRange({})];
    queueMicrotask(() => {
      for (const entry of allEntries) {
        if (queryKeys.every((k) => String(entry.value[k]) === String(query[k])))
          scanStream.push(entry.value);
      }
    });
    return Source.from(scanStream);
  }

  private _makeIndexKey(
    data: ROW,
    columns: Exclude<keyof ROW, "id">[],
  ): string {
    const values = columns.map((col) => String(data[col]));
    return [...values, String(data.id)].join("\x00");
  }

  insert(data: ROW): [TX] extends [never] ? Promise<boolean> : boolean {
    this._rowSchema?.parse(data);

    try {
      this._lmdb.getWriteTxnId();
      return this._insert(data) as never;
    } catch (error: any) {
      return this._lmdb.transaction(() => this._insert(data)) as never;
    }
  }
  update(
    data: Partial<Omit<ROW, "id">> & Pick<ROW, "id">,
  ): [TX] extends [never] ? Promise<boolean> : boolean {
    this._rowSchema?.partial().parse(data);

    try {
      this._lmdb.getWriteTxnId();
      return this._update(data) as never;
    } catch (error) {
      return this._lmdb.transaction(() => this._update(data)) as never;
    }
  }
  upsert(data: ROW): [TX] extends [never] ? Promise<boolean> : boolean {
    if (this._lmdb.doesExist(data.id)) {
      return this.update(data) as never;
    } else {
      return this.insert(data) as never;
    }
  }
  delete(id: ROW["id"]): [TX] extends [never] ? Promise<boolean> : boolean {
    try {
      this._lmdb.getWriteTxnId();
      return this._delete(id) as never;
    } catch (error) {
      return this._lmdb.transaction(() => this._delete(id)) as never;
    }
  }
  private _insert(data: ROW) {
    if (this._lmdb.doesExist(data.id)) return false;

    if (this.options?.triggers?.beforeInsert)
      this.options.triggers.beforeInsert(this._db, data);

    if (this._indexes) {
      for (const { columns, indexdb } of this._indexes) {
        const indexKey = this._makeIndexKey(data, columns);
        indexdb.put(indexKey, data.id);
      }
    }

    this._lmdb.put(data.id, data).then((r) => {
      if (r) this._$insert?.push(data as ROW);
    });

    return true;
  }
  private _update(data: Partial<Omit<ROW, "id">> & Pick<ROW, "id">) {
    const old = this._lmdb.get(data.id);
    if (!old) return false;

    if (this.options?.triggers?.beforeUpdate)
      this.options.triggers.beforeUpdate(this._db, data, old);

    data = { ...old, ...data };

    if (this._indexes) {
      for (const { columns, indexdb } of this._indexes) {
        const oldIndexKey = this._makeIndexKey(old, columns);
        const newIndexKey = this._makeIndexKey(data as ROW, columns);

        if (oldIndexKey !== newIndexKey) {
          indexdb.remove(oldIndexKey);
          indexdb.put(newIndexKey, data.id);
        }
      }
    }

    this._lmdb.put(data.id, data as ROW).then((r) => {
      if (r) this._$update?.push(data as ROW);
    });

    return true;
  }
  private _delete(id: ROW["id"]) {
    const old = this._lmdb.get(id);
    if (!old) return false;

    if (this.options?.triggers?.beforeDelete)
      this.options.triggers.beforeDelete(this._db, old);

    if (this._indexes) {
      for (const { columns, indexdb } of this._indexes) {
        const indexKey = this._makeIndexKey(old, columns);
        indexdb.remove(indexKey);
      }
    }

    this._lmdb.remove(old.id).then((r) => {
      if (r) this._$delete?.push(old);
    });
    return true;
  }
  drop(): Promise<void> {
    return this._lmdb.clearAsync();
  }
}

export namespace Table {
  export type Shape = {
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
}
