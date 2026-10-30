import { Source, Stream } from "@watchmanlab/stream";
import type { Database } from "lmdb";
import type { Db } from "./db";

type InTransaction<DB extends Db<any, any>> =
  DB extends Db<any, infer TX> ? (TX extends true ? true : false) : never;
export class Table<
  NAME extends string,
  DB extends Db<any, true>,
  SCHEMA,
  ROW extends Record<string, any>,
> {
  private _db: DB;
  private _lmdb: Database<ROW>;
  private _$insert?: Stream<ROW>;
  private _$update?: Stream<ROW>;
  private _$delete?: Stream<ROW>;
  private _schema?: SCHEMA;

  constructor(
    db: DB,
    lmdb: Database,
    schema: SCHEMA,
    private options?: Table.Options<DB, ROW>,
  ) {
    this._db = db;
    this._lmdb = lmdb;

    this._schema = options?.noValidation ? undefined : schema;
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

  // find(query: Partial<Omit<ROW, "id">>): Source<ROW> {
  //   const queryKeys = Object.keys(query) as Exclude<keyof ROW, "id">[];
  //   if (queryKeys.length === 0)
  //     return fromIterable(this._lmdb.getRange({}).map((e) => e.value));

  //   if (this._indexes) {
  //     for (const { columns, indexdb } of this._indexes) {
  //       const matchesAll = columns.every((col) => queryKeys.includes(col));
  //       if (matchesAll) {
  //         const searchValues = columns
  //           .map((col) => String(query[col]))
  //           .join("\x00");
  //         const indexRange = indexdb.getRange({
  //           start: searchValues,
  //           end: searchValues + "\xff",
  //         });

  //         const rowStream = new Stream<ROW>();
  //         const entries = [...indexRange];

  //         queueMicrotask(() => {
  //           for (const entry of entries) {
  //             const row = this._lmdb.get(entry.value as any);
  //             if (row) {
  //               const passesRemainingFilters = queryKeys.every(
  //                 (k) => String(row[k]) === String(query[k]),
  //               );
  //               if (passesRemainingFilters) rowStream.push(row);
  //             }
  //           }
  //         });
  //         return Source.from(rowStream);
  //       }
  //     }
  //   }

  //   const scanStream = new Stream<ROW>();
  //   const allEntries = [...this._lmdb.getRange({})];
  //   queueMicrotask(() => {
  //     for (const entry of allEntries) {
  //       if (queryKeys.every((k) => String(entry.value[k]) === String(query[k])))
  //         scanStream.push(entry.value);
  //     }
  //   });
  //   return Source.from(scanStream);
  // }

  insert(
    data: ROW,
  ): InTransaction<DB> extends true ? boolean : Promise<boolean> {
    this._schema?.parse(data);

    try {
      this._lmdb.getWriteTxnId();
      return this._insert(data) as never;
    } catch (error: any) {
      return this._lmdb.transaction(() => this._insert(data)) as never;
    }
  }
  update(
    data: Partial<Omit<ROW, "id">> & Pick<ROW, "id">,
  ): InTransaction<DB> extends true ? boolean : Promise<boolean> {
    this._schema?.partial().parse(data);

    try {
      this._lmdb.getWriteTxnId();
      return this._update(data) as never;
    } catch (error) {
      return this._lmdb.transaction(() => this._update(data)) as never;
    }
  }
  upsert(
    data: ROW,
  ): InTransaction<DB> extends true ? boolean : Promise<boolean> {
    if (this._lmdb.doesExist(data.id)) {
      return this.update(data) as never;
    } else {
      return this.insert(data) as never;
    }
  }
  delete(
    id: ROW["id"],
  ): InTransaction<DB> extends true ? boolean : Promise<boolean> {
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
  export type InferRow<T extends Table<any, any, any, any>> =
    T extends Table<any, any, any, infer R> ? R : never;
  export type Options<
    DB extends Db<any, true>,
    ROW extends Record<string, any>,
  > = {
    noValidation?: boolean;
    triggers?: Triggers<DB, ROW>;
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
}
