import type { Database } from "lmdb";
import type { Db } from "./db";
import type { Table } from "./table";

export class Index<
  DB extends Db<any, true>,
  TABLE extends Table<DB, any, any>,
  ROW extends Table.InferRow<TABLE> = Table.InferRow<TABLE>,
> {
  private _lmdb: Database;
  private _table: TABLE;
  private _columns: (keyof ROW)[];
  constructor(lmdb: Database, table: TABLE, columns: (keyof ROW)[]) {
    this._lmdb = lmdb;
    this._table = table;
    this._columns = columns;
  }

  private _makeIndexKey(
    data: Table.Row,
    columns: Exclude<keyof Table.Row, "id">[],
  ): string {
    const values = columns.map((col) => String(data[col]));
    return [...values, String(data.id)].join("\x00");
  }

  insert() {
    if (this._indexes) {
      for (const { columns, indexdb } of this._indexes) {
        const indexKey = this._makeIndexKey(data, columns);
        indexdb.put(indexKey, data.id);
      }
    }
  }
  update() {
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
  }

  delete() {
    if (this._indexes) {
      for (const { columns, indexdb } of this._indexes) {
        const indexKey = this._makeIndexKey(old, columns);
        indexdb.remove(indexKey);
      }
    }
  }
}

export namespace Index {
  export type InferIndex<T extends string, U extends string = T> = [T] extends [
    never,
  ]
    ? ""
    : T extends any
      ?
          | T
          | (InferIndex<Exclude<U, T>> extends ""
              ? never
              : `${T}:${InferIndex<Exclude<U, T>>}`)
      : never;
}
