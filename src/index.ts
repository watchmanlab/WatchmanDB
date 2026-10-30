import type { Database } from "lmdb";
import type { Table } from "./table";
import { listen } from "@watchmanlab/stream";

export class Index<
  TABLE extends Table<any, any, any>,
  ROW extends Table.InferRow<TABLE> = Table.InferRow<TABLE>,
  COLS extends Exclude<keyof ROW, "id">[] = Exclude<keyof ROW, "id">[],
> {
  private _lmdb: Database;
  private _columns: COLS;
  constructor(lmdb: Database, table: TABLE, columns: COLS) {
    this._lmdb = lmdb;
    this._columns = columns;

    table.$beforeInsert.pipe(listen((row) => this._insert(row)));
    table.$beforeUpdate.pipe(listen(({ New, Old }) => this._update(New, Old)));
    table.$beforeDelete.pipe(listen((row) => this._delete(row)));
  }

  private _makeIndexKey(row: ROW): string {
    const values = this._columns.map((col) => String(row[col]));
    return [...values, String(row.id)].join("\x00");
  }

  private _insert(row: ROW) {
    const indexKey = this._makeIndexKey(row);
    this._lmdb.put(indexKey, row.id);
  }
  private _update(newRow: ROW, oldRow: ROW) {
    const oldIndexKey = this._makeIndexKey(oldRow);
    const newIndexKey = this._makeIndexKey(newRow);

    if (oldIndexKey !== newIndexKey) {
      this._lmdb.remove(oldIndexKey);
      this._lmdb.put(newIndexKey, newRow.id);
    }
  }

  private _delete(row: ROW) {
    const indexKey = this._makeIndexKey(row);
    this._lmdb.remove(indexKey);
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
