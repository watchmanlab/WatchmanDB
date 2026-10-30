import type { Db } from "./db";
import type { Table } from "./table";

export class Index<DB extends Db<any>,TABLE extends Table<any,any>> {

    private _db:DB
  constructor(db:DB,table:) {

    this._db = db
  }

  private _makeIndexKey(
    data: ROW,
    columns: Exclude<keyof ROW, "id">[],
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
