import type { Database } from "lmdb";
import type { Table } from "./table";
import { fromIterable, map, Source } from "@watchmanlab/stream";

export class Index {
  private _name: string;
  private _lmdb: Database;
  private _columns: string[];
  constructor(
    name: string,
    tableLmdb: Database,
    lmdb: Database,
    columns: string[],
  ) {
    this._name = name;
    this._lmdb = lmdb;
    this._columns = columns;

    // build the index if new and its table has rows
    if (
      lmdb.getKeys()[Symbol.iterator]().next().done &&
      !tableLmdb.getKeys()[Symbol.iterator]().next().done
    ) {
      tableLmdb.transaction(() =>
        tableLmdb.getRange().forEach(({ value }) => this.insert(value)),
      );
    }
  }

  private _makeIndexKey(row: Table.Row): string {
    const values = this._columns.map((col) => String(row[col]));
    return [...values, String(row.id)].join("\x00");
  }

  find(query: Record<string, any>): Source<any> | undefined {
    const queryKeys = Object.keys(query) as string[];

    const matchesAll = this._columns.every((col) =>
      queryKeys.includes(col as string),
    );
    if (!matchesAll) return;

    const searchValues = this._columns
      .map((col) => String(query[col]))
      .join("\x00");

    return fromIterable(
      this._lmdb.getRange({
        start: searchValues,
        end: searchValues + "\xff",
      }),
    ).pipe(map((e) => e.value));
  }
  insert(row: Table.Row) {
    const indexKey = this._makeIndexKey(row);
    this._lmdb.put(indexKey, row.id);
  }
  update(newRow: Table.Row, oldRow: Table.Row) {
    const oldIndexKey = this._makeIndexKey(oldRow);
    const newIndexKey = this._makeIndexKey(newRow);

    if (oldIndexKey !== newIndexKey) {
      this._lmdb.remove(oldIndexKey);
      this._lmdb.put(newIndexKey, newRow.id);
    }
  }

  delete(row: Table.Row) {
    const indexKey = this._makeIndexKey(row);
    this._lmdb.remove(indexKey);
  }
  drop(): Promise<void> {
    return this._lmdb.clearAsync();
  }
}
