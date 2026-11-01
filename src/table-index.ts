import type { Database } from "lmdb";
import type { Table } from "./table";
import { fromIterable, map, Source, tap } from "@watchmanlab/stream";

export class Index {
  private _name: string;
  private _lmdb: Database;
  private _columns: string[];
  private _keysSet: Set<string>;
  constructor(
    name: string,
    tableLmdb: Database,
    lmdb: Database,
    columns: string[],
  ) {
    this._name = name;
    this._lmdb = lmdb;
    this._columns = columns;
    this._keysSet = new Set<string>();

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
    const values = this._columns.map((col) => row[col] + "\x00");
    return [...values, row.id].join("\x00");
  }
  private _matchQuery(query: Record<string, any>): boolean {
    const queryKeys = Object.keys(query);
    const queryLen = queryKeys.length;

    if (this._columns.length < queryLen) return false;

    let matchCount = 0;

    for (let i = 0; i < queryLen; i++) {
      const index = this._columns[i]!;

      if (index in query) {
        matchCount++;
      } else {
        break;
      }
    }

    return matchCount === queryLen;
  }
  find(query: Record<string, any>): Source<any> | undefined {
    if (!this._matchQuery(query)) return;

    const searchValues = this._columns
      .map((col) => query[col] + "\x00")
      .join("\x00");

    console.log("query", Object.keys(query));
    console.log("index hit:", this._columns);
    console.log("---------------------");

    return fromIterable(
      this._lmdb.getRange({
        start: searchValues,
        end: searchValues + "\xff",
        inclusiveEnd: true,
      }),
    )
      .pipe(tap((v) => console.log("key found:", v.key)))
      .pipe(map((e) => e.value));
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
