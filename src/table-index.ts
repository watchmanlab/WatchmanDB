import type { Database, RootDatabase } from "lmdb";
import type { Table } from "./table";
import {
  filter,
  fromIterable,
  listen,
  map,
  Source,
  tap,
} from "@watchmanlab/stream";
export type IndexMatchResult =
  | { type: "FULL_COVER" } // 0% row validation needed
  | { type: "RESIDUAL_FILTER"; indexKeysCount: number } // Use index first, then filter rows
  | { type: "NO_COVER" }; // Cannot use this index
export class Index {
  private _name: string;
  private _db: Database;
  private _table: Table<any, any, any, any>;
  private _columns: string[];

  constructor(
    env: RootDatabase,
    table: Table<any, any, any, any>,
    columns: string[],
  ) {
    this._name = ["index", ...columns, table.name].join(":");
    this._db = env.openDB({ name: this._name });
    this._table = table;
    this._columns = columns;

    // build the index if new and its table has rows
    if (
      this._db.getKeys()[Symbol.iterator]().next().done &&
      !this._table.find()[Symbol.iterator]().next().done
    ) {
      this._db.transaction(() =>
        this._table.find().pipe(listen((row) => this.insert(row))),
      );
    }
  }
  get columns(): string[] {
    return this._columns;
  }
  private _makeIndexKey(row: Table.Row): string {
    const values = this._columns.map((col) => row[col] + "\x00");
    return [...values, row.id].join("\x00");
  }

  private _matchQuery(
    query: Record<string, any>,
    queryLen: number,
  ): IndexMatchResult {
    const indexLen = this._columns.length;

    let matchCount = 0;

    for (let i = 0; i < Math.min(queryLen, indexLen); i++) {
      if (this._columns[i]! in query) {
        matchCount++;
      } else {
        break;
      }
    }

    if (matchCount === queryLen) {
      return { type: "FULL_COVER" };
    }

    // Case 2: The query is larger, but the index columns are fully satisfied.
    // This is your Residual Filtering sweet spot!
    if (matchCount === indexLen) {
      return { type: "RESIDUAL_FILTER", indexKeysCount: indexLen };
    }

    // Case 3: Leftmost prefix broke early and didn't cover either criteria fully
    return { type: "NO_COVER" };
  }

  find(
    query: Record<string, any>,
    queryLen: number,
  ): { source: Source<any>; strategy: Table.IndexStrategy } | undefined {
    const match = this._matchQuery(query, queryLen);

    if (match.type === "NO_COVER") return;

    const searchValues = this._columns
      .map((col) => query[col] + "\x00")
      .join("\x00");

    let source = fromIterable(
      this._db.getRange({
        start: searchValues,
        end: searchValues + "\xff",
        inclusiveEnd: true,
      }),
    ).pipe(map((e) => this._table.get(e.value)!));

    if (match.type === "RESIDUAL_FILTER") {
      const indexedColumns = this._columns;
      const residualKeys = Object.keys(query).filter(
        (k) => !indexedColumns.includes(k),
      );
      const residualLen = residualKeys.length;

      return {
        source: source.pipe(
          filter((row) => {
            for (let i = 0; i < residualLen; i++) {
              const key = residualKeys[i]!;
              if (row[key] !== query[key]) {
                return false;
              }
            }
            return true;
          }),
        ),
        strategy: "NRESIDUAL_FILTER",
      };
    }

    return { source, strategy: "FULL_COVER" };
  }

  insert(row: Table.Row) {
    const indexKey = this._makeIndexKey(row);
    this._db.put(indexKey, row.id);
  }
  update(newRow: Table.Row, oldRow: Table.Row) {
    const oldIndexKey = this._makeIndexKey(oldRow);
    const newIndexKey = this._makeIndexKey(newRow);

    if (oldIndexKey !== newIndexKey) {
      this._db.remove(oldIndexKey);
      this._db.put(newIndexKey, newRow.id);
    }
  }

  delete(row: Table.Row) {
    const indexKey = this._makeIndexKey(row);
    this._db.remove(indexKey);
  }
  clear(): Promise<void> {
    return this._db.clearAsync();
  }
}
