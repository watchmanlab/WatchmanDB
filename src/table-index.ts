import type { Database } from "lmdb";
import type { Table } from "./table";
import { filter, fromIterable, map, Source, tap } from "@watchmanlab/stream";
export type IndexMatchResult =
  | { type: "FULL_COVER" } // 0% row validation needed
  | { type: "RESIDUAL_FILTER"; indexKeysCount: number } // Use index first, then filter rows
  | { type: "NO_COVER" }; // Cannot use this index
export class Index {
  private _name: string;
  private _lmdb: Database;
  private _tableLmdb: Database;
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
    this._tableLmdb = tableLmdb;
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

  find(query: Record<string, any>, queryLen: number): Source<any> | undefined {
    const match = this._matchQuery(query, queryLen);

    // If the index doesn't align with the prefix at all, skip it immediately
    if (match.type === "NO_COVER") return;

    // Build the compound key prefix using only the index columns.
    // This works perfectly for both FULL_COVER and RESIDUAL_FILTER!
    const searchValues = this._columns
      .map((col) => query[col] + "\x00")
      .join("\x00");

    console.log("query", Object.keys(query));
    console.log("index hit:", this._columns, `(${match.type})`);
    console.log("---------------------");

    // Fetch from the index table range
    let source = fromIterable(
      this._lmdb.getRange({
        start: searchValues,
        end: searchValues + "\xff",
        inclusiveEnd: true,
      }),
    )
      .pipe(tap((v) => console.log("key found:", v.key)))
      .pipe(map((e) => e.value)); // Yields the Row IDs/Values

    // If the query is larger than the index, apply residual filtering to the stream
    if (match.type === "RESIDUAL_FILTER") {
      // Isolate the extra keys that aren't managed by this index
      const indexedColumns = this._columns;
      const residualKeys = Object.keys(query).filter(
        (k) => !indexedColumns.includes(k),
      );
      const residualLen = residualKeys.length;

      return source.pipe(
        filter((id) => {
          // 1. Fetch the raw row record from your primary storage table using the gathered ID
          // (If your index value *is* already the full row document, omit the .get(idOrRow) lookup)
          const row = this._tableLmdb.get(id);
          if (!row) return false;

          // 2. Performance-optimized, non-allocating filter loop
          for (let i = 0; i < residualLen; i++) {
            const key = residualKeys[i]!;
            if (row[key] !== query[key]) {
              return false; // Mismatch found, drop this candidate
            }
          }
          return true; // Valid record matches the extra fields!
        }),
      );
    }

    return source;
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
