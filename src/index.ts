import type { Database } from "lmdb";
import type { Table } from "./table";
import { fromIterable, map, Source } from "@watchmanlab/stream";

export class Index<
  ROW extends Table.Row,
  COL extends Exclude<keyof ROW, "id"> = Exclude<keyof ROW, "id">,
> {
  private _lmdb: Database;
  private _columns: COL[];
  constructor(lmdb: Database, columns: COL[]) {
    this._lmdb = lmdb;
    this._columns = columns;
  }

  private _makeIndexKey(row: ROW): string {
    const values = this._columns.map((col) => String(row[col]));
    return [...values, String(row.id)].join("\x00");
  }

  find(query: Record<any, any>): Source<ROW["id"]> | undefined {
    const queryKeys = Object.keys(query) as string[];

    // Check if this index completely satisfies all fields in the user query
    const matchesAll = this._columns.every((col) =>
      queryKeys.includes(col as string),
    );
    if (!matchesAll) return;

    // Construct composite search key up to the index boundary
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
  insert(row: ROW) {
    const indexKey = this._makeIndexKey(row);
    this._lmdb.put(indexKey, row.id);
  }
  update(newRow: ROW, oldRow: ROW) {
    const oldIndexKey = this._makeIndexKey(oldRow);
    const newIndexKey = this._makeIndexKey(newRow);

    if (oldIndexKey !== newIndexKey) {
      this._lmdb.remove(oldIndexKey);
      this._lmdb.put(newIndexKey, newRow.id);
    }
  }

  delete(row: ROW) {
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

const idx = new Index<{
  id: number;
  name: string;
  age: number;
  location: [number, number];
}>({} as Database, ["name", "age"]);
