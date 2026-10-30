import { Source, Stream } from "@watchmanlab/stream";
import { open, type Database, type Key } from "lmdb";
import type { Db } from "./db";
import type { SchemaProvider } from "./schema-provider";
import { ZodSchema } from "./zod-schema";
import z from "zod";

export class Table<SCHEMA, ROW extends Table.Row, TX extends boolean = false> {
  private _lmdb: Database<Readonly<ROW>>;
  private _$insert?: Stream<ROW>;
  private _$update?: Stream<ROW>;
  private _$delete?: Stream<ROW>;
  private _schemaProvider: SchemaProvider<SCHEMA, ROW>;

  constructor(
    lmdb: Database,
    schemaProvider: SchemaProvider<SCHEMA, ROW>,
    private options?: Table.Options,
  ) {
    this._lmdb = lmdb;

    this._schemaProvider = schemaProvider;
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

  insert(data: ROW): TX extends true ? boolean : Promise<boolean> {
    try {
      this._lmdb.getWriteTxnId();
      return this._insert(data) as never;
    } catch (error: any) {
      return this._lmdb.transaction(() => this._insert(data)) as never;
    }
  }
  update(
    data: Partial<Omit<ROW, "id">> & Pick<ROW, "id">,
  ): TX extends true ? boolean : Promise<boolean> {
    try {
      this._lmdb.getWriteTxnId();
      return this._update(data) as never;
    } catch (error) {
      return this._lmdb.transaction(() => this._update(data)) as never;
    }
  }
  upsert(data: ROW): TX extends true ? boolean : Promise<boolean> {
    if (this._lmdb.doesExist(data.id)) {
      return this.update(data) as never;
    } else {
      return this.insert(data) as never;
    }
  }
  delete(id: ROW["id"]): TX extends true ? boolean : Promise<boolean> {
    try {
      this._lmdb.getWriteTxnId();
      return this._delete(id) as never;
    } catch (error) {
      return this._lmdb.transaction(() => this._delete(id)) as never;
    }
  }
  private _insert(data: ROW) {
    if (this._lmdb.doesExist(data.id)) return false;

    // this.options?.triggers?.beforeInsert?.(this._db, data);

    const parsed = Object.freeze(this._schemaProvider.parse(data));

    this._lmdb.put(parsed.id, parsed).then((r) => {
      if (r) this._$insert?.push(parsed);
    });

    return true;
  }
  private _update(data: Partial<Omit<ROW, "id">> & Pick<ROW, "id">) {
    const old = this._lmdb.get(data.id);
    if (!old) return false;

    // this.options?.triggers?.beforeUpdate?.(this._db, data, old);

    const parsed = Object.freeze(
      this._schemaProvider.parse({ ...old, ...data }),
    );

    this._lmdb.put(parsed.id, parsed).then((r) => {
      if (r) this._$update?.push(parsed);
    });

    return true;
  }
  private _delete(id: ROW["id"]) {
    const old = this._lmdb.get(id);
    if (!old) return false;

    // this?.options?.triggers?.beforeDelete?.(this._db, old);

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
  export type Row = Record<string, any> & { id: Key };
  export type InferRow<T extends Table<any, any, any>> =
    T extends Table<any, infer R, any> ? R : never;
  export type Options = {
    ignoreValidation?: boolean;
  };

  export type Triggers<DB extends Db<any, true>, ROW extends Row> = {
    beforeInsert?: (db: DB, data: ROW) => void;
    beforeUpdate?: (
      db: DB,
      data: Partial<Omit<ROW, "id">> & Pick<ROW, "id">,
      old: ROW,
    ) => void;
    beforeDelete?: (db: DB, old: ROW) => void;
  };
}

const lmdb = open("", {});
const table = new Table(
  lmdb,
  new ZodSchema({
    id: z.string(),
    name: z.string(),
    age: z.number(),
  }),
);

let v = table.insert({ id: "", name: "sof", age: 33 });
