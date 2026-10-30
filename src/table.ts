import { Source, Stream } from "@watchmanlab/stream";
import type { Database, Key } from "lmdb";
import type { SchemaProvider } from "./schema-provider";

export class Table<SCHEMA, ROW extends Table.Row, TX extends boolean = false> {
  private _lmdb: Database<Readonly<ROW>>;
  private _schemaProvider: SchemaProvider<SCHEMA, ROW>;
  private _$beforeInsert?: Stream<Readonly<ROW>>;
  private _$afterInsert?: Stream<Readonly<ROW>>;
  private _$beforeUpdate?: Stream<{ New: Readonly<ROW>; Old: Readonly<ROW> }>;
  private _$afterUpdate?: Stream<{ New: Readonly<ROW>; Old: Readonly<ROW> }>;
  private _$beforeDelete?: Stream<Readonly<ROW>>;
  private _$afterDelete?: Stream<Readonly<ROW>>;

  constructor(lmdb: Database, schemaProvider: SchemaProvider<SCHEMA, ROW>) {
    this._lmdb = lmdb;
    this._schemaProvider = schemaProvider;
  }
  get $afterInsert(): Source<Readonly<ROW>> {
    return Source.from((this._$afterInsert ??= new Stream()));
  }
  get $beforeInsert(): Source<Readonly<ROW>> {
    return Source.from((this._$beforeInsert ??= new Stream()));
  }
  get $beforeUpdate(): Source<{ New: Readonly<ROW>; Old: Readonly<ROW> }> {
    return Source.from((this._$beforeUpdate ??= new Stream()));
  }
  get $afterUpdate(): Source<{ New: Readonly<ROW>; Old: Readonly<ROW> }> {
    return Source.from((this._$afterUpdate ??= new Stream()));
  }
  get $beforeDelete(): Source<Readonly<ROW>> {
    return Source.from((this._$beforeDelete ??= new Stream()));
  }
  get $afterDelete(): Source<Readonly<ROW>> {
    return Source.from((this._$afterDelete ??= new Stream()));
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

  private _isInsideTransaction(): boolean {
    try {
      return this._lmdb.getWriteTxnId() > 0;
    } catch (error) {
      return false;
    }
  }
  insert(row: ROW): TX extends true ? boolean : Promise<boolean> {
    if (this._isInsideTransaction()) {
      return this._insert(row) as never;
    } else {
      return this._lmdb.transaction(() => this._insert(row)) as never;
    }
  }
  update(
    row: Partial<Omit<ROW, "id">> & Pick<ROW, "id">,
  ): TX extends true ? boolean : Promise<boolean> {
    if (this._isInsideTransaction()) {
      return this._update(row) as never;
    } else {
      return this._lmdb.transaction(() => this._update(row)) as never;
    }
  }
  upsert(row: ROW): TX extends true ? boolean : Promise<boolean> {
    if (this._lmdb.doesExist(row.id)) {
      return this.update(row) as never;
    } else {
      return this.insert(row) as never;
    }
  }
  delete(id: ROW["id"]): TX extends true ? boolean : Promise<boolean> {
    if (this._isInsideTransaction()) {
      return this._delete(id) as never;
    } else {
      return this._lmdb.transaction(() => this._delete(id)) as never;
    }
  }
  private _insert(row: ROW) {
    if (this._lmdb.doesExist(row.id)) return false;

    const parsed = this._schemaProvider.parse(row);

    this._$beforeInsert?.push(parsed);

    this._lmdb.put(parsed.id, parsed).then((r) => {
      if (r) this._$afterInsert?.push(parsed);
    });

    return true;
  }
  private _update(row: Partial<Omit<ROW, "id">> & Pick<ROW, "id">) {
    const Old = this._lmdb.get(row.id);
    if (!Old) return false;

    const parsed = this._schemaProvider.parse({ ...Old, ...row });

    this._$beforeUpdate?.push({ New: parsed, Old });

    this._lmdb.put(parsed.id, parsed).then((r) => {
      if (r) this._$afterUpdate?.push({ New: parsed, Old });
    });

    return true;
  }
  private _delete(id: ROW["id"]) {
    const old = this._lmdb.get(id);
    if (!old) return false;

    this._$beforeDelete?.push(old);

    this._lmdb.remove(old.id).then((r) => {
      if (r) this._$afterDelete?.push(old);
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
}
