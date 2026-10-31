import { filter, fromIterable, map, Source, Stream } from "@watchmanlab/stream";
import type { Database, Key } from "lmdb";
import type { SchemaProvider } from "./schema-provider";
import type { Index } from "./index";

export class Table<SCHEMA, ROW extends Table.Row, TX extends boolean = false> {
  private _lmdb: Database<Readonly<ROW>>;
  private _schemaProvider: SchemaProvider<SCHEMA, ROW>;
  private _indexes?: Index<ROW>[];
  private _$beforeInsert?: Stream<Readonly<ROW>>;
  private _$afterInsert?: Stream<Readonly<ROW>>;
  private _$beforeUpdate?: Stream<{ New: Readonly<ROW>; Old: Readonly<ROW> }>;
  private _$afterUpdate?: Stream<{ New: Readonly<ROW>; Old: Readonly<ROW> }>;
  private _$beforeDelete?: Stream<Readonly<ROW>>;
  private _$afterDelete?: Stream<Readonly<ROW>>;

  constructor(
    lmdb: Database,
    schemaProvider: SchemaProvider<SCHEMA, ROW>,
    indexes?: Index<ROW>[],
  ) {
    this._lmdb = lmdb;
    this._schemaProvider = schemaProvider;
    this._indexes = indexes;
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

  find(query: Partial<Omit<ROW, "id">>): Source<ROW> {
    const queryKeys = Object.keys(query) as string[];

    if (queryKeys.length === 0)
      return fromIterable(this._lmdb.getRange({})).pipe(map((e) => e.value));

    // console.log("ddd");

    if (this._indexes) {
      for (const index of this._indexes) {
        const source = index.find(query);
        if (!source) continue;

        return source
          .pipe(map((id) => this.get(id)))
          .pipe(
            filter((row) =>
              queryKeys.every(
                (k) => String((row as any)[k]) === String((query as any)[k]),
              ),
            ),
          );
      }
    }

    return fromIterable(this._lmdb.getRange({}))
      .pipe(map((e) => e.value))
      .pipe(
        filter((row) =>
          queryKeys.every(
            (k) => String((row as any)[k]) === String((query as any)[k]),
          ),
        ),
      );
  }

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

    for (let i = 0; i < (this._indexes?.length ?? 0); i++) {
      this._indexes?.[i]?.insert(parsed);
    }

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

    for (let i = 0; i < (this._indexes?.length ?? 0); i++) {
      this._indexes?.[i]?.update(parsed, Old);
    }

    this._lmdb.put(parsed.id, parsed).then((r) => {
      if (r) this._$afterUpdate?.push({ New: parsed, Old });
    });

    return true;
  }
  private _delete(id: ROW["id"]) {
    const old = this._lmdb.get(id);
    if (!old) return false;

    this._$beforeDelete?.push(old);

    for (let i = 0; i < (this._indexes?.length ?? 0); i++) {
      this._indexes?.[i]?.insert(old);
    }

    this._lmdb.remove(old.id).then((r) => {
      if (r) this._$afterDelete?.push(old);
    });
    return true;
  }
  async drop(): Promise<void> {
    await Promise.all([
      this._lmdb.clearAsync(),
      this._indexes?.map((idx) => idx.drop()),
    ]);
    return;
  }
}

export namespace Table {
  export type Row = Record<string, any> & { id: Key };
  export type InferRow<T extends Table<any, any, any>> =
    T extends Table<any, infer R, any> ? R : never;
}
