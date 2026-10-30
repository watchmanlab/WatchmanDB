import type { Key } from "lmdb";
import type { Table } from "./table";

export interface SchemaProvider<SCHEMA> {
  define<S extends SCHEMA>(schema: S): S;
  parse<S extends SCHEMA>(data: unknown, schema: S): Table.Row;
}
