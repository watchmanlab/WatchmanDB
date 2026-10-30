import type { Key } from "lmdb";

export interface SchemaProvider<SCHEMA> {
  define<S extends SCHEMA>(schema: S): S;
  parse<S extends SCHEMA>(
    data: unknown,
    schema: S,
  ): Record<string, any> & { id: Key };
  partialParse<S extends SCHEMA>(
    data: unknown,
    schema: S,
  ): Partial<Record<string, any> & { id: Key }>;
}
