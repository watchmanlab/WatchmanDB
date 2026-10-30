import type { Key } from "lmdb";

export abstract class SchemaProvider<SCHEMA, OUT extends Record<string, any>> {
  constructor(public readonly schema: SCHEMA) {}
  abstract parse(data: unknown): OUT;
}
