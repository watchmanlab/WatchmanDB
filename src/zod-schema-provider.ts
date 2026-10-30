import type { Key } from "lmdb";
import type { SchemaProvider } from "./schema-provider";
import { z } from "zod";

export class ZodProvider<
  SCHEMA extends z.ZodObject<{ id: z.ZodType<Key> }>,
> implements SchemaProvider<SCHEMA> {
  define<S extends SCHEMA>(schema: S): S {
    return schema;
  }
  parse<S extends SCHEMA>(data: unknown, schema: S): z.output<S> {
    return schema.parse(data);
  }
}
