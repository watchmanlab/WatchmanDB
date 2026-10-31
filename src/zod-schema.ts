import type { Key } from "lmdb";
import type { SchemaProvider } from "./schema-provider.js";
import { z } from "zod";

export class ZodSchema<
  SHAPE extends {
    readonly id: z.ZodString | z.ZodNumber;
  },
> implements SchemaProvider<z.ZodObject<SHAPE>, z.output<z.ZodObject<SHAPE>>> {
  schema: z.ZodObject<SHAPE, z.core.$strip>;
  constructor(shape: SHAPE) {
    this.schema = z.object(shape);
  }
  parse(data: unknown): Readonly<z.output<z.ZodObject<SHAPE>>> {
    return Object.freeze(this.schema.parse(data));
  }
  looseObject() {
    this.schema = z.looseObject(this.schema.shape);
    return this;
  }
}
