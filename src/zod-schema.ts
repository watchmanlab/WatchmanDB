import type { Key } from "lmdb";
import { SchemaProvider } from "./schema-provider";
import { z } from "zod";

export class ZodSchema<
  SHAPE extends {
    readonly id: z.ZodType<Key>;
  },
> extends SchemaProvider<z.ZodObject<SHAPE>, z.output<z.ZodObject<SHAPE>>> {
  constructor(shape: SHAPE) {
    super(z.object(shape));
  }
  override parse(data: unknown): Readonly<z.output<z.ZodObject<SHAPE>>> {
    return Object.freeze(this.schema.parse(data));
  }
}
