import type { SchemaProvider } from "./schema-provider";
import { z } from "zod";

export function zodProvider<T extends z.ZodObject>(
  schema: T,
): SchemaProvider<T, z.output<T>> {
  return {
    schema,
    parse: (data) => schema.parse(data),
    partialParse: (data) => schema.partial().parse(data) as any,
  };
}
