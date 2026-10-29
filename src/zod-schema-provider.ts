import type { SchemaProvider } from "./schema-provider";
import { z } from "zod";

export function zodProvider<T extends z.core.$ZodShape>(
  shape: T,
): SchemaProvider<T, z.output<z.ZodObject<T>>> {
  return {
    shape,
    parse: (data) => z.object(shape).parse(data),
    partialParse: (data) => z.object(shape).partial().parse(data) as any,
  };
}
