import { Compile } from "typebox/compile";
import { Type } from "typebox";
import type { SchemaProvider } from "./schema-provider";

export function typeboxProvider<T extends Type.TObject>(
  schema: T,
): SchemaProvider<T, Type.Static<T>> {
  const compiler = Compile(schema);
  const compilerPartial = Compile(Type.Partial(schema));
  return {
    schema,
    parse: (data) => {
      if (compiler.Check(data)) return data as Type.Static<T>;
      throw new Error(compiler.Errors(data)[0]?.message || "Validation failed");
    },
    partialParse: (data) => {
      if (compilerPartial.Check(data)) return data as Partial<Type.Static<T>>;
      throw new Error(
        compilerPartial.Errors(data)[0]?.message || "Validation failed",
      );
    },
  };
}
