import Type from "typebox";
import Schema from "typebox/schema";
import type { SchemaProvider } from "./schema-provider.js";

export class TypBoxSchema<
  SHAPE extends { readonly id: Type.TString | Type.TNumber },
> implements SchemaProvider<
  Type.TObject<SHAPE>,
  Type.Static<Type.TObject<SHAPE>>
> {
  schema: Type.TObject<SHAPE>;
  private validator: Schema.Validator;
  constructor(shape: SHAPE, options?: Type.TObjectOptions) {
    this.schema = Type.Object(shape, options);

    this.validator = Schema.Compile(this.schema);
  }
  parse(data: unknown): Readonly<Type.Static<Type.TObject<SHAPE>>> {
    return this.validator.Parse(data) as never;
  }
}
