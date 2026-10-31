export interface SchemaProvider<SCHEMA, OUT extends Record<string, any>> {
  schema: SCHEMA;
  parse(data: unknown): Readonly<OUT>;
}

export namespace SchemaProvider {
  export type InferOutput<SP extends SchemaProvider<any, any>> =
    SP extends SchemaProvider<any, infer O> ? O : never;
}
