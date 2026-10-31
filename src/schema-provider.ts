export abstract class SchemaProvider<SCHEMA, OUT extends Record<string, any>> {
  constructor(public readonly schema: SCHEMA) {}
  abstract parse(data: unknown): Readonly<OUT>;
}

export namespace SchemaProvider {
  export type InferOutput<SP extends SchemaProvider<any, any>> =
    SP extends SchemaProvider<any, infer O> ? O : never;
}
