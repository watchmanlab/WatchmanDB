export interface SchemaProvider<SCHEMA, OUT extends Record<string, any>> {
  readonly schema: SCHEMA;
  parse(data: unknown): OUT;
  partialParse(data: unknown): Partial<OUT>;
}

export type InferProviderOutput<T> =
  T extends SchemaProvider<any, infer OUT> ? OUT : never;
