export interface SchemaProvider<
  SHAPE extends Record<string, any>,
  OUT extends Record<string, any>,
> {
  readonly shape: SHAPE;
  parse(data: unknown): OUT;
  partialParse(data: unknown): Partial<OUT>;
}

export type InferProviderOutput<T> =
  T extends SchemaProvider<any, infer OUT> ? OUT : never;
