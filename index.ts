import z from "zod";
import { create } from "./src/db";
import { ZodSchema } from "./src/zod-schema";
import { TypBoxSchema } from "./src";
import Type from "typebox";
import {} from "bun";

try {
  await Bun.file("test.mdb").unlink();
  await Bun.file("test.mdb-lock").unlink();
} catch (error) {}
//
const db = create({
  path: "test.mdb",
  dbSchema: {
    user: new TypBoxSchema({
      id: Type.Number(),
      name: Type.String(),
      age: Type.Number(),
    }),
    apply: new ZodSchema({
      id: z.number(),
      userId: z.string(),
      price: z.number(),
    }),
  },
  indexes: {
    user: [["age"], ["name", "age"]],
    // apply: [["price"], ["userId", "price"]],
  },
  options: {
    compression: true,
  },
});

console.time("insert");
await db.transaction((db) => {
  for (let i = 1; i <= 100_000; i++) {
    db.tables.user.insert({ id: i + 10, name: `${i + 10}-sof`, age: i });
  }
});
console.timeEnd("insert");

console.time("find");

for (const user of db.tables.user.find({ age: 100, name: "110-sof" })) {
  console.log(user);
}
console.timeEnd("find");
