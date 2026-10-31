import z from "zod";
import { create } from "./src/db";
import { ZodSchema } from "./src/zod-schema";
import { toConsole } from "@watchmanlab/stream";

try {
  await Bun.file("test.mdb").unlink();
  await Bun.file("test.mdb-lock").unlink();
} catch (error) {}

const db = create({
  path: "test.mdb",
  dbSchema: {
    user: new ZodSchema({
      id: z.string(),
      name: z.string(),
      age: z.number(),
    }).looseObject(),
    apply: new ZodSchema({
      id: z.number(),
      userId: z.string(),
      price: z.number(),
    }),
  },
  indexes: {
    // user: [["age"], ["age", "name"]],
    // apply: [["price"], ["userId", "price"]],
  },
});

await db.transaction((db) => {
  db.tables.user.insert({ id: "a", name: "sof", age: 44 });
  db.tables.user.insert({ id: "b", name: "dahmounth", age: 33 });
  // db.tables.apply.insert({ id: 123, userId: "a", price: 200 });
});

db.tables.user.find({ name: "dahmounth" }).pipe(toConsole());
