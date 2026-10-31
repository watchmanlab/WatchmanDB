import z from "zod";
import { Db } from "./src/db";
import { ZodSchema } from "./src/zod-schema";
import { toConsole } from "@watchmanlab/stream";

try {
  await Bun.file("test.mdb").unlink();
  await Bun.file("test.mdb-lock").unlink();
} catch (error) {}
//
const db = Db.create({
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
    user: [["age"], ["name", "age"]],
    apply: [["price"], ["userId", "price"]],
  },
});

await db.transaction((db) => {
  db.tables.user.insert({ id: "a", name: "sof", age: 42 });
  db.tables.user.insert({ id: "b", name: "dahmounth", age: 33 });
  db.tables.user.insert({ id: "c", name: "yanis", age: 6 });
  db.tables.user.insert({ id: "d", name: "iline", age: 9 });
  // db.tables.apply.insert({ id: 123, userId: "a", price: 200 });
});

db.tables.user.find().pipe(toConsole());
