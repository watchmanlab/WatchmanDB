import z from "zod";
import { Db } from "./src/db";
import { ZodSchema } from "./src/zod-schema";
import {
  listen,
  resolve,
  toConsole,
  delay,
  tapTerminate,
  fromIterable,
} from "@watchmanlab/stream";
import { TypBoxSchema } from "./src";
import Type from "typebox";
import {} from "bun";

try {
  await Bun.file("test.mdb").unlink();
  await Bun.file("test.mdb-lock").unlink();
} catch (error) {}
//
const db = Db.create({
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
    // user: [["age"], ["name", "age"]],
    // apply: [["price"], ["userId", "price"]],
  },
});

console.time("insert");
await db.transaction((db) => {
  for (let i = 1; i <= 250_000; i++) {
    db.tables.user.insert({ id: i, name: `${i}-sof`, age: i });
  }
});
console.timeEnd("insert"); //[983.52ms] how long????

console.time("find");

for (const user of db.tables.user.find()) {
  //   await new Promise((r) => setTimeout(r, 10));
  //   console.log(user.id);
}
console.timeEnd("find");

// db.tables.user.find().pipe(toConsole());

// console.log(db.env.getStats());
