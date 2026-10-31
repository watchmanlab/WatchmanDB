# WatchmanDB

A ultra-high-performance, lightweight, and embedded database engine built for Node.js and Bun. Architected directly on top of **LMDB native memory maps**, and full type safety.

WatchmanDB breaks away from traditional database constraints, unifying key/value performance, schema flexibility, polyglot validations, and user-land relational capabilities into a single, cohesive engine integrated directly with demand-driven reactive streams.

---

## Core Features & Architectural Breakthroughs

### 1. Compile-Time Transaction Guarding

WatchmanDB splits its surface API.

- **Outside Transactions:** Operations are asynchronous and return standard `Promise<boolean>` wrappers.
- **Inside Transaction Blocks:** The engine strips away the Promise allocation overhead completely. Operations return immediate, raw, synchronous `boolean` results inside the hot execution loop, unleashing maximum throughput.

### 2.Multi-Paradigm Adaptability

WatchmanDB handles structural shifts effortlessly, supporting multiple data paradigms simultaneously without adding specialized engine weight:

- **Key/Value Store:** Native, direct embedded performance with zero abstraction lookup penalties.
- **Document Store:** Polymorphic "MongoDB-like" row flexibility allowing varied shapes across rows in the same table.
- **Relational Database:** Fast, user-land joins executed on-demand via synchronous lookups or stream operators.

### 3. Schema Polymorphism

While the engine requires structural validation configurations, passing a permissive schema provider allows row attributes to vary natively across records in the _same_ table. You gain the open, schema-flexible document freedom of MongoDB paired with the strict, ACID transaction reliability of an embedded transactional engine—completely bypassing the need for database migration scripts.

### 4. Polyglot Schema Interoperability

Because the engine relies on the abstract `SchemaProvider` interface rather than a single coupled library dependency, you can natively combine entirely different validation engines (such as Zod, TypeBox, ArkType, or raw manual parsing functions) across separate tables within the _same_ runtime database configuration—maintaining absolute compile-time and runtime type safety throughout the system.

### 5. Reactive Pipeline Durability Channels

WatchmanDB operations are wired directly into a pull-on-push synchronous streaming pipeline via `@watchmanlab/stream`:

- **`$before*` Streams (Shallow Scratchpad):** Fully synchronous, lazy, and transient streams that trigger _before_ mutations are written to disk. Any validation rejection or exception thrown downstream will immediately bubble up and trigger a full, clean LMDB transaction abort and rollback.
- **`$after*` Streams (Durable Confirmation):** Microtask confirmation channels that fire exclusively after a transaction has successfully committed and synchronized data safely to the physical disk platter.
- The `Source` stream returned by `Table.find()` is **strictly lazy and demand-driven**. Calling `find()` allocates no cursors, schedules no microtasks, and performs **zero database or memory lookups** up front. The query path remains entirely dormant until a consumer explicitly consume the source. If a query stream is never consumed, its operational footprint on your CPU and memory cache is absolute zero.

- **`live queries`:** Because everything is natively composed around reactive streaming primitives, real-time live queries require no dedicated server-side engines or custom tracking layers. Users can create long-lived, real-time streaming queries entirely in user land by blending/merging initial history lookups with the `$afterInsert` / `$afterUpdate` channels.

- **Zero-Buffer Network Streaming:** Because even the synchronous pull mechanism is fully lazy on a per-value basis, database queries can be streamed directly into a web-standard `ReadableStream` for HTTP responses. Data flows seamlessly from LMDB's memory-mapped files (`mmap`) straight to the client application's network socket without allocating massive intermediate arrays or creating extra memory locations in the V8 heap.

---

## 📦 Installation

```bash
bun add @watchmanlab/db zod
```

---

## 🛠️ Complete Usage Example

### 1. Database Definition & Schema Provisioning

```typescript
import { z } from "zod";
import { create, ZodSchema } from "./db";

// You can implement other SchemaProviders like TypeBoxSchema or ArkTypeSchema
// seamlessly alongside Zod because of the decoupled SchemaProvider abstraction.

const db = create({
  path: "watchman.mdb",
  dbSchema: {
    user: new ZodSchema({
      id: z.string(),
      name: z.string(),
      age: z.number(),
    }),
    // Permissive, polymorphic table allowing schema flexibility
    request: new ZodSchema({
      id: z.string(),
      type: z.string(),
      userId: z.string(),
    }).looseObject(), // 👈 Allows arbitrary, category-specific fields across rows
  },
  indexes: {
    user: [["age"], ["name", "age"]],
    request: [["userId"], ["type", "userId"]],
  },
});
```

### 2. Stream Pipelines, Constraints, and Mutations

```typescript
import { map, toConsole } from "@watchmanlab/stream";

// 1. Hook into the shallow $beforeInsert pipeline for transaction-safe constraints
db.tables.request.$beforeInsert
  .pipe(
    map((row) => {
      const userExists = db.tables.user.get(row.userId);
      if (!userExists) {
        // Throwing an error instantly halts execution and rolls back all mutations
        throw new Error(
          `Abort: Target user context ${row.userId} does not exist.`,
        );
      }
      return row;
    }),
  )
  .pipe(toConsole());

// 2. Track real-time disk durability confirmations asynchronously
db.tables.request.$afterInsert.pipe(toConsole());

// 3. High-throughput synchronous transaction mutations
await db.transaction((tx) => {
  tx.tables.user.insert({ id: "usr_sofiane", name: "Sofiane", age: 42 });

  // Polymorphic insertions within the same dynamic table
  tx.tables.request.insert({
    id: "req_01",
    type: "ride",
    userId: "usr_sofiane",
    destinations: ["Point A", "Point B"],
    pauses: [5, 10], // Multi-destination pause configurations
  });

  tx.tables.request.insert({
    id: "req_02",
    type: "delivery",
    userId: "usr_sofiane",
    packageWeight: 12.5,
    fragile: true,
  });
});
```

### 3. Composing User-Land Relational Joins (Demand-Driven execution)

```typescript
import { map, toConsole, listen } from "@watchmanlab/stream";
// Lookups are lazy: find() executes NO database memory reads until source is consumed
const queryStream = db.tables.user.find({ name: "Sofiane" }).pipe(
  map((user) => ({
    ...user,
    requests: db.tables.request.find({ userId: user.id }),
  })),
);

// Execution kicks off here. Only now does LMDB execute memory range lookups.
queryStream.pipe(toConsole());
```

### 4. Composing User-Land Live Real-time Queries

```typescript
import { merge, filter } from "@watchmanlab/stream";

const targetUserId = "usr_sofiane";

// 1. Fetch historical data on demand (zero lookup cost until execution)
const history = db.tables.request.find({ userId: targetUserId });

// 2. Listen to the verified disk-commit stream for future updates
const updates = db.tables.request.afterInsert.pipe(
  filter((row) => row.userId === targetUserId),
);

// 3. Combine both tracks natively into a seamless, long-lived live stream
const liveRequestQuery = history.pipe(merge(updates));

for await (const request of liveRequestQuery) {
  console.log(request);
}
```

---

MIT License

Copyright (c) 2026 WatchmanLab
