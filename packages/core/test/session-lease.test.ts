import { expect } from "bun:test"
import { Clock, Deferred, Effect, Exit, Fiber } from "effect"
import { TestClock } from "effect/testing"
import { eq } from "drizzle-orm"
import { Database } from "@opencode/core/database/database"
import { AppNodeBuilder } from "@opencode/core/effect/app-node-builder"
import { ProjectTable } from "@opencode/core/project/sql"
import { Project } from "@opencode/core/project"
import { AbsolutePath } from "@opencode/core/schema"
import { SessionSchema } from "@opencode/core/session/schema"
import { SessionLease } from "@opencode/core/session/lease"
import { SessionRunCoordinator } from "@opencode/core/session/run-coordinator"
import { SessionLeaseTable, SessionTable } from "@opencode/core/session/sql"
import { testEffect } from "./lib/effect"

const it = testEffect(AppNodeBuilder.build(Database.node))
const seed = Effect.gen(function* () {
  const db = (yield* Database.Service).db
  const sessionID = SessionSchema.ID.create()
  yield* db
    .insert(ProjectTable)
    .values({
      id: Project.ID.global,
      worktree: AbsolutePath.make("/project"),
      sandboxes: [],
    })
    .onConflictDoNothing()
    .run()
    .pipe(Effect.orDie)
  yield* db
    .insert(SessionTable)
    .values({
      id: sessionID,
      project_id: Project.ID.global,
      slug: sessionID,
      directory: "/project",
      title: "lease test",
      version: "test",
    })
    .run()
    .pipe(Effect.orDie)
  return { db, sessionID, a: yield* SessionLease.make, b: yield* SessionLease.make }
})

it.effect("only one independent owner can execute; release advances generation", () =>
  Effect.gen(function* () {
    const { db, sessionID, a, b } = yield* seed
    yield* a.use(
      sessionID,
      Effect.gen(function* () {
        expect(Exit.isFailure(yield* b.use(sessionID, Effect.void).pipe(Effect.exit))).toBe(true)
        yield* a.use(sessionID, SessionLease.check(sessionID))
        expect((yield* db.select().from(SessionLeaseTable).get())?.generation).toBe(1)
      }),
    )
    yield* b.use(
      sessionID,
      Effect.gen(function* () {
        expect((yield* db.select().from(SessionLeaseTable).get())?.generation).toBe(2)
      }),
    )
    expect((yield* db.select().from(SessionLeaseTable).get())?.owner_id).toBeNull()
  }),
)

it.effect("different sessions can execute independently", () =>
  Effect.gen(function* () {
    const first = yield* seed
    const second = yield* seed
    yield* first.a.use(first.sessionID, first.b.use(second.sessionID, Effect.void))
  }),
)

it.effect("an expired foreign owner is not silently taken over", () =>
  Effect.gen(function* () {
    const { db, sessionID, a, b } = yield* seed
    yield* a.use(
      sessionID,
      Effect.gen(function* () {
        yield* db.update(SessionLeaseTable).set({ expires_at: 0 }).run()
        expect(Exit.isFailure(yield* SessionLease.check(sessionID).pipe(Effect.exit))).toBe(true)
        expect(Exit.isFailure(yield* b.use(sessionID, Effect.void).pipe(Effect.exit))).toBe(true)
      }),
    )
  }),
)

it.effect("generation fences stale work and stale cleanup cannot release the new owner", () =>
  Effect.gen(function* () {
    const { db, sessionID, a, b } = yield* seed
    const started = yield* Deferred.make<void>()
    const check = yield* Deferred.make<void>()
    const old = yield* a
      .use(
        sessionID,
        Effect.gen(function* () {
          yield* Deferred.succeed(started, undefined)
          yield* Deferred.await(check)
          yield* SessionLease.check(sessionID)
        }),
      )
      .pipe(Effect.exit, Effect.forkScoped)
    yield* Deferred.await(started)
    // Simulate a future explicit recovery operation; production never steals this row.
    yield* db.update(SessionLeaseTable).set({ owner_id: null }).run()
    yield* b.use(
      sessionID,
      Effect.gen(function* () {
        yield* Deferred.succeed(check, undefined)
        expect(Exit.isFailure(yield* Fiber.join(old))).toBe(true)
        yield* SessionLease.check(sessionID)
        expect((yield* db.select().from(SessionLeaseTable).get())?.owner_id).not.toBeNull()
      }),
    )
  }),
)

it.effect("heartbeat keeps a long-running execution owned and is stopped at release", () =>
  Effect.gen(function* () {
    const { db, sessionID, a, b } = yield* seed
    const started = yield* Deferred.make<void>()
    const running = yield* a
      .use(sessionID, Deferred.succeed(started, undefined).pipe(Effect.andThen(Effect.never)))
      .pipe(Effect.forkScoped)
    yield* Deferred.await(started)
    yield* TestClock.adjust(70_000)
    const row = yield* db.select().from(SessionLeaseTable).get()
    expect(row!.expires_at).toBeGreaterThan(yield* Clock.currentTimeMillis)
    expect(Exit.isFailure(yield* b.use(sessionID, Effect.void).pipe(Effect.exit))).toBe(true)
    yield* Fiber.interrupt(running)
    yield* TestClock.adjust(20_000)
    expect((yield* db.select().from(SessionLeaseTable).get())?.owner_id).toBeNull()
  }),
)

it.effect("heartbeat ownership loss interrupts work and fences finalizer writes", () =>
  Effect.gen(function* () {
    const { db, sessionID, a } = yield* seed
    const started = yield* Deferred.make<void>()
    const running = yield* a
      .use(
        sessionID,
        Deferred.succeed(started, undefined).pipe(
          Effect.andThen(Effect.never),
          Effect.ensuring(
            SessionLease.check(sessionID).pipe(
              Effect.andThen(
                db
                  .update(SessionTable)
                  .set({ title: "stale" })
                  .where(eq(SessionTable.id, sessionID))
                  .run()
                  .pipe(Effect.orDie),
              ),
            ),
          ),
        ),
      )
      .pipe(Effect.exit, Effect.forkScoped)
    yield* Deferred.await(started)
    yield* db.update(SessionLeaseTable).set({ generation: 2 }).run()
    yield* TestClock.adjust(10_000)
    expect(Exit.isFailure(yield* Fiber.join(running))).toBe(true)
    expect((yield* db.select().from(SessionTable).get())?.title).toBe("lease test")
  }),
)

it.effect("wake holds ownership before admission releases and conflicts do not drain", () =>
  Effect.gen(function* () {
    const { sessionID, a, b } = yield* seed
    const coordinator = yield* SessionRunCoordinator.make<SessionSchema.ID, never>({
      guard: a.use,
      drain: () => Effect.never,
    })
    let foreignDrains = 0
    const other = yield* SessionRunCoordinator.make<SessionSchema.ID, never>({
      guard: b.use,
      drain: () =>
        Effect.sync(() => {
          foreignDrains++
        }),
    })
    yield* a.use(sessionID, coordinator.wake(sessionID))
    expect(yield* coordinator.isActive(sessionID)).toBe(true)
    expect(Exit.isFailure(yield* other.wake(sessionID).pipe(Effect.exit))).toBe(true)
    expect(foreignDrains).toBe(0)
    yield* coordinator.interrupt(sessionID)
    yield* coordinator.awaitIdle(sessionID)
    yield* b.use(sessionID, Effect.void)
  }),
)
