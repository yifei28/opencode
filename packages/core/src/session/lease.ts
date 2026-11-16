export * as SessionLease from "./lease.js"

import { and, eq, gt, isNull, sql } from "drizzle-orm"
import { Clock, Context, Data, Deferred, Effect, Fiber, Option, Scope } from "effect"
import { hostname } from "node:os"
import { Database } from "../database/database.js"
import { KeyedMutex } from "../effect/keyed-mutex.js"
import { SessionLeaseTable } from "./sql.js"
import type { SessionSchema } from "./schema.js"

const lifetime = 60_000
const interval = 10_000

// Like permission refusals, ownership failures cross internal API boundaries as
// defects; the session HTTP middleware translates them into a typed 409 response.
export class Conflict extends Data.TaggedError("Session.LeaseConflict")<{
  sessionID: string
  hostname: string
  expired: boolean
}> {
  override get message() {
    return `This conversation ${this.expired ? "has an unresponsive owner" : "is active"} on ${this.hostname}. View it here, or use /fork to continue separately. The original conversation has not been taken over.`
  }
}

export class Guard extends Context.Service<
  Guard,
  {
    sessionID: string
    check: Effect.Effect<void>
  }
>()("@opencode/SessionLeaseGuard") {}

/** Check inside the write transaction, or immediately before starting a tool. */
export const check = (sessionID: string) =>
  Effect.gen(function* () {
    const guard = yield* Effect.serviceOption(Guard)
    if (Option.isSome(guard) && guard.value.sessionID === sessionID) yield* guard.value.check
  })

export const make = Effect.gen(function* () {
  const db = (yield* Database.Service).db
  const scope = yield* Scope.Scope
  const owner = crypto.randomUUID()
  const host = hostname()
  const locks = KeyedMutex.makeUnsafe<SessionSchema.ID>()
  type Entry = {
    generation: number
    users: number
    lost: Deferred.Deferred<never>
    heartbeat: Fiber.Fiber<void>
    check: Effect.Effect<void>
  }
  const entries = new Map<SessionSchema.ID, Entry>()
  const current = (sessionID: SessionSchema.ID) =>
    db.select().from(SessionLeaseTable).where(eq(SessionLeaseTable.session_id, sessionID)).get().pipe(Effect.orDie)
  const conflict = (sessionID: SessionSchema.ID) =>
    Effect.gen(function* () {
      const row = yield* current(sessionID)
      return yield* Effect.die(
        new Conflict({
          sessionID,
          hostname: row?.hostname ?? host,
          expired: !row || row.expires_at <= (yield* Clock.currentTimeMillis),
        }),
      )
    })

  const acquire = (sessionID: SessionSchema.ID) =>
    Effect.gen(function* () {
      const existing = entries.get(sessionID)
      if (existing) {
        yield* existing.check
        existing.users++
        return existing
      }
      const now = yield* Clock.currentTimeMillis
      // An expired remote owner is not evidence of death. Only explicit release
      // permits reuse; after a crash the user forks rather than stealing the turn.
      const row = yield* db
        .insert(SessionLeaseTable)
        .values({
          session_id: sessionID,
          owner_id: owner,
          hostname: host,
          expires_at: now + lifetime,
          generation: 1,
        })
        .onConflictDoUpdate({
          target: SessionLeaseTable.session_id,
          set: {
            owner_id: owner,
            hostname: host,
            expires_at: now + lifetime,
            generation: sql`${SessionLeaseTable.generation} + 1`,
          },
          where: isNull(SessionLeaseTable.owner_id),
        })
        .returning()
        .get()
        .pipe(Effect.orDie)
      if (!row) return yield* conflict(sessionID)
      const matches = and(
        eq(SessionLeaseTable.session_id, sessionID),
        eq(SessionLeaseTable.owner_id, owner),
        eq(SessionLeaseTable.generation, row.generation),
      )
      const verify = Effect.gen(function* () {
        const actual = yield* current(sessionID)
        if (
          actual?.owner_id !== owner ||
          actual.generation !== row.generation ||
          actual.expires_at <= (yield* Clock.currentTimeMillis)
        )
          yield* conflict(sessionID)
      })
      const lost = yield* Deferred.make<never>()
      const heartbeat = yield* Effect.gen(function* () {
        yield* Effect.sleep(interval)
        const now = yield* Clock.currentTimeMillis
        const renewed = yield* db
          .update(SessionLeaseTable)
          .set({ expires_at: now + lifetime })
          .where(and(matches, gt(SessionLeaseTable.expires_at, now)))
          .returning()
          .get()
          .pipe(Effect.orDie)
        if (!renewed) yield* conflict(sessionID)
      }).pipe(
        Effect.forever,
        Effect.catchCause((cause) => Deferred.failCause(lost, cause)),
        Effect.asVoid,
        Effect.forkIn(scope),
      )
      const entry = { generation: row.generation, users: 1, lost, heartbeat, check: verify }
      entries.set(sessionID, entry)
      return entry
    }).pipe(locks.withLock(sessionID))

  const release = (sessionID: SessionSchema.ID, entry: Entry) =>
    Effect.gen(function* () {
      entry.users--
      if (entry.users !== 0) return
      yield* Fiber.interrupt(entry.heartbeat)
      entries.delete(sessionID)
      yield* db
        .update(SessionLeaseTable)
        .set({ owner_id: null, expires_at: 0 })
        .where(
          and(
            eq(SessionLeaseTable.session_id, sessionID),
            eq(SessionLeaseTable.owner_id, owner),
            eq(SessionLeaseTable.generation, entry.generation),
          ),
        )
        .run()
        .pipe(Effect.catchCause(Effect.logError))
    }).pipe(locks.withLock(sessionID))

  return {
    use: <A, E, R>(sessionID: SessionSchema.ID, effect: Effect.Effect<A, E, R>) =>
      Effect.acquireUseRelease(
        acquire(sessionID),
        (entry) =>
          effect.pipe(
            Effect.provideService(Guard, { sessionID, check: entry.check }),
            Effect.raceFirst(Deferred.await(entry.lost)),
          ),
        (entry) => release(sessionID, entry),
      ),
  }
})
