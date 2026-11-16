import { SessionLease } from "@opencode/core/session/lease"
import { SessionBusyError } from "@opencode/protocol/errors"
import { SessionLeaseMiddleware } from "@opencode/protocol/middleware/session-lease"
import { Session } from "@opencode/schema/session"
import { Effect, Layer } from "effect"

export const sessionLeaseLayer = Layer.succeed(
  SessionLeaseMiddleware,
  SessionLeaseMiddleware.of((effect) =>
    effect.pipe(
      Effect.catchDefect((error) =>
        error instanceof SessionLease.Conflict
          ? Effect.fail(new SessionBusyError({ sessionID: Session.ID.make(error.sessionID), message: error.message }))
          : Effect.die(error),
      ),
    ),
  ),
)
