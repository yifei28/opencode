import { HttpApiMiddleware } from "effect/http-api"
import { SessionBusyError } from "../errors.js"

export class SessionLeaseMiddleware extends HttpApiMiddleware.Service<SessionLeaseMiddleware>()(
  "@opencode/HttpApiSessionLease",
  { error: SessionBusyError },
) {}
