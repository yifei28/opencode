# Session Ownership (Experimental)

This branch adds conversation ownership on top of the NFS rollback-journal
branch. It does not change the production module or elect a shared backend.

## Behavior

- Independent servers cannot execute the same conversation concurrently.
- Clients connected to the same server continue to share its conversation.
- Reads and the existing `/fork` command remain available on another server.
- Conflicting session operations return HTTP 409 (`SessionBusyError`), naming
  the owning host and suggesting `/fork`. There is no new automatic fork or
  takeover dialog. The user chooses when to fork.
- Normal completion releases ownership. A crashed owner's lease is retained;
  after expiration it is reported as unresponsive, not assumed dead. Fork to
  continue with a separate conversation. There is no automatic takeover.

## Storage and Cost

`session_lease` is a new table in the existing database, created by the normal
migration machinery. It records session ID, owner-instance UUID, hostname,
expiration and generation. Acquisition is an atomic conditional upsert. Only
released rows can be acquired; each new acquisition advances the generation.

An occupied lease renews every 10 seconds and expires after 60 seconds without
renewal. Multiple local operations share one heartbeat for that session. Idle
sessions and readers do not poll. Mutating session operations briefly acquire
ownership; execution and shell work retain it until cleanup completes.

Lost ownership interrupts cooperative execution. Durable execution events
check the owner, generation and expiration inside their write transaction;
tools check ownership before dispatch. Cleanup only releases its own generation.
Prompt admission waits for the execution guard before giving up its reference.

## Boundaries

- All servers sharing a database must use this patch. Older clients do not
  participate in leases and can bypass them. Do not mix patched and old writers.
- This still requires reliable SQLite file locks. Use the rollback-journal fix
  for shared NFS storage; leases do not repair broken filesystem locking.
- The managed-service registration/port collision is a separate problem. Our
  cross-node tests use independent foreground servers and private runtime state.
- A heartbeat cannot prove that a remote process died. There is deliberately
  no automatic expired-lease takeover or public force-unlock endpoint.
- Already launched commands, external requests, and submitted Slurm jobs cannot
  be undone by an ownership check. This is not exactly-once tool execution or a
  security boundary against arbitrary plugins or direct database manipulation.
- Forks use OpenCode's existing history-copy behavior. They do not create an
  isolated filesystem; use separate worktrees for concurrent project edits.
- Background-job recovery and full managed-service/TUI workflows need a longer
  deployment pilot. Tests do not establish safety under arbitrary NFS outages,
  clock jumps, or non-cooperative external tools.

## Validation

Run focused tests from `packages/core`:

```sh
bun run script/test.ts test/session-lease.test.ts test/session-execution.test.ts test/session-run-coordinator.test.ts
```

The tests cover independent owners, same-owner reentrancy, separate sessions,
heartbeats, expiration without takeover, generation fencing, stale cleanup,
prompt-to-execution handoff, and restart-recovery isolation.

External disposable-server tests additionally exercise the actual HTTP API on
one host and across two login nodes: overlapping shell execution, rejection
without side effects, shared-server clients, reads, forks with preserved
messages, clean release, SIGKILL, and expired ownership. Those tests use bounded
shell waits and synthetic history, not model inference or GPU/Slurm workloads.
