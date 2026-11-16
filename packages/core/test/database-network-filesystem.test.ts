import { expect, test } from "bun:test"
import { Database } from "bun:sqlite"
import { existsSync } from "node:fs"
import { join } from "node:path"
import { Effect } from "effect"
import { Service, layer } from "@opencode/core/database/database"
import { Global } from "@opencode/util/global"
import { isNetworkFilesystemType } from "@opencode/core/database/network-filesystem"
import { tmpdir } from "./fixture/tmpdir"

test.each([0x517b, 0x1021997, 0x65735546, 0x6969, 0xff534d42])("detects network type %i", (type) => {
  expect(isNetworkFilesystemType(type)).toBe(true)
})

test("local ext filesystem is not classified as network", () => {
  expect(isNetworkFilesystemType(0xef53)).toBe(false)
})

const inspect = (filename: string, wal?: boolean) =>
  Effect.runPromise(
    Effect.gen(function* () {
      const { db } = yield* Service
      return {
        mode: yield* db.all<{ journal_mode: string }>("PRAGMA journal_mode"),
        integrity: yield* db.all<{ integrity_check: string }>("PRAGMA integrity_check"),
        shm: existsSync(filename + "-shm"),
      }
    }).pipe(
      Effect.provide(layer({ path: filename, wal })),
      Effect.provideService(Global.Service, Global.make({ data: "/tmp" })),
      Effect.scoped,
    ),
  )

test("local filesystem defaults to WAL", async () => {
  await using tmp = await tmpdir()
  expect((await inspect(join(tmp.path, "probe.db"))).mode).toEqual([{ journal_mode: "wal" }])
})

test("explicit rollback has no SHM and survives reopen", async () => {
  await using tmp = await tmpdir()
  const filename = join(tmp.path, "probe.db")
  for (let i = 0; i < 2; i++) {
    expect(await inspect(filename, false)).toEqual({
      mode: [{ journal_mode: "delete" }],
      integrity: [{ integrity_check: "ok" }],
      shm: false,
    })
  }
})

test("WAL-to-rollback conversion preserves existing data", async () => {
  await using tmp = await tmpdir()
  const filename = join(tmp.path, "probe.db")
  await inspect(filename, true)
  const original = new Database(filename)
  original.run("CREATE TABLE probe (value TEXT)")
  original.run("INSERT INTO probe VALUES ('committed')")
  original.close()
  expect((await inspect(filename, false)).mode).toEqual([{ journal_mode: "delete" }])
  const converted = new Database(filename)
  expect(converted.query("SELECT value FROM probe").all()).toEqual([{ value: "committed" }])
  converted.close()
  expect((await inspect(filename, true)).mode).toEqual([{ journal_mode: "wal" }])
})
