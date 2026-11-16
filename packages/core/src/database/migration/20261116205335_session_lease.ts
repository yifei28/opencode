import { Effect } from "effect"
import type { DatabaseMigration } from "../migration.js"

const migration: DatabaseMigration.Migration = {
  id: "20261116205335_session_lease",
  up(tx) {
    return Effect.gen(function* () {
      yield* tx.run(`
        CREATE TABLE \`session_lease\` (
          \`session_id\` text PRIMARY KEY,
          \`owner_id\` text,
          \`hostname\` text NOT NULL,
          \`expires_at\` integer NOT NULL,
          \`generation\` integer NOT NULL,
          CONSTRAINT \`fk_session_lease_session_id_session_v2_id_fk\` FOREIGN KEY (\`session_id\`) REFERENCES \`session_v2\`(\`id\`) ON DELETE CASCADE
        );
      `)
    })
  },
}

export default migration
