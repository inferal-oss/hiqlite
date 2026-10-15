# Snapshot sync error regression

Run the deterministic regression tests from the repository root:

```sh
cargo test -p hiqlite --no-default-features --features sqlite,auto-heal \
  --lib snapshot_sync -- --nocapture
```

The tests use real SQLite `VACUUM INTO`, temporary files and rename operations.
A private sync callback injects an error at each durability barrier without
requiring a faulty disk, privileged mounts, network services or credentials.
Production uses `File::sync_data` through the same function.

- A snapshot-file sync failure must return an error before rename. The previous
  snapshot must remain byte-for-byte unchanged.
- A parent-directory sync failure must return an error after rename. The previous
  snapshot must remain byte-for-byte unchanged. A renamed file is not evidence
  that directory publication was durable.
- A successful snapshot must cross both barriers and contain the original SQLite
  rows, with no temporary file left behind.

To reproduce the old behavior, remove the `?` from the two `sync(...)` calls in
`hiqlite/src/store/state_machine/sqlite/writer.rs` and rerun the command. Both
failure tests fail because the function reports success. The success test passes.

These tests verify error propagation and publication ordering. They do not
simulate physical power loss or qualify the full Raft storage implementation.
