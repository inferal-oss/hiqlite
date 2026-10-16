# WAL durability audit and regression tests

This audit targets `hiqlite-wal` 0.15.1 and the Unix `LogSync::Immediate` path.
It covers acknowledgment ordering, metadata publication, segment creation,
prefix purge, suffix truncation and recovery from retained local files.

Run the focused deterministic regressions from the repository root:

```sh
cargo test -p hiqlite-wal --features auto-heal --lib wal_durability -- --nocapture
```

Run the complete WAL suite, including optional oversized-entry handling:

```sh
cargo test -p hiqlite-wal --all-features --lib
```

The ignored `wal_durability_crash_helper` is not a skipped assertion. The process
crash test launches it explicitly as a subprocess, waits for acknowledged
operations, kills it before shutdown or Drop can flush, then reopens the files.

## Findings and intended behavior

- Metadata replacement ignores directory sync errors. Return the error before
  vote/purge success.
- New segments sync file data without their directory entry. Sync the file and
  parent before using the segment.
- Truncation changes only in-memory bounds; deep recovery restores removed
  records. Clear the suffix and persist data/header before returning.
- A partial purge failure rolls the frontier back into missing history. Keep
  the durable frontier and stop the writer.
- Startup silently omits existing unreadable segments. Fail closed without
  omitting or replacing history.
- Legacy repair guesses a purged predecessor's term from a later entry. Require
  explicit recovery instead of inventing history.
- An early rollover I/O error drops the append's completion callback. Complete
  the callback with an error and stop.

Metadata publication must finish before destructive purge work begins. A purge
may retain extra obsolete files after failure, but it must not claim that
already deleted history is still available. An I/O failure during vote, purge,
truncate or append is not a reason to continue using the same writer state.

The tests inject errors through private, thread-local filesystem wrappers. The
hooks compile out of production builds and require no credentials, privileged
mounts, external services or global environment mutations. Tests also cover
successful publication, replacement with a shorter entry, failure before purge
deletion, callback completion and new directory hierarchy publication.

## Storage contract and limits

Only `Immediate` is in scope for durable append acknowledgment. The other sync
modes intentionally retain a writeback window. In Linux, blocking mapped flush
uses `MS_SYNC`; the kernel routes it through `vfs_fsync_range(..., datasync=1)`.
This is separate from persisting new or removed directory entries.
See the [Linux implementation](https://github.com/torvalds/linux/blob/v6.12/mm/msync.c)
and [filesystem sync documentation](https://www.man7.org/linux/man-pages/man2/fsync.2.html).

The strict directory barrier is implemented on Unix. Other platforms return an
unsupported-operation error rather than silently claim durable publication.
This audit's runtime qualification is Linux. Existing damaged headers or missing
purge metadata now require explicit recovery; startup must not turn them into a
fresh or shorter history. `auto-heal` still has its existing record-level checks.

Clearing the truncated mapped suffix writes zeros through the remaining segment.
That is a correctness-first choice and can add I/O for large sparse segments.
Performance optimization must retain the same crash/recovery contract.

These tests demonstrate software ordering and error handling, plus process-crash
recovery. They do not discard the host kernel's dirty pages, emulate real power
loss, qualify a filesystem/device cache, or prove Raft under arbitrary storage
corruption. Whole-volume loss and stale-volume replacement still require an
application membership/identity protocol. A failed writer or Raft core requires
readiness and supervision that detect the failure even if its process stays up.
