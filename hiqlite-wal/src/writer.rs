use crate::error::Error;
use crate::lockfile::LockFile;
use crate::log_store_impl::{deserialize, serialize};
use crate::metadata::Metadata;
use crate::reader::LogReadMemo;
use crate::wal::WalFileSet;
use openraft::{LeaderId, LogId};
use std::fmt::{Debug, Formatter};
use std::sync::{Arc, RwLock};
use std::time::Duration;
use std::{io, thread};
use thread_priority::ThreadPriority;
use tokio::sync::oneshot;
use tokio::time::Interval;
use tokio::{task, time};
use tracing::{debug, error, warn};

pub enum Action {
    Append {
        rx: flume::Receiver<Option<(u64, Vec<u8>)>>,
        callback: Box<dyn FnOnce(Result<(), io::Error>) + Send>,
        ack: oneshot::Sender<Result<(), Error>>,
    },
    Remove {
        from: u64,
        until: u64,
        last_log: Option<Vec<u8>>,
        ack: oneshot::Sender<Result<(), Error>>,
    },
    Vote {
        value: Vec<u8>,
        ack: oneshot::Sender<Result<(), Error>>,
    },
    Sync,
    Shutdown(oneshot::Sender<()>),
}

impl Debug for Action {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Action::Append { .. } => write!(f, "Action::Append"),
            Action::Remove { .. } => write!(f, "Action::Remove"),
            Action::Vote { .. } => write!(f, "Action::Vote"),
            Action::Sync => write!(f, "Action::Sync"),
            Action::Shutdown(_) => write!(f, "Action::Shutdown"),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum LogSync {
    Immediate,
    ImmediateAsync,
    IntervalMillis(u64),
}

impl TryFrom<&str> for LogSync {
    type Error = Error;
    fn try_from(s: &str) -> Result<Self, Self::Error> {
        match s {
            "immediate" => Ok(Self::Immediate),
            "immediate_async" => Ok(Self::ImmediateAsync),
            v => {
                if let Some(ms) = v.strip_prefix("interval_") {
                    let Ok(ms) = ms.parse::<u64>() else {
                        return Err(Error::Generic(
                            format!(
                                "Invalid value for log_sync interval, cannot parse as u64: {v}"
                            )
                            .into(),
                        ));
                    };
                    Ok(Self::IntervalMillis(ms))
                } else {
                    Err(Error::Generic(
                        format!("Cannot parse LogSync - invalid value: {v}").into(),
                    ))
                }
            }
        }
    }
}

#[allow(clippy::type_complexity)]
pub fn spawn(
    base_path: String,
    lockfile: LockFile,
    sync: LogSync,
    wal_size: u32,
    wal_deep_integrity_check: bool,
    meta: Arc<RwLock<Metadata>>,
) -> Result<(flume::Sender<Action>, Arc<RwLock<WalFileSet>>), Error> {
    let mut set = WalFileSet::read(base_path, wal_size)?;
    // TODO emit a warning log in that case and tell the user how to resolve or "force start" in
    // that case, or should be maybe `auto-heal` as much as possible?
    let mut buf = Vec::with_capacity(32);
    set.check_integrity(&mut buf, wal_deep_integrity_check)?;
    if set.files.is_empty() {
        buf.clear();
        set.add_file(wal_size, &mut buf)?;
    }
    let wal_locked = Arc::new(RwLock::new(set.clone_no_map()));

    // TODO remove with version > 0.14
    // This is a fix for a bug from previous versions. Can be removed in later ones,
    // it would be safe to do probably around version >= 0.13.
    if meta.read()?.last_purged_log_id.is_none()
        && let Some(front) = set.files.front_mut()
        && front.wal_no > 1
        && front.id_from > 2
    {
        warn!("Trying to fix bad LogState for `last_purged_logid`");
        let mut buf = Vec::with_capacity(16);
        let mut memo: Option<LogReadMemo> = None;
        front.mmap()?;
        front.read_logs(front.id_from, front.id_until, &mut memo, &mut buf)?;
        let (_, bytes) = buf.first().unwrap();
        let log: openraft::log_id::LogId<u64> = deserialize(bytes)?;
        let log_id: openraft::log_id::LogId<u64> = LogId {
            leader_id: LeaderId {
                term: log.leader_id.term,
                node_id: log.leader_id.node_id,
            },
            index: log.index - 1,
        };
        front.mmap_drop();

        meta.write()?.last_purged_log_id = Some(serialize(&log_id)?);
        Metadata::write(meta.clone(), &set.base_path)?;
    }

    let (tx, rx) = flume::bounded::<Action>(1);
    let wal = wal_locked.clone();
    let snc = sync.clone();
    thread::spawn(move || run(lockfile, meta, wal, set, rx, snc, wal_size));

    if let LogSync::IntervalMillis(millis) = &sync {
        let interval = time::interval(Duration::from_millis(*millis));
        spawn_syncer(tx.clone(), interval);
    }

    Ok((tx, wal_locked))
}

/// Flush the active WAL file so that everything written to it is on disk.
///
/// `flush_async` only starts the writeback and returns, so a WAL flushed that way
/// is not known to be on disk. Only a flush that returns may clear `is_dirty`.
fn flush_blocking(
    wal: &mut WalFileSet,
    buf: &mut Vec<u8>,
    is_dirty: &mut bool,
) -> Result<(), Error> {
    if !*is_dirty {
        return Ok(());
    }

    let active = wal.active();
    buf.clear();
    active.update_header(buf)?;
    active.flush()?;
    *is_dirty = false;

    Ok(())
}

fn spawn_syncer(tx_writer: flume::Sender<Action>, mut interval: Interval) {
    task::spawn(async move {
        loop {
            interval.tick().await;
            if tx_writer.send_async(Action::Sync).await.is_err() {
                debug!("Error sending ActionWrite::Sync to LogStoreWriter - exiting");
                break;
            }
        }
    });
}

/// There are a lot of `unwrap()`s in this task. The reason is simply, if most of these fail, it can
/// only be because of a non-recoverable error anyway and the application should crash, so that
/// the next health check can restart it.
///
/// Everything related to locking and memory mapping is being `unwrap()`ped. If anything fails in
/// this regard, it's either a physical storage or OS issue and this code an do nothing about it.
fn run(
    lockfile: LockFile,
    meta: Arc<RwLock<Metadata>>,
    wal_locked: Arc<RwLock<WalFileSet>>,
    mut wal: WalFileSet,
    rx: flume::Receiver<Action>,
    sync: LogSync,
    wal_size: u32,
) -> Result<(), Error> {
    let _ = ThreadPriority::Max.set_for_current();

    let mut is_dirty = false;
    let mut shutdown_ack: Option<oneshot::Sender<()>> = None;
    let data_len_limit = wal_size as usize - wal.active().offset_logs() - 2;

    // openraft will read chunks of 64 logs for bigger tasks
    let mut buf: Vec<u8> = Vec::with_capacity(64);
    let mut buf_logs: Vec<(u64, Vec<u8>)> = Vec::with_capacity(1);

    wal.active().mmap_mut()?;

    while let Ok(action) = rx.recv() {
        match action {
            Action::Append { rx, callback, ack } => {
                debug!("WAL Writer - Action::Append");

                let mut res = Ok(());
                {
                    let mut active = wal.active();

                    // There is no need to specifically catch a possibly closed channel and respond
                    // with an error. If that channel is being closed mid-batch append, it means
                    // openraft (and all other tasks) when down, and it dropped the whole log store.
                    // If that ever happens, there is no one listening on the other end of your ack
                    // and callback, which means no one would ever even get our result.
                    while let Ok(Some((id, bytes))) = rx.recv() {
                        if bytes.len() > data_len_limit {
                            // A single raft entry cannot span WAL files. By default an
                            // oversized entry is a non-recoverable setup issue (it needs a
                            // config change with a full restart, or code changes), so the
                            // writer panics. With the `oversized-entry-error` feature the
                            // append instead fails with `Error::WalSizeExceeded` and the
                            // writer keeps serving subsequent requests. With the default
                            // `wal_size` of 2MB this is easily reached by a single large
                            // INSERT, transaction or batch.
                            #[cfg(feature = "oversized-entry-error")]
                            {
                                res = Err(Error::WalSizeExceeded(
                                    format!(
                                        "`data` length must not exceed `wal_size` -> data \
                                        length is {} vs wal_size (without header) is \
                                        {data_len_limit}",
                                        bytes.len(),
                                    )
                                    .into(),
                                ));
                                break;
                            }
                            #[cfg(not(feature = "oversized-entry-error"))]
                            {
                                panic!(
                                    "`data` length must not exceed `wal_size` -> data length \
                                    is {} vs wal_size (without header) is {data_len_limit}",
                                    bytes.len(),
                                );
                            }
                        }

                        if !active.has_space(bytes.len() as u32) {
                            buf.clear();
                            wal.roll_over(wal_size, &mut buf)?;
                            {
                                let mut lock = wal_locked.write().unwrap();
                                lock.active = wal.active;
                                lock.clone_files_from_no_mmap(&wal.files);
                            }
                            active = wal.active();
                        }

                        buf.clear();
                        if let Err(err) = active.append_log(id, &bytes, &mut buf) {
                            res = Err(err);
                            break;
                        }
                        debug_assert_eq!(
                            active.id_until, id,
                            "active.id_until and id don't match: {} != {id}",
                            active.id_until
                        );
                    }
                }

                {
                    let mut lock = wal_locked.write().unwrap();
                    debug_assert_eq!(lock.active, wal.active);
                    lock.active().clone_from_no_mmap(wal.active());
                }

                let mut res_cb = match &res {
                    Ok(_) => Ok(()),
                    Err(err) => Err(io::Error::other(err.to_string())),
                };
                // We want to ack before flush for throughput. If anything goes wrong during flush,
                // the openraft callback will get notified about it.
                if let Err(err) = ack.send(res) {
                    // this should usually not happen, but it may during an incorrect shutdown
                    error!("error sending back ack after logs append: {err:?}");
                }

                // The WAL now holds bytes that are not known to be on disk, and only a
                // blocking flush can clear that state again. `flush_async` merely starts the
                // writeback, which is why `Action::Remove` and `Action::Vote` below still flush.
                is_dirty = true;
                match &sync {
                    LogSync::Immediate => {
                        if let Err(err) = flush_blocking(&mut wal, &mut buf, &mut is_dirty) {
                            // If an immediate flush fails, we want to stop for data consistency.
                            error!(
                                "Error during immediate WAL file sync. Stopping Writer to preserve \
                                data consistency. {err:?}"
                            );
                            callback(Err(io::Error::other(err.to_string())));
                            break;
                        }
                    }
                    LogSync::ImmediateAsync => {
                        if let Err(err) = wal.active().flush_async() {
                            res_cb = Err(io::Error::other(err.to_string()));
                        }
                    }
                    LogSync::IntervalMillis(_) => {
                        // flushes from ticker task
                    }
                }

                // openraft takes this callback as "these entries are on disk" and commits on
                // a quorum of such acks. Only `Immediate` upholds that here: the async levels
                // deliberately ack first and trade the writeback window for throughput.
                // TODO check openraft how we should behave in this scenario. I guess, as
                //  long as we don't callback(), the Raft will fully block. -> verify + double-check
                //  Do we need to callback() when this batch is processed apart from the result, or
                //  only ever if it fully succeeded?
                callback(res_cb);

                // Roll WAL pre-emptively if only very few space is left at this point, because
                // if we just wrote some chunks, me probably have a very short break now until the
                // next request comes in.
                //
                // TODO fixed 4kB -> make configurable?
                if wal.active().space_left() < 4 * 1024 {
                    buf.clear();
                    wal.roll_over(wal_size, &mut buf)?;
                    {
                        let mut lock = wal_locked.write().unwrap();
                        lock.active = wal.active;
                        lock.clone_files_from_no_mmap(&wal.files);
                    }
                }
            }
            Action::Remove {
                from,
                until,
                last_log,
                ack,
            } => {
                debug!(
                    "WAL Writer - Action::Remove from {from} until {until} / \
                    last_log: {last_log:?}\n{wal:?}"
                );

                // Before removing any logs, make sure that all in-memory buffers are flushed. If
                // at least headers and metadata are not up to date, and a crash happens in the
                // middle of removing logs, we could end up with a hole between Snapshot and latest
                // existing Raft Log, which must never happen.
                //
                // The flush has to block. `flush_async` only starts the writeback and returns,
                // so a crash during the removal below can still land after the deletions and
                // before the header reaches disk, which is the hole this guards against.
                if let Err(err) = flush_blocking(&mut wal, &mut buf, &mut is_dirty) {
                    ack.send(Err(err)).unwrap();
                    continue;
                }

                // Persist the purge frontier before deleting (too low = hole into deleted files;
                // too high = extra files). Revert it again if the deletion fails below.
                let previous_purged = meta.read()?.last_purged_log_id.clone();
                let persist_purged = last_log.is_some();
                if persist_purged {
                    meta.write()?.last_purged_log_id = last_log;
                    Metadata::write(meta.clone(), &wal.base_path)?;
                }

                buf.clear();
                buf_logs.clear();
                match wal.shift_delete_logs(from, until, wal_size, &mut buf, &mut buf_logs) {
                    Ok(_) => {
                        {
                            let mut lock = wal_locked.write().unwrap();
                            lock.active = wal.active;
                            lock.clone_files_from_no_mmap(&wal.files);
                        }
                        ack.send(Ok(())).unwrap();
                    }
                    Err(err) => {
                        if persist_purged {
                            // deletion failed: revert the frontier, but still report the error
                            let revert_res = {
                                // `Metadata::write` re-locks the meta, so release the guard first
                                let mut m = meta
                                    .write()
                                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                                m.last_purged_log_id = previous_purged;
                                drop(m);
                                Metadata::write(meta.clone(), &wal.base_path)
                            };
                            if let Err(revert_err) = revert_res {
                                error!(
                                    "Failed to revert last_purged_log_id after log deletion failure: {revert_err}"
                                );
                            }
                        }
                        ack.send(Err(err)).unwrap();
                    }
                }
            }
            Action::Vote { value, ack } => {
                debug!("WAL Writer - Action::Vote");

                // Blocking on purpose: clearing `is_dirty` while an msync is still in flight
                // would make the interval ticker skip a WAL that never reached disk.
                flush_blocking(&mut wal, &mut buf, &mut is_dirty)?;

                meta.write()?.vote = Some(value);
                let res = Metadata::write(meta.clone(), &wal.base_path);

                ack.send(res).unwrap();
            }
            Action::Sync => {
                // The ticker is the only flush in `IntervalMillis` mode and no append waits on
                // it, so it blocks: `msync(MS_ASYNC)` starts no writeback on Linux at all.
                flush_blocking(&mut wal, &mut buf, &mut is_dirty)?;
            }
            Action::Shutdown(ack) => {
                debug!("Raft logs store writer is being shut down");
                shutdown_ack = Some(ack);
                break;
            }
        }
    }

    debug!("Logs Writer exiting");

    let active = wal.active();
    buf.clear();
    active.update_header(&mut buf)?;
    active.flush()?;
    Metadata::write(meta, &wal.base_path)?;

    // drop the lockfile before trying to remove it to unlock it
    drop(lockfile);
    LockFile::remove(&wal.base_path).expect("LockFile removal failed");

    if let Some(ack) = shutdown_ack {
        ack.send(())
            .expect("Shutdown handler to always wait for ack from logs");
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::durability::test_io::{FaultGuard, TestDir};

    fn durability_writer_fixture(base: &str) -> (LockFile, Arc<RwLock<Metadata>>, WalFileSet) {
        let lock = LockFile::create(base).unwrap();
        lock.lock().unwrap();
        let meta = Arc::new(RwLock::new(Metadata::read_or_create(base).unwrap()));
        let set = WalFileSet::read(base.to_string(), 8192).unwrap();
        (lock, meta, set)
    }

    #[test]
    fn wal_durability_rollover_failure_completes_callback() {
        let dir = TestDir::new("rollover-callback");
        let (lock, meta, mut set) = durability_writer_fixture(dir.as_str());
        set.active().mmap_mut().unwrap();
        set.active()
            .append_log(1, &vec![1; 6000], &mut Vec::new())
            .unwrap();
        let locked = Arc::new(RwLock::new(set.clone_no_map()));
        let (tx, rx) = flume::bounded(1);
        let worker = std::thread::spawn(move || {
            let _fault = FaultGuard::new("sync_directory", 1);
            run(lock, meta, locked, set, rx, LogSync::Immediate, 8192)
        });
        let (entries, input) = flume::bounded(2);
        let (ack, response) = oneshot::channel();
        let (callback, result) = std::sync::mpsc::channel();
        tx.send(Action::Append {
            rx: input,
            ack,
            callback: Box::new(move |res| {
                callback.send(res.is_err()).unwrap();
            }),
        })
        .unwrap();
        entries.send(Some((2, vec![2; 3000]))).unwrap();
        entries.send(None).unwrap();
        let acknowledged = response.blocking_recv();
        let callback_failed = result.recv_timeout(Duration::from_secs(5));
        drop(tx);
        let stopped = worker.join().unwrap();
        assert!(matches!(acknowledged, Ok(Err(_))));
        assert!(
            callback_failed.unwrap(),
            "failed rollover must complete LogFlushed with an error"
        );
        assert!(stopped.is_err());
    }

    #[test]
    fn wal_durability_vote_error_stops_writer() {
        let dir = TestDir::new("vote-error");
        let (lock, meta, set) = durability_writer_fixture(dir.as_str());
        let locked = Arc::new(RwLock::new(set.clone_no_map()));
        let (tx, rx) = flume::bounded(1);
        let worker = std::thread::spawn(move || {
            let _fault = FaultGuard::new("sync_directory", 1);
            run(lock, meta, locked, set, rx, LogSync::Immediate, 8192)
        });
        let (ack, response) = oneshot::channel();
        tx.send(Action::Vote {
            value: vec![7],
            ack,
        })
        .unwrap();
        let acknowledged = response.blocking_recv();
        drop(tx);
        assert!(worker.join().unwrap().is_err());
        assert!(matches!(acknowledged, Ok(Err(_))));
    }

    #[test]
    fn wal_durability_purge_metadata_error_prevents_deletion() {
        let dir = TestDir::new("purge-metadata-error");
        let (lock, meta, mut set) = durability_writer_fixture(dir.as_str());
        set.active().mmap_mut().unwrap();
        for id in 1..=3 {
            set.active()
                .append_log(id, b"entry", &mut Vec::new())
                .unwrap();
            if id != 3 {
                set.roll_over(8192, &mut Vec::new()).unwrap();
            }
        }
        let locked = Arc::new(RwLock::new(set.clone_no_map()));
        let (tx, rx) = flume::bounded(1);
        let worker = std::thread::spawn(move || {
            let fault = FaultGuard::new("sync_directory", 1);
            let result = run(lock, meta, locked, set, rx, LogSync::Immediate, 8192);
            (result, fault.events())
        });
        let (ack, response) = oneshot::channel();
        tx.send(Action::Remove {
            from: 0,
            until: 3,
            last_log: Some(vec![3]),
            ack,
        })
        .unwrap();
        let acknowledged = response.blocking_recv();
        drop(tx);
        let (result, events) = worker.join().unwrap();
        assert!(matches!(acknowledged, Ok(Err(_))));
        assert!(result.is_err());
        assert!(!events.iter().any(|(op, _)| *op == "remove_file"));
    }

    #[test]
    #[ignore = "subprocess helper for wal_durability_process_crash_roundtrip"]
    fn wal_durability_crash_helper() {
        use std::io::Write;
        let base = std::env::var("HQL_WAL_DURABILITY_TEST_PATH").unwrap();
        let (lock, meta, set) = durability_writer_fixture(&base);
        let locked = Arc::new(RwLock::new(set.clone_no_map()));
        let (tx, rx) = flume::bounded(1);
        std::thread::spawn(move || run(lock, meta, locked, set, rx, LogSync::Immediate, 8192));
        let append = |id, bytes: Vec<u8>| {
            let (entries, input) = flume::bounded(2);
            let (ack, response) = oneshot::channel();
            let (flushed, ready) = std::sync::mpsc::channel();
            tx.send(Action::Append {
                rx: input,
                ack,
                callback: Box::new(move |res| {
                    flushed.send(res.is_ok()).unwrap();
                }),
            })
            .unwrap();
            entries.send(Some((id, bytes))).unwrap();
            entries.send(None).unwrap();
            response.blocking_recv().unwrap().unwrap();
            assert!(ready.recv_timeout(Duration::from_secs(5)).unwrap());
        };
        for id in 1..=6 {
            append(id, vec![id as u8; 1500]);
        }
        let (ack, response) = oneshot::channel();
        tx.send(Action::Remove {
            from: 5,
            until: u64::MAX,
            last_log: None,
            ack,
        })
        .unwrap();
        response.blocking_recv().unwrap().unwrap();
        append(5, b"replacement".to_vec());
        let (ack, response) = oneshot::channel();
        tx.send(Action::Remove {
            from: 0,
            until: 3,
            last_log: Some(vec![3]),
            ack,
        })
        .unwrap();
        response.blocking_recv().unwrap().unwrap();
        let (ack, response) = oneshot::channel();
        tx.send(Action::Vote {
            value: vec![7],
            ack,
        })
        .unwrap();
        response.blocking_recv().unwrap().unwrap();
        println!("WAL_DURABILITY_READY");
        std::io::stdout().flush().unwrap();
        loop {
            std::thread::park();
        }
    }

    #[test]
    fn wal_durability_process_crash_roundtrip() {
        use std::io::{BufRead, BufReader};
        use std::process::{Command, Stdio};
        let dir = TestDir::new("process-crash");
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args([
                "--ignored",
                "--exact",
                "writer::tests::wal_durability_crash_helper",
                "--nocapture",
            ])
            .env("HQL_WAL_DURABILITY_TEST_PATH", dir.as_str())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap();
        let stdout = child.stdout.take().unwrap();
        let (ready, received) = std::sync::mpsc::channel();
        let reader = std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                if line.unwrap().contains("WAL_DURABILITY_READY") {
                    let _ = ready.send(());
                    return;
                }
            }
        });
        let result = received.recv_timeout(Duration::from_secs(20));
        // kill before Drop/shutdown can flush or repair anything for the test.
        let _ = child.kill();
        child.wait().unwrap();
        reader.join().unwrap();
        result.expect("child must reach the acknowledged crash boundary");
        let metadata = Metadata::read_or_create(dir.as_str()).unwrap();
        assert_eq!(metadata.vote, Some(vec![7]));
        assert_eq!(metadata.last_purged_log_id, Some(vec![3]));
        let mut set = WalFileSet::read(dir.as_str().to_string(), 8192).unwrap();
        set.check_integrity(&mut Vec::new(), true).unwrap();
        let mut entries = Vec::new();
        for file in &mut set.files {
            if file.data_start.is_none() || file.id_until < 4 {
                continue;
            }
            file.mmap().unwrap();
            file.read_logs(file.id_from.max(4), file.id_until, &mut None, &mut entries)
                .unwrap();
        }
        assert_eq!(
            entries,
            vec![(4, vec![4; 1500]), (5, b"replacement".to_vec())]
        );
    }

    #[test]
    fn wal_durability_partial_purge_keeps_durable_frontier() {
        let dir = TestDir::new("partial-purge");
        let base = dir.as_str().to_string();
        let meta = Arc::new(RwLock::new(Metadata::read_or_create(&base).unwrap()));
        let lock = LockFile::create(&base).unwrap();
        lock.lock().unwrap();
        let mut set = WalFileSet::read(base.clone(), 8192).unwrap();
        set.active().mmap_mut().unwrap();
        for id in 1..=3 {
            set.active()
                .append_log(id, b"entry", &mut Vec::new())
                .unwrap();
            if id != 3 {
                set.roll_over(8192, &mut Vec::new()).unwrap();
            }
        }
        let locked = Arc::new(RwLock::new(set.clone_no_map()));
        let (tx, rx) = flume::bounded(1);
        let worker_meta = meta.clone();
        let worker = std::thread::spawn(move || {
            let _fault = FaultGuard::new("remove_file", 2);
            run(lock, worker_meta, locked, set, rx, LogSync::Immediate, 8192)
        });
        let frontier = serialize(&LogId {
            leader_id: LeaderId::new(1, 1),
            index: 3,
        })
        .unwrap();
        let (ack, response) = oneshot::channel();
        tx.send(Action::Remove {
            from: 0,
            until: 3,
            last_log: Some(frontier.clone()),
            ack,
        })
        .unwrap();
        assert!(response.blocking_recv().unwrap().is_err());
        drop(tx);
        let result = worker.join();
        assert_eq!(
            Metadata::read_or_create(&base).unwrap().last_purged_log_id,
            Some(frontier),
            "never roll the frontier back into already deleted files"
        );
        assert!(
            matches!(result, Ok(Err(_))),
            "stop after a partial destructive I/O failure"
        );
    }

    #[test]
    fn wal_durability_missing_frontier_is_not_invented() {
        let dir = TestDir::new("missing-frontier");
        let base = dir.as_str().to_string();
        let meta = Arc::new(RwLock::new(Metadata::read_or_create(&base).unwrap()));
        let mut file = crate::wal::WalFile::new(2, &base, 3, 3, 8192).unwrap();
        file.create_file(&mut Vec::new()).unwrap();
        file.mmap_mut().unwrap();
        let log = serialize(&LogId::<u64> {
            leader_id: LeaderId::new(3, 1),
            index: 3,
        })
        .unwrap();
        file.append_log(3, &log, &mut Vec::new()).unwrap();
        drop(file);
        let lock = LockFile::create(&base).unwrap();
        lock.lock().unwrap();
        let outcome = spawn(
            base.clone(),
            lock,
            LogSync::Immediate,
            8192,
            false,
            meta.clone(),
        );
        let failed = outcome.is_err();
        if let Ok((tx, _)) = outcome {
            let (ack, response) = oneshot::channel();
            tx.send(Action::Shutdown(ack)).unwrap();
            response.blocking_recv().unwrap();
        }
        assert!(
            failed,
            "do not guess the missing purge frontier from a later term"
        );
        assert!(
            Metadata::read_or_create(&base)
                .unwrap()
                .last_purged_log_id
                .is_none()
        );
    }

    #[cfg(feature = "oversized-entry-error")]
    fn append(tx: &flume::Sender<Action>, id: u64, bytes: Vec<u8>) -> Result<(), Error> {
        let (ack_tx, ack_rx) = oneshot::channel();
        let (entry_tx, entry_rx) = flume::bounded(1);
        tx.send(Action::Append {
            rx: entry_rx,
            callback: Box::new(|_| {}),
            ack: ack_tx,
        })
        .unwrap();
        entry_tx.send(Some((id, bytes))).unwrap();
        entry_tx.send(None).unwrap();
        ack_rx.blocking_recv().unwrap()
    }

    #[cfg(feature = "oversized-entry-error")]
    #[test]
    fn oversized_entry_errors_without_killing_writer() {
        let base = "test_data/oversized_entry".to_string();
        let _ = std::fs::remove_dir_all(&base);
        std::fs::create_dir_all(&base).unwrap();

        let lockfile = LockFile::create(&base).unwrap();
        lockfile.lock().unwrap();
        let meta = Arc::new(RwLock::new(Metadata::read_or_create(&base).unwrap()));

        let (tx, _wal) = spawn(
            base.clone(),
            lockfile,
            LogSync::Immediate,
            64 * 1024,
            false,
            meta.clone(),
        )
        .unwrap();

        // an entry larger than the whole WAL file must error, not panic the writer
        let res = append(&tx, 1, vec![0u8; 70 * 1024]);
        assert!(res.is_err(), "oversized entry must error, got {res:?}");

        // the writer must still be alive and accept a normal append afterwards
        let res = append(&tx, 2, b"ok".to_vec());
        assert!(res.is_ok(), "writer must survive: {res:?}");

        // graceful shutdown (removes the lockfile)
        let (ack_tx, ack_rx) = oneshot::channel();
        tx.send(Action::Shutdown(ack_tx)).unwrap();
        ack_rx.blocking_recv().unwrap();
    }

    #[cfg(not(feature = "oversized-entry-error"))]
    #[test]
    fn oversized_entry_panics_and_kills_writer() {
        let base = "test_data/oversized_entry_panic".to_string();
        let _ = std::fs::remove_dir_all(&base);
        std::fs::create_dir_all(&base).unwrap();

        let lockfile = LockFile::create(&base).unwrap();
        lockfile.lock().unwrap();
        let meta = Arc::new(RwLock::new(Metadata::read_or_create(&base).unwrap()));

        let (tx, _wal) = spawn(
            base.clone(),
            lockfile,
            LogSync::Immediate,
            64 * 1024,
            false,
            meta.clone(),
        )
        .unwrap();

        let send_append = |id: u64, bytes: Vec<u8>| {
            let (ack_tx, ack_rx) = oneshot::channel();
            let (entry_tx, entry_rx) = flume::bounded(1);
            // The writer thread may already be dead, so every send can fail and is ignored;
            // the assertion is that the append is never acked.
            let _ = tx.send(Action::Append {
                rx: entry_rx,
                callback: Box::new(|_| ()),
                ack: ack_tx,
            });
            let _ = entry_tx.send(Some((id, bytes)));
            let _ = entry_tx.send(None);
            ack_rx
        };

        // an oversized entry panics the writer thread by default; the append is never acked
        let mut ack = send_append(1, vec![0u8; 70 * 1024]);
        assert!(
            !recv_with_timeout(&mut ack),
            "oversized entry must panic the writer (no ack)"
        );

        // the writer thread is dead: a subsequent append is never acked either
        let mut ack = send_append(2, b"ok".to_vec());
        assert!(
            !recv_with_timeout(&mut ack),
            "writer must be dead after the panic"
        );
    }

    #[cfg(not(feature = "oversized-entry-error"))]
    fn recv_with_timeout(ack: &mut tokio::sync::oneshot::Receiver<Result<(), Error>>) -> bool {
        for _ in 0..10 {
            if ack.try_recv().is_ok() {
                return true;
            }
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
        false
    }
}
