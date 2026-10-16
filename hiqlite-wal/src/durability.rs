use std::fs::{self, File};
use std::io;
use std::path::Path;

pub(crate) fn sync_file(file: &File, path: &Path) -> io::Result<()> {
    #[cfg(test)]
    test_io::record("sync_file", path)?;
    let _ = path;
    file.sync_data()
}

pub(crate) fn sync_directory(path: &Path) -> io::Result<()> {
    #[cfg(test)]
    test_io::record("sync_directory", path)?;
    #[cfg(unix)]
    {
        File::open(path)?.sync_all()
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "durable directory sync requires Unix",
        ))
    }
}

pub(crate) fn remove_file(path: &Path) -> io::Result<()> {
    #[cfg(test)]
    test_io::record("remove_file", path)?;
    fs::remove_file(path)
}

pub(crate) fn create_dir_all(path: &Path) -> io::Result<()> {
    let mut missing = Vec::new();
    let mut next = Some(path);
    while let Some(directory) = next {
        if directory.as_os_str().is_empty() || directory.try_exists()? {
            break;
        }
        missing.push(directory);
        next = directory.parent();
    }
    fs::create_dir_all(path)?;
    // Persist children before the directory entries naming their parents.
    for directory in &missing {
        sync_directory(directory)?;
    }
    if let Some(parent) = missing.last().and_then(|directory| directory.parent()) {
        sync_directory(if parent.as_os_str().is_empty() {
            Path::new(".")
        } else {
            parent
        })?;
    }
    Ok(())
}

#[cfg(test)]
pub(crate) mod test_io {
    use std::cell::RefCell;
    use std::io;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU64, Ordering};

    #[derive(Default)]
    struct State {
        fail: Option<(&'static str, usize)>,
        hits: usize,
        events: Vec<(&'static str, PathBuf)>,
    }

    thread_local! {
        static STATE: RefCell<State> = RefCell::new(State::default());
    }

    pub(crate) struct FaultGuard;

    impl FaultGuard {
        pub(crate) fn new(operation: &'static str, occurrence: usize) -> Self {
            STATE.with(|state| {
                *state.borrow_mut() = State {
                    fail: Some((operation, occurrence)),
                    ..State::default()
                }
            });
            Self
        }

        pub(crate) fn events(&self) -> Vec<(&'static str, PathBuf)> {
            STATE.with(|state| state.borrow().events.clone())
        }
    }

    impl Drop for FaultGuard {
        fn drop(&mut self) {
            STATE.with(|state| *state.borrow_mut() = State::default());
        }
    }

    pub(crate) fn record(operation: &'static str, path: &Path) -> io::Result<()> {
        STATE.with(|state| {
            let mut state = state.borrow_mut();
            if state.fail.is_none() {
                return Ok(());
            }
            state.events.push((operation, path.to_path_buf()));
            if let Some((target, occurrence)) = state.fail
                && operation == target
            {
                state.hits += 1;
                if state.hits == occurrence {
                    return Err(io::Error::other(format!("injected {operation} failure")));
                }
            }
            Ok(())
        })
    }

    pub(crate) struct TestDir(pub(crate) PathBuf);

    impl TestDir {
        pub(crate) fn new(name: &str) -> Self {
            static NEXT: AtomicU64 = AtomicU64::new(0);
            let path = std::env::temp_dir().join(format!(
                "hiqlite-wal-{name}-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            std::fs::create_dir(&path).unwrap();
            Self(path)
        }

        pub(crate) fn as_str(&self) -> &str {
            self.0.to_str().unwrap()
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_io::{FaultGuard, TestDir};

    #[test]
    fn wal_durability_new_directories_sync_existing_parent() {
        let root = TestDir::new("mkdir");
        let path = root.0.join("new").join("logs");
        let trace = FaultGuard::new("none", 1);
        create_dir_all(&path).unwrap();
        assert_eq!(
            trace.events(),
            vec![
                ("sync_directory", path.clone()),
                ("sync_directory", root.0.join("new")),
                ("sync_directory", root.0.clone()),
            ]
        );
        let failed_path = root.0.join("failed");
        let _fault = FaultGuard::new("sync_directory", 1);
        assert!(create_dir_all(&failed_path).is_err());
    }
}
