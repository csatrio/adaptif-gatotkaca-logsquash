//! Exclusive advisory file locks, one holder per file in the whole system (DESIGN.md decision 122).
//!
//! - **Unix:** POSIX record locks (`fcntl(F_SETLK)` on the whole file). They belong to the process, so a child process
//!   never holds a copy of one (a `flock` lock is shared with every child forked while it is held, until the child
//!   executes), and they are released when the process ends, whatever its children do.
//! - **Windows (and other systems):** `LockFileEx`, released with `UnlockFile` before the handle is closed.
//!
//! POSIX locks have two traps, both handled by a table of the locks this process holds, keyed by file identity:
//!
//! - Two locks of one process never conflict: a second lock of a file this process holds would succeed. The table
//!   answers [`Error::Locked`] for it instead.
//! - Closing *any* handle of a file releases every lock of the process on it. The table keeps the only handle the crate
//!   opens on a locked file, and a file in the table is never opened again: read and write its content through
//!   [`FileLock::file`]. Code outside the crate that opens and closes a lock file releases the lock (do not touch the
//!   files of a store by hand).
//!
//! A lock file is never deleted by the code that may race for it, except [`FileLock::remove`], which unlinks it while
//! it is still held (used for files nobody locks again, see its documentation).

use crate::error::{Error, Result};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, PoisonError};

/// The identity of a file: device and inode on Unix, the canonical path elsewhere.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum Key {
    #[cfg(unix)]
    Inode(u64, u64),
    #[cfg(not(unix))]
    Path(PathBuf),
}

/// The locks held by this process, with the handle each one was taken through.
static HELD: Mutex<Option<HashMap<Key, Arc<File>>>> = Mutex::new(None);

fn held() -> std::sync::MutexGuard<'static, Option<HashMap<Key, Arc<File>>>> {
    // the table stays consistent when a holder panics: every change to it is a single insert or remove
    HELD.lock().unwrap_or_else(PoisonError::into_inner)
}

#[cfg(unix)]
fn key_of_path(path: &Path) -> std::io::Result<Key> {
    use std::os::unix::fs::MetadataExt;
    let meta = std::fs::metadata(path)?;
    Ok(Key::Inode(meta.dev(), meta.ino()))
}

#[cfg(unix)]
fn key_of_file(file: &File, _path: &Path) -> std::io::Result<Key> {
    use std::os::unix::fs::MetadataExt;
    let meta = file.metadata()?;
    Ok(Key::Inode(meta.dev(), meta.ino()))
}

#[cfg(not(unix))]
fn key_of_path(path: &Path) -> std::io::Result<Key> {
    Ok(Key::Path(std::fs::canonicalize(path)?))
}

#[cfg(not(unix))]
fn key_of_file(_file: &File, path: &Path) -> std::io::Result<Key> {
    key_of_path(path)
}

/// Takes the lock without waiting: `Ok(true)` if taken, `Ok(false)` if another process holds it.
#[cfg(unix)]
fn try_lock_file(file: &File) -> std::io::Result<bool> {
    use rustix::fs::{FlockOperation, fcntl_lock};
    match fcntl_lock(file, FlockOperation::NonBlockingLockExclusive) {
        Ok(()) => Ok(true),
        // POSIX allows either for a lock held elsewhere
        Err(e) if e == rustix::io::Errno::AGAIN || e == rustix::io::Errno::ACCESS => Ok(false),
        Err(e) => Err(e.into()),
    }
}

#[cfg(unix)]
fn unlock_file(file: &File) {
    let _ = rustix::fs::fcntl_lock(file, rustix::fs::FlockOperation::NonBlockingUnlock);
}

#[cfg(not(unix))]
fn try_lock_file(file: &File) -> std::io::Result<bool> {
    match fs4::FileExt::try_lock(file) {
        Ok(()) => Ok(true),
        Err(fs4::TryLockError::WouldBlock) => Ok(false),
        Err(fs4::TryLockError::Error(e)) => Err(e),
    }
}

#[cfg(not(unix))]
fn unlock_file(file: &File) {
    // explicit: Windows releases the lock of a closed handle only after a while
    let _ = fs4::FileExt::unlock(file);
}

/// An exclusive lock on a file, held until dropped.
pub(crate) struct FileLock {
    key: Key,
    file: Arc<File>,
    path: PathBuf,
}

impl FileLock {
    /// Locks `path` (created if missing) without waiting. Fails with [`Error::Locked`] if any process, this one
    /// included, holds it.
    pub(crate) fn try_lock(path: &Path) -> Result<Self> {
        let mut table = held();
        let table = table.get_or_insert_with(HashMap::new);
        // Never open a file this process has locked: closing that second handle would release the lock.
        match key_of_path(path) {
            Ok(key) if table.contains_key(&key) => return Err(Error::Locked),
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
        let file = OpenOptions::new().read(true).write(true).create(true).truncate(false).open(path)?;
        let key = key_of_file(&file, path)?;
        if table.contains_key(&key) {
            // Replaced since the check (only possible by hand): keep this handle open, closing it would unlock.
            std::mem::forget(file);
            return Err(Error::Locked);
        }
        if !try_lock_file(&file)? {
            return Err(Error::Locked);
        }
        let file = Arc::new(file);
        table.insert(key.clone(), file.clone());
        Ok(Self { key, file, path: path.to_path_buf() })
    }

    /// The handle the lock was taken through: the only one to read or write the file with.
    pub(crate) fn file(&self) -> &File {
        &self.file
    }

    /// Deletes the file, then releases the lock. Only for a file that nobody locks again by name (another process that
    /// opened it before the deletion would lock the deleted file); errors are ignored (Windows may refuse).
    pub(crate) fn remove(self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

impl Drop for FileLock {
    fn drop(&mut self) {
        let mut table = held();
        if let Some(table) = table.as_mut() {
            table.remove(&self.key);
        }
        unlock_file(&self.file);
        // the handle closes when the last Arc goes, which is this one: the table's copy was just removed
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_second_lock_in_the_same_process_is_refused_and_a_drop_releases() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("lock");
        let first = FileLock::try_lock(&path).unwrap();
        assert!(matches!(FileLock::try_lock(&path), Err(Error::Locked)));
        // the refused attempt did not open and close a handle: the lock is still held
        assert!(matches!(FileLock::try_lock(&path), Err(Error::Locked)));
        drop(first);
        let again = FileLock::try_lock(&path).unwrap();
        drop(again);
    }

    #[test]
    fn the_content_is_read_and_written_through_the_lock() {
        use std::io::{Read, Seek, SeekFrom, Write};
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("lock");
        let lock = FileLock::try_lock(&path).unwrap();
        let mut file = lock.file();
        file.write_all(b"owner").unwrap();
        file.seek(SeekFrom::Start(0)).unwrap();
        let mut text = String::new();
        file.read_to_string(&mut text).unwrap();
        assert_eq!(text, "owner");
        assert!(matches!(FileLock::try_lock(&path), Err(Error::Locked)));
        lock.remove();
        assert!(!path.exists());
        FileLock::try_lock(&path).unwrap();
    }
}
