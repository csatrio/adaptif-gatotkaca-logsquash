//! Lock helpers (DESIGN.md decision 131): a lock poisoned by a panic becomes an error instead of another panic.
//!
//! A panic while a lock is held (a bug, or running out of memory) can leave the state behind it half-changed. Calls
//! that return a [`Result`] then fail with [`Error::Corrupt`] instead of panicking in turn, so a server keeps running
//! and can reopen the store. Calls without a `Result` (counters such as `len`), caches, and switch-overs made of whole
//! assignments that a panic cannot leave half-done take the guard anyway.

use crate::error::{Error, Result};
use std::sync::{LockResult, PoisonError};

/// The reason of the error for a poisoned lock.
pub(crate) const POISONED: &str = "a call panicked while it held a lock of this store; open the store again";

pub(crate) trait Unpoison<G> {
    /// The guard, or [`Error::Corrupt`] if a panic poisoned the lock.
    fn or_poisoned(self) -> Result<G>;

    /// The guard, poisoned or not.
    fn ignore_poison(self) -> G;
}

impl<G> Unpoison<G> for LockResult<G> {
    fn or_poisoned(self) -> Result<G> {
        self.map_err(|_| Error::Corrupt(POISONED))
    }

    fn ignore_poison(self) -> G {
        self.unwrap_or_else(PoisonError::into_inner)
    }
}
