//! This module contains lock guards for use in the lifecycle traits
//!
//! Specifically they exist to work around a lack of support for generic associated
//! types within traits. https://github.com/rust-lang/rust/issues/44265
//!
//! ```ignore
//! trait MyTrait {
//!     type Guard;
//!
//!     fn read(&self) -> Self::Guard<'_> <-- this is not valid rust
//! }
//! ```
//!
//! The structs in this module therefore provide concrete types that can be
//! used in their place
//!
//! ```
//! use lifecycle::LifecycleReadGuard;
//! trait MyTrait {
//!     type AdditionalData;
//!     type LockedType;
//!
//!     fn read(&self) -> LifecycleReadGuard<'_, Self::LockedType, Self::AdditionalData>;
//! }
//! ```
//!
//! One drawback of this approach is that the type returned from the read() method can't
//! be a user-provided type that implements a trait
//!
//! ```ignore
//! trait MyTrait {
//!     type Guard: GuardTrait; <-- this makes for a nice API
//!
//!     fn read(&self) -> Self::Guard<'_> <-- this is not valid rust
//! }
//! ```
//!
//! Instead we have to use associated functions, which are a bit more cumbersome
//!
//! ```
//! use lifecycle::LifecycleReadGuard;
//! trait MyTrait {
//!     type AdditionalData;
//!     type LockedType;
//!
//!     fn read(&self) -> LifecycleReadGuard<'_, Self::LockedType, Self::AdditionalData>;
//!
//!     fn guard_func(s: LifecycleReadGuard<'_, Self::LockedType, Self::AdditionalData>);
//! }
//!
//! fn test<T: MyTrait>(t: T) {
//!     let read = t.read();
//!     T::guard_func(read);
//! }
//! ```
//!

use std::ops::{Deref, DerefMut};

use tracker::{RwLock, RwLockUpgradableReadGuard, RwLockWriteGuard};

/// The `LifecycleReadGuard` combines a `RwLockUpgradableReadGuard` with an arbitrary
/// data payload of type `D`.
///
/// The data of `P` can be immutably accessed through `std::ops::Deref` akin
/// to a normal read guard or smart pointer
///
/// Note: The `LifecycleReadGuard` will not block other readers to `RwLock<P>` but
/// they will block other upgradeable readers, e.g. other `LifecycleReadGuard`
pub struct LifecycleReadGuard<'a, P, D> {
    data: D,
    guard: RwLockUpgradableReadGuard<'a, P>,
}

impl<'a, P, D> LifecycleReadGuard<'a, P, D> {
    /// Create a new `LifecycleReadGuard` from the provided lock
    pub fn new(data: D, lock: &'a RwLock<P>) -> Self {
        let guard = lock.upgradable_read();
        Self { data, guard }
    }

    /// Upgrades this to a `LifecycleWriteGuard`
    pub fn upgrade(self) -> LifecycleWriteGuard<'a, P, D> {
        let guard = RwLockUpgradableReadGuard::upgrade(self.guard);
        LifecycleWriteGuard {
            data: self.data,
            guard,
        }
    }

    /// Returns a reference to the contained data payload
    pub fn data(&self) -> &D {
        &self.data
    }

    /// Drops the locks held by this guard and returns the data payload
    pub fn unwrap(self) -> D {
        self.data
    }
}

impl<'a, P, D> Deref for LifecycleReadGuard<'a, P, D> {
    type Target = P;
    #[inline]
    fn deref(&self) -> &P {
        &self.guard
    }
}

/// A `LifecycleWriteGuard` combines a `RwLockWriteGuard` with an arbitrary
/// data payload of type `D`.
///
/// The data of `P` can be immutably accessed through `std::ops::Deref` akin to
/// a normal read guard or smart pointer, and also mutably through
/// `std::ops::DerefMut` akin to a normal write guard
///
pub struct LifecycleWriteGuard<'a, P, D> {
    data: D,
    guard: RwLockWriteGuard<'a, P>,
}

impl<'a, P, D> LifecycleWriteGuard<'a, P, D> {
    /// Create a new `LifecycleWriteGuard` from the provided lock
    pub fn new(data: D, lock: &'a RwLock<P>) -> Self {
        let guard = lock.write();
        Self { data, guard }
    }

    /// Returns a reference to the contained data payload
    pub fn data(&self) -> &D {
        &self.data
    }

    /// Drops the locks held by this guard and returns the data payload
    pub fn unwrap(self) -> D {
        self.data
    }
}

impl<'a, P, D> Deref for LifecycleWriteGuard<'a, P, D> {
    type Target = P;
    #[inline]
    fn deref(&self) -> &P {
        &self.guard
    }
}

impl<'a, P, D> DerefMut for LifecycleWriteGuard<'a, P, D> {
    #[inline]
    fn deref_mut(&mut self) -> &mut P {
        &mut self.guard
    }
}
