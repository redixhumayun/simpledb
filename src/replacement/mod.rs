//! Buffer pool replacement policies for cache eviction.
//!
//! This module provides three replacement policies, each enabled via Cargo features.
//! Exactly one must be enabled at compile time.
//!
//! # Available Policies
//!
//! - **LRU** (`replacement_lru`): Least Recently Used policy using intrusive doubly-linked list.
//!   Evicts the least recently accessed frame. Best for workloads with temporal locality.
//!
//! - **Clock** (`replacement_clock`): Second-chance algorithm with circular buffer and reference bits.
//!   Approximates LRU with lower overhead. Good general-purpose policy.
//!
//! - **SIEVE** (`replacement_sieve`): Hybrid policy combining list structure with clock-like hand.
//!   Designed to resist scan-resistant workloads. See SIEVE paper for details.
//!
//! # Interface
//!
//! All policies expose a `PolicyState` struct with three methods:
//! - `record_hit()`: Called on cache hit to update access tracking
//! - `on_frame_assigned()`: Called when a frame is newly allocated
//! - `evict_frame()`: Selects and returns a victim frame for eviction

#[cfg(any(
    all(feature = "replacement_lru", feature = "replacement_clock"),
    all(feature = "replacement_lru", feature = "replacement_sieve"),
    all(feature = "replacement_clock", feature = "replacement_sieve"),
))]
compile_error!("Enable only one buffer replacement policy feature (LRU, Clock, or SIEVE)");

#[cfg(not(any(
    feature = "replacement_lru",
    feature = "replacement_clock",
    feature = "replacement_sieve"
)))]
compile_error!("At least one buffer replacement policy feature must be enabled");

#[cfg(feature = "replacement_lru")]
mod lru;
#[cfg(feature = "replacement_lru")]
pub use lru::PolicyState;

#[cfg(feature = "replacement_clock")]
mod clock;
#[cfg(feature = "replacement_clock")]
pub use clock::PolicyState;

#[cfg(feature = "replacement_sieve")]
mod sieve;
#[cfg(feature = "replacement_sieve")]
pub use sieve::PolicyState;
