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
