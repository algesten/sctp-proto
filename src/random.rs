//! The random source behind every value sctp-proto draws.
//!
//! Verification tags, initial TSNs, state cookies and association ids all come from [`fill`]. An
//! embedder that must draw every random value from its own source installs it once with
//! [`set_fill`].

use core::num::NonZeroU32;
use std::sync::OnceLock;

/// The process-wide random fill, installed once with [`set_fill`].
static FILL: OnceLock<fn(&mut [u8])> = OnceLock::new();

/// Install the random source every value sctp-proto draws comes from: verification tags, initial
/// TSNs, state cookies and association ids.
///
/// `fill` must fill the whole slice with random bytes, or panic. It is called from inside
/// sctp-proto's own calls, on whatever thread drives the [`Endpoint`](crate::Endpoint) or
/// [`Association`](crate::Association). The first call installs it for the life of the process and
/// returns `true`; a later call changes nothing and returns `false`.
///
/// With no fill installed, a draw falls back to `rand`'s thread RNG when the `rand` feature is on
/// (the default), and panics when it is off, so a build without `rand` never produces a value from
/// a source the embedder did not choose.
pub fn set_fill(fill: fn(&mut [u8])) -> bool {
    FILL.set(fill).is_ok()
}

/// Fill `buf` from the installed fill, or the fallback when none is installed.
pub(crate) fn fill(buf: &mut [u8]) {
    match FILL.get() {
        Some(f) => f(buf),
        None => fallback(buf),
    }
}

#[cfg(feature = "rand")]
fn fallback(buf: &mut [u8]) {
    rand::RngCore::fill_bytes(&mut rand::rng(), buf);
}

#[cfg(not(feature = "rand"))]
#[cold]
fn fallback(_buf: &mut [u8]) {
    panic!("sctp-proto: no random fill installed (call sctp_proto::set_fill first)");
}

/// A random `u32`.
pub(crate) fn random_u32() -> u32 {
    let mut b = [0; 4];
    fill(&mut b);
    u32::from_le_bytes(b)
}

/// A random non-zero `u32`: draws again on zero, as `rand::random::<NonZeroU32>()` does.
pub(crate) fn random_nonzero_u32() -> NonZeroU32 {
    loop {
        if let Some(v) = NonZeroU32::new(random_u32()) {
            return v;
        }
    }
}
