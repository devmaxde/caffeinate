//! FUSE3 daemon. Read-only mount serving the evmap-backed virtual tree.
//!
//! On Linux: `run("/tmp/qontext")` mounts and blocks.
//! On non-Linux: returns an error explaining the situation.

use anyhow::Result;

#[cfg(target_os = "linux")]
mod fuse;

/// Mount the FS at `mountpoint` and block. Read-only.
pub fn run(mountpoint: &str) -> Result<()> {
    #[cfg(target_os = "linux")]
    {
        fuse::mount(mountpoint)
    }
    #[cfg(not(target_os = "linux"))]
    {
        anyhow::bail!(
            "FUSE3 mount not supported on this platform ({}). \
             Run on Linux. Other crates work fine for solo dev. \
             mountpoint requested = {}",
            std::env::consts::OS,
            mountpoint
        )
    }
}
