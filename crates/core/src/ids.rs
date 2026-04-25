//! Path -> stable u64 inode for FUSE.
//!
//! FUSE needs u64 inodes. We hash paths and keep a bidirectional cache.
//! Collision risk is negligible for hackathon scale (<10k nodes).

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

static MAP: OnceLock<Mutex<InodeMap>> = OnceLock::new();

#[derive(Default)]
struct InodeMap {
    by_path: HashMap<String, u64>,
    by_ino: HashMap<u64, String>,
    next: u64,
}

fn map() -> &'static Mutex<InodeMap> {
    MAP.get_or_init(|| {
        let mut m = InodeMap::default();
        m.next = 2; // 1 is reserved by FUSE for root
        m.by_path.insert("/".into(), 1);
        m.by_ino.insert(1, "/".into());
        Mutex::new(m)
    })
}

pub fn ino_for(path: &str) -> u64 {
    let mut g = map().lock().unwrap();
    if let Some(&ino) = g.by_path.get(path) {
        return ino;
    }
    let ino = g.next;
    g.next += 1;
    g.by_path.insert(path.to_string(), ino);
    g.by_ino.insert(ino, path.to_string());
    ino
}

pub fn path_for(ino: u64) -> Option<String> {
    let g = map().lock().unwrap();
    g.by_ino.get(&ino).cloned()
}

/// Reset the cache. Test-only.
#[cfg(test)]
pub fn reset() {
    let mut g = map().lock().unwrap();
    g.by_path.clear();
    g.by_ino.clear();
    g.next = 2;
    g.by_path.insert("/".into(), 1);
    g.by_ino.insert(1, "/".into());
}
