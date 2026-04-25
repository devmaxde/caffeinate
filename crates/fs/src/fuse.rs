//! Linux-only FUSE3 implementation.

use anyhow::Result;
use fuser::{
    FileAttr, FileType, Filesystem, MountOption, ReplyAttr, ReplyData, ReplyDirectory,
    ReplyEntry, Request,
};
use qontext_core::ids;
use qontext_core::model::NodeKind;
use qontext_core::state;
use std::ffi::OsStr;
use std::time::{Duration, UNIX_EPOCH};

const TTL: Duration = Duration::from_secs(1);

pub fn mount(mountpoint: &str) -> Result<()> {
    std::fs::create_dir_all(mountpoint)?;
    let opts = vec![
        MountOption::FSName("qontext".into()),
        MountOption::AutoUnmount,
        MountOption::AllowOther,
        MountOption::RO,
    ];
    tracing::info!(mountpoint, "mounting FUSE");
    fuser::mount2(QontextFs, mountpoint, &opts)?;
    Ok(())
}

struct QontextFs;

impl Filesystem for QontextFs {
    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let parent_path = match ids::path_for(parent) {
            Some(p) => p,
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };
        let child_name = name.to_string_lossy();
        let child_path = join_path(&parent_path, &child_name);

        match state::read_node(&child_path) {
            Some(node) => {
                let ino = ids::ino_for(&child_path);
                reply.entry(&TTL, &attr_for(&node, ino), 0);
            }
            None => reply.error(libc::ENOENT),
        }
    }

    fn getattr(&mut self, _req: &Request, ino: u64, reply: ReplyAttr) {
        let path = match ids::path_for(ino) {
            Some(p) => p,
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };
        match state::read_node(&path) {
            Some(node) => reply.attr(&TTL, &attr_for(&node, ino)),
            None => reply.error(libc::ENOENT),
        }
    }

    fn read(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock: Option<u64>,
        reply: ReplyData,
    ) {
        let path = match ids::path_for(ino) {
            Some(p) => p,
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };
        match state::read_node(&path) {
            Some(node) if matches!(node.kind, NodeKind::File) => {
                let bytes = node.content.as_bytes();
                let off = (offset as usize).min(bytes.len());
                let end = (off + size as usize).min(bytes.len());
                reply.data(&bytes[off..end]);
            }
            _ => reply.error(libc::ENOENT),
        }
    }

    fn readdir(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        let path = match ids::path_for(ino) {
            Some(p) => p,
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };
        let dir = match state::read_node(&path) {
            Some(n) if n.is_dir() => n,
            _ => {
                reply.error(libc::ENOENT);
                return;
            }
        };

        // synthetic "." and ".." entries first
        let mut entries: Vec<(u64, FileType, String)> = vec![
            (ino, FileType::Directory, ".".into()),
            (ino, FileType::Directory, "..".into()),
        ];
        for child_path in &dir.children {
            let kind = match state::read_node(child_path) {
                Some(n) if n.is_dir() => FileType::Directory,
                _ => FileType::RegularFile,
            };
            let name = basename(child_path);
            entries.push((ids::ino_for(child_path), kind, name));
        }

        for (i, (e_ino, kind, name)) in entries.into_iter().enumerate().skip(offset as usize) {
            if reply.add(e_ino, (i + 1) as i64, kind, &name) {
                break;
            }
        }
        reply.ok();
    }
}

fn join_path(parent: &str, child: &str) -> String {
    if parent == "/" {
        format!("/{}", child)
    } else {
        format!("{}/{}", parent, child)
    }
}

fn basename(path: &str) -> String {
    path.rsplit('/').next().unwrap_or("").to_string()
}

fn attr_for(node: &qontext_core::FileNode, ino: u64) -> FileAttr {
    let kind = match node.kind {
        NodeKind::Dir => FileType::Directory,
        NodeKind::File => FileType::RegularFile,
    };
    let mtime = UNIX_EPOCH + Duration::from_secs(node.mtime_secs);
    FileAttr {
        ino,
        size: node.size,
        blocks: 0,
        atime: mtime,
        mtime,
        ctime: mtime,
        crtime: mtime,
        kind,
        perm: 0o644,
        nlink: 1,
        uid: unsafe { libc::getuid() },
        gid: unsafe { libc::getgid() },
        rdev: 0,
        flags: 0,
        blksize: 4096,
    }
}
