use std::path::{Path, PathBuf};
use std::sync::Arc;

use ce_adapters::Registry;
use ce_core::Document;
use tokio::sync::{mpsc, Semaphore};
use tracing::{debug, warn};

pub struct WalkItem {
    pub path: PathBuf,
    pub adapter: String,
    pub docs: Result<Vec<Document>, String>,
}

pub struct WalkOpts {
    pub concurrency: usize,
    pub sniff_bytes: usize,
}

impl Default for WalkOpts {
    fn default() -> Self {
        Self { concurrency: 16, sniff_bytes: 256 }
    }
}

pub fn walk(root: &Path, registry: Arc<Registry>, opts: WalkOpts) -> mpsc::Receiver<WalkItem> {
    let (tx, rx) = mpsc::channel(64);
    let sem = Arc::new(Semaphore::new(opts.concurrency));
    let walker = ignore::WalkBuilder::new(root).build();

    for entry in walker {
        let entry = match entry {
            Ok(e) => e,
            Err(e) => { warn!(error=%e, "walk error"); continue; }
        };
        if !entry.file_type().map(|f| f.is_file()).unwrap_or(false) {
            continue;
        }
        let path = entry.into_path();
        let registry = registry.clone();
        let tx = tx.clone();
        let sem = sem.clone();
        let sniff_bytes = opts.sniff_bytes;

        tokio::spawn(async move {
            let _permit = sem.acquire_owned().await.expect("sem closed");
            let sniff = read_sniff(&path, sniff_bytes).await.unwrap_or_default();
            let Some(adapter) = registry.pick(&path, &sniff) else {
                debug!(?path, "no adapter");
                return;
            };
            let name = adapter.name().to_string();
            let docs = adapter.read(&path).await.map_err(|e| e.to_string());
            let _ = tx.send(WalkItem { path, adapter: name, docs }).await;
        });
    }
    drop(tx);
    rx
}

async fn read_sniff(path: &Path, n: usize) -> std::io::Result<Vec<u8>> {
    use tokio::io::AsyncReadExt;
    let mut f = tokio::fs::File::open(path).await?;
    let mut buf = vec![0u8; n];
    let read = f.read(&mut buf).await?;
    buf.truncate(read);
    Ok(buf)
}
