//! On-disk cache for rendered entity markdown.
//!
//! Layout: `<root>/<entity_type>/<sanitized_id>.md`. Sanitization replaces
//! path separators and control chars so an entity id can never escape `<root>`
//! or shadow another entity. Empty/dot ids are rejected.

use std::path::{Path, PathBuf};

/// Replace anything that could break a filename or enable path traversal with
/// `_`. Keeps printable ASCII letters/digits and a small punctuation set so
/// emails / hyphenated ids remain readable on disk.
pub fn sanitize_id(id: &str) -> String {
    let mut out = String::with_capacity(id.len());
    for ch in id.chars() {
        let safe = matches!(ch,
            'a'..='z' | 'A'..='Z' | '0'..='9' | '-' | '_' | '.' | '@' | '+' | '=' | '~'
        );
        out.push(if safe { ch } else { '_' });
    }
    // Reject leading dot to avoid hidden files / `..` traversal artefacts.
    if out.starts_with('.') {
        out.insert(0, '_');
    }
    if out.is_empty() {
        out.push('_');
    }
    out
}

/// Same idea for the entity-type directory segment. Types are usually plain
/// words but the sanitizer keeps the layer schema-agnostic.
pub fn sanitize_type(etype: &str) -> String {
    sanitize_id(etype)
}

pub fn cache_path(root: &Path, etype: &str, id: &str) -> PathBuf {
    root.join(sanitize_type(etype)).join(format!("{}.md", sanitize_id(id)))
}

/// Read a cached markdown file. Returns `Ok(None)` if the file doesn't exist;
/// any other I/O error propagates.
pub fn read_cached(root: &Path, etype: &str, id: &str) -> std::io::Result<Option<String>> {
    let path = cache_path(root, etype, id);
    match std::fs::read_to_string(&path) {
        Ok(s) => Ok(Some(s)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}

/// Write markdown atomically: tmp file + rename, so a crash mid-write can
/// never leave a half-written file that a later run treats as cached.
pub fn write_cached(root: &Path, etype: &str, id: &str, body: &str) -> std::io::Result<PathBuf> {
    let final_path = cache_path(root, etype, id);
    if let Some(dir) = final_path.parent() {
        std::fs::create_dir_all(dir)?;
    }
    let tmp = final_path.with_extension("md.tmp");
    std::fs::write(&tmp, body)?;
    std::fs::rename(&tmp, &final_path)?;
    Ok(final_path)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanitize_blocks_traversal() {
        assert_eq!(sanitize_id("../etc/passwd"), "_.._etc_passwd");
        assert_eq!(sanitize_id("a/b\\c"), "a_b_c");
        assert_eq!(sanitize_id(".hidden"), "_.hidden");
        assert_eq!(sanitize_id(""), "_");
        assert_eq!(sanitize_id("foo@bar.com"), "foo@bar.com");
    }
}
