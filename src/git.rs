use anyhow::Context;
use git_url_parse::GitUrl;
use std::path::{Path, PathBuf};

#[derive(Debug)]
pub struct GitInfo {
    commit: git2::Oid,
    origin_url: GitUrl,
    is_dirty: bool,
    current_dir: PathBuf,
}

impl GitInfo {
    pub fn repository_name(&self) -> &str {
        &self.origin_url.name
    }
}

impl GitInfo {
    pub fn new<P: AsRef<Path>>(dir: P) -> anyhow::Result<Self> {
        let repo = git2::Repository::discover(dir)?;
        let head = repo.head()?;
        let commit = head.peel_to_commit()?.id();
        let origin_url = repo
            .find_remote("origin")?
            .url()
            .ok_or_else(|| anyhow::anyhow!("origin URL is not a valid UTF-8"))
            .and_then(|s| GitUrl::parse(s).with_context(|| format!("malformed URL: {:?}", s)))?;

        let head_tree = head.peel_to_tree()?;
        let mut diff_opt = git2::DiffOptions::new();
        diff_opt.include_untracked(true);
        let diff_files = repo
            .diff_tree_to_workdir_with_index(Some(&head_tree), Some(&mut diff_opt))?
            .deltas()
            .len();
        let is_dirty = diff_files > 0;

        let rootdir = repo
            .workdir()
            .ok_or_else(|| anyhow::anyhow!("this is a bare repository"))?;
        let current_dir = std::env::current_dir()?
            .strip_prefix(rootdir)?
            .to_path_buf();
        Ok(Self {
            commit,
            origin_url,
            is_dirty,
            current_dir,
        })
    }
}
