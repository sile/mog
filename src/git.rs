use git_url_parse::GitUrl;
use orfail::OrFail;
use std::path::{Path, PathBuf};

#[derive(Debug)]
pub struct GitInfo {
    pub commit: git2::Oid,
    pub origin_url: GitUrl,
    pub is_dirty: bool,
    pub current_dir: PathBuf,
}

impl GitInfo {
    pub fn repository_name(&self) -> &str {
        &self.origin_url.name
    }

    pub fn https_url(&self) -> Option<String> {
        self.origin_url
            .host
            .as_ref()
            .map(|host| format!("https://{}/{}", host, self.origin_url.fullname))
    }
}

impl GitInfo {
    pub fn new<P: AsRef<Path>>(dir: P) -> orfail::Result<Self> {
        let repo = git2::Repository::discover(dir).or_fail()?;
        let head = repo.head().or_fail()?;
        let commit = head.peel_to_commit().or_fail()?.id();
        let origin_url = repo
            .find_remote("origin")
            .or_fail()?
            .url()
            .or_fail_with(|_| "origin URL is not a valid UTF-8".to_string())
            .and_then(|s| {
                GitUrl::parse(s)
                    .map_err(|e| orfail::Failure::new(format!("malformed URL: {:?} ({e})", s)))
            })?;

        let head_tree = head.peel_to_tree().or_fail()?;
        let mut diff_opt = git2::DiffOptions::new();
        diff_opt.include_untracked(true);
        let diff_files = repo
            .diff_tree_to_workdir_with_index(Some(&head_tree), Some(&mut diff_opt))
            .or_fail()?
            .deltas()
            .len();
        let is_dirty = diff_files > 0;

        let rootdir = repo
            .workdir()
            .or_fail_with(|_| "this is a bare repository".to_string())?;
        let current_dir = std::env::current_dir()
            .or_fail()?
            .strip_prefix(rootdir)
            .or_fail()?
            .to_path_buf();
        Ok(Self {
            commit,
            origin_url,
            is_dirty,
            current_dir,
        })
    }
}
