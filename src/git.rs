use std::path::Path;

#[derive(Debug)]
pub struct GitInfo {
    commit: git2::Oid,
    // TODO: url, dirty, path
}

impl GitInfo {
    pub fn new<P: AsRef<Path>>(dir: P) -> anyhow::Result<Self> {
        let repo = git2::Repository::open(dir)?;
        let head = repo.head()?;
        let commit = head.peel_to_commit()?.id();

        // dbg!(repo.head()?.name());
        // dbg!(repo.head()?.peel_to_commit()?.id());
        // dbg!(repo.find_remote("origin")?.url());

        Ok(Self { commit })
    }
}
