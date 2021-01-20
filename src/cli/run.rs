use crate::git;
use crate::util::MetadataStoreOpt;
use std::process::Command;

#[derive(Debug, structopt::StructOpt)]
pub struct RunOpt {
    #[structopt(flatten)]
    pub mlmd: MetadataStoreOpt,

    pub command_name: String,
    pub command_args: Vec<String>,
}

impl RunOpt {
    pub async fn execute(&self) -> anyhow::Result<()> {
        let _store = self.mlmd.connect().await?;

        let git_info = git::GitInfo::new(std::env::current_dir()?)?;
        dbg!(&git_info);

        // let execution_id = store.put_
        let mut child = Command::new(&self.command_name)
            .args(self.command_args.iter())
            .spawn()?;
        child.wait()?;
        Ok(())
    }
}
