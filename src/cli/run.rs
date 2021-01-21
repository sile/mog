use crate::git;
use crate::util::MetadataStoreOpt;
use std::process::Command;

#[derive(Debug, structopt::StructOpt)]
pub struct RunOpt {
    #[structopt(flatten)]
    pub mlmd: MetadataStoreOpt,

    // env
    // allow-dirty
    // context
    // tempdir, persistent-if-fail, upload
    // capture-{stdout, stderr}
    pub command_name: String,
    pub command_args: Vec<String>,
}

impl RunOpt {
    pub async fn execute(&self) -> anyhow::Result<()> {
        let mut store = self.mlmd.connect().await?;

        let properties = ExecutionProperties::new()?;

        let execution_type_id = store
            .put_execution_type("mog_run@0.0")
            .can_add_fields() // TODO
            .can_omit_fields() // TODO
            .properties(properties.property_types())
            .execute()
            .await?;
        dbg!(execution_type_id);

        // let execution_id = store.put_
        let mut child = Command::new(&self.command_name)
            .args(self.command_args.iter())
            .spawn()?;
        child.wait()?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct ExecutionProperties {
    pub user: Option<String>,
    pub hostname: Option<String>,
    pub git: git::GitInfo,
}

impl ExecutionProperties {
    pub fn new() -> anyhow::Result<Self> {
        Ok(Self {
            user: std::env::var("USER").ok(),
            hostname: hostname::get()
                .ok()
                .and_then(|s| s.to_str().map(|s| s.to_owned())),
            git: git::GitInfo::new(std::env::current_dir()?)?,
        })
    }

    pub fn property_types(&self) -> mlmd::metadata::PropertyTypes {
        use mlmd::metadata::PropertyType;

        vec![
            ("user", PropertyType::String),
            ("hostname", PropertyType::String),
            ("command", PropertyType::String),
            ("git_commit", PropertyType::String),
            ("git_url", PropertyType::String),
            ("git_cwd", PropertyType::String),
            ("git_dirty", PropertyType::Int),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_owned(), v))
        .collect()
    }
}
