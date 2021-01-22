use crate::env;
use crate::git;
use crate::util::MetadataStoreOpt;
use anyhow::Context as _;
use std::process::Command;

#[derive(Debug, structopt::StructOpt)]
pub struct RunOpt {
    #[structopt(flatten)]
    pub mlmd: MetadataStoreOpt,

    #[structopt(long = "env", name = "KEY(=VALUE)")]
    pub envs: Vec<EnvKeyValue>,

    #[structopt(long = "secret-env", name = "KEY(=VALUE)")]
    pub secret_envs: Vec<EnvKeyValue>,

    // context
    // tempdir, persistent-if-fail, upload
    // capture-{stdout, stderr}
    // forbid-dirty
    pub command_name: String,
    pub command_args: Vec<String>,
}

impl RunOpt {
    pub async fn execute(&self) -> anyhow::Result<()> {
        let mut store = self.mlmd.connect().await?;

        let properties = ExecutionProperties::new(self)?;

        let execution_type_id = store
            .put_execution_type("mog_run@0.0")
            .can_add_fields() // TODO
            .can_omit_fields() // TODO
            .properties(properties.property_types())
            .execute()
            .await?;

        let mut custom_properties = mlmd::metadata::PropertyValues::new();
        for env in &self.envs {
            custom_properties.insert(format!("env_{}", env.key), env.value.clone().into());
        }
        for env in &self.secret_envs {
            custom_properties.insert(format!("secret_env_{}", env.key), "".into());
        }

        let execution_id = store
            .post_execution(execution_type_id)
            .properties(properties.property_values())
            .custom_properties(custom_properties)
            .state(mlmd::metadata::ExecutionState::New)
            .execute()
            .await?;

        let mut command = Command::new(&self.command_name);
        for env in &self.envs {
            command.env(&env.key, &env.value);
        }
        for env in &self.secret_envs {
            command.env(&env.key, &env.value);
        }
        let mut child = command
            .args(self.command_args.iter())
            .env(env::KEY_CURRENT_EXECUTION_ID, execution_id.to_string())
            .spawn()?;
        store
            .put_execution(execution_id)
            .state(mlmd::metadata::ExecutionState::Running)
            .execute()
            .await?;
        child.wait()?;
        store
            .put_execution(execution_id)
            .state(mlmd::metadata::ExecutionState::Complete)
            .execute()
            .await?;

        Ok(())
    }
}

#[derive(Debug)]
pub struct ExecutionProperties {
    pub user: Option<String>,
    pub hostname: Option<String>,
    pub command: String,
    pub git: git::GitInfo,
}

impl ExecutionProperties {
    pub fn new(opt: &RunOpt) -> anyhow::Result<Self> {
        let mut command = opt.command_name.clone();
        if !opt.command_args.is_empty() {
            command += &opt.command_args.join(" ");
        }

        Ok(Self {
            user: std::env::var("USER").ok(),
            hostname: hostname::get()
                .ok()
                .and_then(|s| s.to_str().map(|s| s.to_owned())),
            command,
            git: git::GitInfo::new(std::env::current_dir()?)?,
        })
    }

    pub fn property_types(&self) -> mlmd::metadata::PropertyTypes {
        use mlmd::metadata::PropertyType;

        // exit_code
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

    pub fn property_values(&self) -> mlmd::metadata::PropertyValues {
        let mut properties = mlmd::metadata::PropertyValues::new();
        properties.insert("command".to_owned(), self.command.clone().into());

        if let Some(v) = self.user.clone() {
            properties.insert("user".to_owned(), v.into());
        }

        if let Some(v) = self.hostname.clone() {
            properties.insert("hostname".to_owned(), v.into());
        }

        properties.insert("git_commit".to_owned(), self.git.commit.to_string().into());
        if let Some(v) = self.git.https_url() {
            properties.insert("git_url".to_owned(), v.into());
        }
        if let Some(v) = self.git.current_dir.to_str() {
            properties.insert("git_cwd".to_owned(), v.to_owned().into());
        }
        properties.insert("git_dirty".to_owned(), (self.git.is_dirty as i32).into());

        properties
    }
}

#[derive(Debug)]
pub struct EnvKeyValue {
    pub key: String,
    pub value: String,
}

impl std::str::FromStr for EnvKeyValue {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut iter = s.splitn(2, '=');
        let key = iter.next().expect("unreachable").to_owned();
        let value = if let Some(value) = iter.next() {
            value.to_owned()
        } else {
            std::env::var(&key)
                .with_context(|| format!("cannot get the value of the envvar {:?}", key))?
                .to_owned()
        };
        Ok(Self { key, value })
    }
}
