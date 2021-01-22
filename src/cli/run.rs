use crate::env;
use crate::git;
use crate::util::MetadataStoreOpt;
use anyhow::Context as _;
use std::io::{Read as _, Write as _};
use std::path::PathBuf;
use std::process::Command;
use tempfile::NamedTempFile;

#[derive(Debug, structopt::StructOpt)]
pub struct RunOpt {
    #[structopt(flatten)]
    pub mlmd: MetadataStoreOpt,

    #[structopt(long = "env")]
    pub envs: Vec<EnvKeyValue>,

    #[structopt(long = "secret-env")]
    pub secret_envs: Vec<EnvKeyValue>,

    // TODO: rename (object-store?)
    #[structopt(long)]
    pub storage: Option<PathBuf>,

    // context
    // tempdir
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
        let child = command
            .args(self.command_args.iter())
            .env(env::KEY_CURRENT_EXECUTION_ID, execution_id.to_string())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()?;
        store
            .put_execution(execution_id)
            .state(mlmd::metadata::ExecutionState::Running)
            .execute()
            .await?;

        // TODO: signal handling
        let result = Runner::new(child, self)
            .map(|runner| runner.run())
            .unwrap_or_else(|e| RunResult {
                result: Err(e.into()),
                stdout_uri: None,
                stderr_uri: None,
            });
        let state = if result.result.is_ok() {
            mlmd::metadata::ExecutionState::Complete
        } else {
            mlmd::metadata::ExecutionState::Failed
        };
        let mut put_request = store.put_execution(execution_id).state(state);
        if let Some(v) = &result.stdout_uri {
            put_request = put_request.property::<&str>("stdout_uri", v.as_str().into());
        }
        if let Some(v) = &result.stderr_uri {
            put_request = put_request.property::<&str>("stderr_uri", v.as_str().into());
        }
        put_request.execute().await?;
        result.result?;

        Ok(())
    }
}

#[derive(Debug)]
pub struct RunResult {
    result: anyhow::Result<std::process::ExitStatus>,
    stdout_uri: Option<String>,
    stderr_uri: Option<String>,
}

#[derive(Debug)]
pub struct Runner {
    child: std::process::Child,
    stdout: NamedTempFile,
    stderr: NamedTempFile,
    storage: Option<PathBuf>,
}

impl Runner {
    pub fn new(child: std::process::Child, opt: &RunOpt) -> anyhow::Result<Self> {
        Ok(Self {
            child,
            stdout: NamedTempFile::new()?,
            stderr: NamedTempFile::new()?,
            storage: opt.storage.clone(),
        })
    }

    pub fn run(mut self) -> RunResult {
        let mut result = RunResult {
            result: self.run_inner(),
            stdout_uri: None,
            stderr_uri: None,
        };
        let _ = self.stdout.flush();
        let _ = self.stderr.flush();

        if let Some(storage) = &self.storage {
            let output = std::process::Command::new(storage)
                .arg(self.stdout.path())
                .stderr(std::process::Stdio::inherit())
                .output()
                .expect("TODO");
            if output.status.success() {
                result.stdout_uri = Some(
                    String::from_utf8(output.stdout)
                        .expect("TODO")
                        .trim()
                        .to_owned(),
                );
            }

            let output = std::process::Command::new(storage)
                .arg(self.stderr.path())
                .stderr(std::process::Stdio::inherit())
                .output()
                .expect("TODO");
            if output.status.success() {
                result.stderr_uri = Some(
                    String::from_utf8(output.stdout)
                        .expect("TODO")
                        .trim()
                        .to_owned(),
                );
            }
        }
        result
    }

    fn run_inner(&mut self) -> anyhow::Result<std::process::ExitStatus> {
        let mut buf = vec![0u8; 4096];
        loop {
            if let Some(r) = &mut self.child.stdout {
                let n = r.read(&mut buf)?;
                self.stdout.write_all(&buf[..n])?;
                std::io::stdout().write_all(&buf[..n])?;
            }
            if let Some(r) = &mut self.child.stderr {
                let n = r.read(&mut buf)?;
                self.stderr.write_all(&buf[..n])?;
                std::io::stderr().write_all(&buf[..n])?;
            }
            if let Some(exit_status) = self.child.try_wait()? {
                return Ok(exit_status);
            }
        }
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
            ("stdout_uri", PropertyType::String),
            ("stderr_uri", PropertyType::String),
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
