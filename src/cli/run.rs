use crate::git;
use crate::util::MetadataStoreOpt;
use orfail::OrFail;
use std::collections::{BTreeMap, BTreeSet};
use std::io::{Read as _, Write as _};
use std::path::PathBuf;
use std::process::Command;
use tempfile::NamedTempFile;

#[derive(Debug, structopt::StructOpt)]
#[structopt(rename_all = "kebab-case")]
pub struct RunOpt {
    #[structopt(flatten)]
    pub mlmd: MetadataStoreOpt,

    #[structopt(long = "env")]
    pub envs: Vec<KeyValue>,

    #[structopt(long = "secret-env")]
    pub secret_envs: Vec<KeyValue>,

    #[structopt(long = "custom-property")]
    pub custom_properties: Vec<KeyValue>,

    #[structopt(long)]
    pub execution_name: Option<String>,

    #[structopt(long, default_value = "MLMD_EXECUTION_ID")]
    pub execution_id_envvar: String,

    #[structopt(long)]
    pub context_name: Option<String>,

    // object storage.
    #[structopt(long)]
    pub storage: Option<PathBuf>,

    #[structopt(long)]
    pub result_dir: Option<PathBuf>,

    #[structopt(long)]
    pub sweep_result_dir: bool,

    #[structopt(long)]
    pub forbid_dirty: bool,

    pub command_name: String,
    pub command_args: Vec<String>,
}

impl RunOpt {
    pub async fn execute(&self) -> orfail::Result<()> {
        let mut store = self.mlmd.connect().await.or_fail()?;

        let properties = ExecutionProperties::new(self).or_fail()?;

        let execution_type_id = store
            .put_execution_type("mog_run@0.0")
            .can_add_fields() // TODO: remove
            .can_omit_fields() // TODO: remove
            .properties(properties.property_types())
            .execute()
            .await
            .or_fail()?;

        let mut req = store
            .post_execution(execution_type_id)
            .properties(properties.property_values()?)
            .state(mlmd::metadata::ExecutionState::New);
        for x in &self.custom_properties {
            req = req.custom_property(&x.key, x.value.as_str());
        }
        if let Some(v) = &self.execution_name {
            req = req.name(v);
        }
        let execution_id = req.execute().await.or_fail()?;

        let context_id = if let Some(context_name) = &self.context_name {
            let type_name = "mog_exp@0.0";
            let context_type_id = store
                .put_context_type(type_name)
                .execute()
                .await
                .or_fail()?;
            let context_id = match store
                .post_context(context_type_id, context_name)
                .execute()
                .await
            {
                Ok(id) => id,
                Err(mlmd::errors::PostError::NameAlreadyExists { .. }) => {
                    store
                        .get_contexts()
                        .type_and_name(type_name, context_name)
                        .execute()
                        .await
                        .or_fail()?[0]
                        .id
                }
                Err(e) => {
                    return Err(e).or_fail()?;
                }
            };
            Some(context_id)
        } else {
            std::env::var(crate::env::KEY_CONTEXT_ID)
                .ok()
                .map(|s| s.parse().map(mlmd::metadata::ContextId::new))
                .transpose()
                .or_fail()?
        };
        if let Some(context_id) = context_id {
            store
                .put_association(context_id, execution_id)
                .execute()
                .await
                .or_fail()?;
        }

        let mut command = Command::new(&self.command_name);
        for env in &self.envs {
            command.env(&env.key, &env.value);
        }
        for env in &self.secret_envs {
            command.env(&env.key, &env.value);
        }
        if let Some(context_id) = context_id {
            command.env(crate::env::KEY_CONTEXT_ID, context_id.to_string());
        }
        let child = command
            .args(self.command_args.iter())
            .env(&self.execution_id_envvar, execution_id.to_string())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .or_fail()?;
        store
            .put_execution(execution_id)
            .state(mlmd::metadata::ExecutionState::Running)
            .execute()
            .await
            .or_fail()?;

        // TODO: signal handling
        let result = Runner::new(child, self)
            .map(|runner| runner.run())
            .unwrap_or_else(|e| RunResult {
                result: Err(e),
                stdout_uri: None,
                stderr_uri: None,
                result_uri: None,
            });
        let state = if result.result.is_ok() {
            mlmd::metadata::ExecutionState::Complete
        } else {
            mlmd::metadata::ExecutionState::Failed
        };
        let mut put_request = store.put_execution(execution_id).state(state);
        if let Some(v) = self.storage.as_ref().and_then(|s| s.to_str()) {
            put_request = put_request.property::<&str>("storage", v);
        }
        if let Some(v) = &result.stdout_uri {
            put_request = put_request.property::<&str>("stdout_uri", v.as_str());
        }
        if let Some(v) = &result.stderr_uri {
            put_request = put_request.property::<&str>("stderr_uri", v.as_str());
        }
        if let Some(v) = &result.result_uri {
            put_request = put_request.property::<&str>("result_uri", v.as_str());
        }
        if let Ok(exit_status) = result.result {
            if let Some(code) = exit_status.code() {
                put_request = put_request.property("exit_code", code);
            }
        }
        put_request.execute().await.or_fail()?;
        result.result?;

        Ok(())
    }
}

#[derive(Debug)]
pub struct RunResult {
    result: orfail::Result<std::process::ExitStatus>,
    stdout_uri: Option<String>,
    stderr_uri: Option<String>,
    result_uri: Option<String>,
}

#[derive(Debug)]
pub struct Runner {
    child: std::process::Child,
    stdout: NamedTempFile,
    stderr: NamedTempFile,
    storage: Option<PathBuf>,
    result_dir: Option<PathBuf>,
    sweep_result_dir: bool,
}

impl Runner {
    pub fn new(child: std::process::Child, opt: &RunOpt) -> orfail::Result<Self> {
        Ok(Self {
            child,
            stdout: NamedTempFile::new().or_fail()?,
            stderr: NamedTempFile::new().or_fail()?,
            storage: opt.storage.clone(),
            result_dir: opt.result_dir.clone(),
            sweep_result_dir: opt.sweep_result_dir,
        })
    }

    pub fn run(mut self) -> RunResult {
        let mut result = RunResult {
            result: self.run_inner(),
            stdout_uri: None,
            stderr_uri: None,
            result_uri: None,
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

            if let Some(result_dir) = &self.result_dir {
                // TODO: Archive the directory and upload the result
                if self.sweep_result_dir {
                    let _ = std::fs::remove_dir_all(result_dir); // TODO: Emit a warning message if `Err(_)`
                }
            }
        }
        result
    }

    fn run_inner(&mut self) -> orfail::Result<std::process::ExitStatus> {
        let mut buf = vec![0u8; 4096];
        loop {
            if let Some(r) = &mut self.child.stdout {
                let n = r.read(&mut buf).or_fail()?;
                self.stdout.write_all(&buf[..n]).or_fail()?;
                std::io::stdout().write_all(&buf[..n]).or_fail()?;
            }
            if let Some(r) = &mut self.child.stderr {
                let n = r.read(&mut buf).or_fail()?;
                self.stderr.write_all(&buf[..n]).or_fail()?;
                std::io::stderr().write_all(&buf[..n]).or_fail()?;
            }
            if let Some(exit_status) = self.child.try_wait().or_fail()? {
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
    pub envvars: BTreeMap<String, String>,
    pub envvars_secret: BTreeSet<String>,
}

impl ExecutionProperties {
    pub fn new(opt: &RunOpt) -> orfail::Result<Self> {
        let mut command = opt.command_name.clone();
        if !opt.command_args.is_empty() {
            command += &opt.command_args.join(" ");
        }

        let git = git::GitInfo::new(std::env::current_dir().or_fail()?).or_fail()?;
        if opt.forbid_dirty && !git.is_dirty {
            return Err(orfail::Failure::new(
                "there are dirty files that aren't commited to the git repository.",
            ));
        }

        let envvars = opt.envs.iter().cloned().map(|e| (e.key, e.value)).collect();
        let envvars_secret = opt.secret_envs.iter().cloned().map(|e| e.key).collect();

        Ok(Self {
            user: std::env::var("USER").ok(),
            hostname: hostname::get()
                .ok()
                .and_then(|s| s.to_str().map(|s| s.to_owned())),
            command,
            git,
            envvars,
            envvars_secret,
        })
    }

    pub fn property_types(&self) -> mlmd::metadata::PropertyTypes {
        use mlmd::metadata::PropertyType;

        vec![
            ("user", PropertyType::String),
            ("hostname", PropertyType::String),
            ("command", PropertyType::String),
            ("exit_code", PropertyType::Int),
            ("envvars", PropertyType::String),
            ("envvars_secret", PropertyType::String),
            ("git_commit", PropertyType::String),
            ("git_url", PropertyType::String),
            ("git_cwd", PropertyType::String),
            ("git_dirty", PropertyType::Int),
            ("storage", PropertyType::String),
            ("stdout_uri", PropertyType::String), // TODO: Use artifact instead.
            ("stderr_uri", PropertyType::String), // TODO: Ditto.
            ("result_uri", PropertyType::String), // TODO: Ditto.
        ]
        .into_iter()
        .map(|(k, v)| (k.to_owned(), v))
        .collect()
    }

    pub fn property_values(&self) -> orfail::Result<mlmd::metadata::PropertyValues> {
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

        if !self.envvars.is_empty() {
            properties.insert(
                "envvars".to_owned(),
                serde_json::to_string(&self.envvars).or_fail()?.into(),
            );
        }
        if !self.envvars_secret.is_empty() {
            properties.insert(
                "envvars_secret".to_owned(),
                serde_json::to_string(&self.envvars_secret)
                    .or_fail()?
                    .into(),
            );
        }

        Ok(properties)
    }
}

#[derive(Debug, Clone)]
pub struct KeyValue {
    pub key: String,
    pub value: String,
}

impl std::str::FromStr for KeyValue {
    type Err = orfail::Failure;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut iter = s.splitn(2, '=');
        let key = iter.next().expect("unreachable").to_owned();
        let value = if let Some(value) = iter.next() {
            value.to_owned()
        } else {
            std::env::var(&key)
                .or_fail_with(|e| format!("cannot get the value of the envvar {:?} ({e})", key))?
                .to_owned()
        };
        Ok(Self { key, value })
    }
}
