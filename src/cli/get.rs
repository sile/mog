use crate::env;
use crate::util;
use orfail::OrFail;

#[derive(Debug, structopt::StructOpt)]
#[structopt(rename_all = "kebab-case")]
pub enum GetOpt {
    Artifacts(GetArtifactsOpt),
    Contexts(GetContextsOpt),
    Events(GetEventsOpt),
    Executions(GetExecutionsOpt),
}

impl GetOpt {
    pub async fn execute(&self) -> orfail::Result<()> {
        match self {
            Self::Artifacts(opt) => opt.execute().await,
            Self::Contexts(opt) => opt.execute().await,
            Self::Events(opt) => opt.execute().await,
            Self::Executions(opt) => opt.execute().await,
        }
    }
}

#[derive(Debug, structopt::StructOpt)]
#[structopt(rename_all = "kebab-case")]
pub struct GetArtifactsOpt {
    #[structopt(long, env = env::KEY_DATABASE, hide_env_values = true)]
    pub database: String,
}

impl GetArtifactsOpt {
    pub async fn execute(&self) -> orfail::Result<()> {
        let mut store = util::mlmd_connect(&self.database).await.or_fail()?;
        let artifacts = store.get_artifacts().execute().await.or_fail()?;
        for artifact in artifacts {
            println!("{:?}", artifact);
        }
        Ok(())
    }
}

#[derive(Debug, structopt::StructOpt)]
#[structopt(rename_all = "kebab-case")]
pub struct GetContextsOpt {
    #[structopt(long, env = env::KEY_DATABASE, hide_env_values = true)]
    pub database: String,
}

impl GetContextsOpt {
    pub async fn execute(&self) -> orfail::Result<()> {
        let mut store = util::mlmd_connect(&self.database).await.or_fail()?;
        let contexts = store.get_contexts().execute().await.or_fail()?;
        for context in contexts {
            println!("{:?}", context);
        }
        Ok(())
    }
}

#[derive(Debug, structopt::StructOpt)]
#[structopt(rename_all = "kebab-case")]
pub struct GetEventsOpt {
    #[structopt(long, env = env::KEY_DATABASE, hide_env_values = true)]
    pub database: String,
}

impl GetEventsOpt {
    pub async fn execute(&self) -> orfail::Result<()> {
        let mut store = util::mlmd_connect(&self.database).await.or_fail()?;
        let events = store.get_events().execute().await.or_fail()?;
        for event in events {
            println!("{:?}", event);
        }
        Ok(())
    }
}

#[derive(Debug, structopt::StructOpt)]
#[structopt(rename_all = "kebab-case")]
pub struct GetExecutionsOpt {
    #[structopt(long, env = env::KEY_DATABASE, hide_env_values = true)]
    pub database: String,
}

impl GetExecutionsOpt {
    pub async fn execute(&self) -> orfail::Result<()> {
        let mut store = util::mlmd_connect(&self.database).await.or_fail()?;
        let executions = store.get_executions().execute().await.or_fail()?;
        for execution in executions {
            println!("{:?}", execution);
        }
        Ok(())
    }
}
