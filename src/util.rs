use crate::env;
use anyhow::Context;

pub async fn mlmd_connect(database_uri: &str) -> anyhow::Result<mlmd::MetadataStore> {
    let store = mlmd::MetadataStore::connect(database_uri)
        .await
        .with_context(|| format!("cannot connect to the database: {:?}", database_uri))?;
    Ok(store)
}

#[derive(Debug, structopt::StructOpt)]
pub struct MetadataStoreOpt {
    #[structopt(long, name="URI", env = env::KEY_DATABASE, hide_env_values = true)]
    pub database: String,
}

impl MetadataStoreOpt {
    pub async fn connect(&self) -> anyhow::Result<mlmd::MetadataStore> {
        mlmd_connect(&self.database).await
    }
}
