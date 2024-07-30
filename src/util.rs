use crate::env;
use orfail::OrFail;

pub async fn mlmd_connect(database_uri: &str) -> orfail::Result<mlmd::MetadataStore> {
    let store = mlmd::MetadataStore::connect(database_uri)
        .await
        .or_fail_with(|e| format!("cannot connect to the database: {:?} ({e})", database_uri))?;
    Ok(store)
}

#[derive(Debug, structopt::StructOpt)]
pub struct MetadataStoreOpt {
    #[structopt(long, name="URI", env = env::KEY_DATABASE, hide_env_values = true)]
    pub database: String,
}

impl MetadataStoreOpt {
    pub async fn connect(&self) -> orfail::Result<mlmd::MetadataStore> {
        mlmd_connect(&self.database).await.or_fail()
    }
}
