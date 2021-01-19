use anyhow::Context;

pub async fn mlmd_connect(database_uri: &str) -> anyhow::Result<mlmd::MetadataStore> {
    let store = mlmd::MetadataStore::connect(database_uri)
        .await
        .with_context(|| format!("cannot connect to the database: {:?}", database_uri))?;
    Ok(store)
}
