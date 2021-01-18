#[derive(Debug, structopt::StructOpt)]
#[structopt(rename_all = "kebab-case")]
pub enum GetOpt {
    Artifacts,
    Contexts,
    Events,
    Executions,
}
