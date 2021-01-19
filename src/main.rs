use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(rename_all = "kebab-case")]
enum Opt {
    Dashboard,
    Get(mog::cli::get::GetOpt),
    Run,
}

#[async_std::main]
async fn main() -> anyhow::Result<()> {
    let opt = Opt::from_args();
    match opt {
        Opt::Dashboard => todo!(),
        Opt::Get(opt) => opt.execute().await?,
        Opt::Run => todo!(),
    }
    Ok(())
}
