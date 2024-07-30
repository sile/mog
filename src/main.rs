use orfail::OrFail;
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(rename_all = "kebab-case")]
enum Opt {
    Dashboard,
    Get(mog::cli::get::GetOpt),
    Run(mog::cli::run::RunOpt),
}

#[tokio::main]
async fn main() -> orfail::Result<()> {
    let opt = Opt::from_args();
    match opt {
        Opt::Dashboard => todo!(),
        Opt::Get(opt) => opt.execute().await.or_fail()?,
        Opt::Run(opt) => opt.execute().await.or_fail()?,
    }
    Ok(())
}
