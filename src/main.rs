use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(rename_all = "kebab-case")]
enum Opt {
    Dashboard,
    Get(mog::cli::get::GetOpt),
    Run,
}

fn main() -> anyhow::Result<()> {
    let opt = Opt::from_args();
    Ok(())
}
