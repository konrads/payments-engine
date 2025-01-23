use payments_engine::{
    account::{AccStore, InMemoryAccStore},
    util::{read_csv_file, to_csv_string},
};
use std::fs::File;
use tracing::warn;
use tracing_subscriber::EnvFilter;

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or(EnvFilter::new("info")))
        .with_writer(std::io::stderr)
        .init();

    let args = std::env::args().collect::<Vec<String>>();

    if args.len() != 2 {
        anyhow::bail!("Usage: {} <input.csv>", args[0]);
    }

    // Pluggable AccStore reference
    let acc_store: &mut dyn AccStore = &mut InMemoryAccStore::default();
    for event in read_csv_file(File::open(&args[1])?) {
        match event {
            Ok(event) => acc_store.add_event(event),
            Err(err) => warn!(?err, "Error processing event"), // Note: skipping errors
        }
    }

    let snapshots = acc_store.snapshots();
    println!("{}", to_csv_string(&snapshots)?);
    Ok(())
}
