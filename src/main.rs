use futures::stream::StreamExt;
use payments_engine::{
    account::{AccStore, InMemoryAccStore},
    util::{read_csv_file, to_csv_string},
};
use tracing::warn;
use tracing_subscriber::EnvFilter;

/// Main entry point, sets up logger, fetches arguments, crates `AccStore`, reads in transaction events and adds them to the `AccStore`.
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or(EnvFilter::new("info")))
        .with_writer(std::io::stderr)
        .init();

    let args = std::env::args().collect::<Vec<String>>();
    if args.len() != 2 {
        anyhow::bail!("Usage: {} <input.csv>", args[0]);
    }
    let input_filename = &args[1];

    // Pluggable AccStore reference
    let acc_store: &mut dyn AccStore = &mut InMemoryAccStore::default();

    let mut input_stream = read_csv_file(tokio::fs::File::open(input_filename).await?).await;
    while let Some(event) = input_stream.next().await {
        match event {
            Ok(event) => acc_store.add_event(event).await,
            Err(err) => warn!(?err, "Error processing event"), // Note: skipping errors
        }
    }

    let snapshots = acc_store.snapshots().await;
    println!("{}", to_csv_string(&snapshots)?);
    Ok(())
}
