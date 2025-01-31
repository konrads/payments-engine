use futures::stream::StreamExt;
use payments_engine::{
    payment_engine::{InMemoryPaymentEngine, PaymentEngine},
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
    let engine: &mut dyn PaymentEngine = &mut InMemoryPaymentEngine::default();

    let input_stream = read_csv_file(tokio::fs::File::open(input_filename).await?).await;
    let mut combined_input_stream = futures::stream::select_all(vec![
        input_stream,
        // input streams from other sources, eg. TCP
    ]);

    while let Some(event) = combined_input_stream.next().await {
        match event {
            Ok(event) => {
                if let Err(err) = engine.add_event(event).await {
                    warn!(?err, "Error processing event") // Note: skipping errors
                }
            }
            Err(err) => warn!(?err, "Error reading event"), // Note: skipping errors
        }
    }

    let snapshots = engine.snapshots().await?;
    println!("{}", to_csv_string(&snapshots)?);
    Ok(())
}
