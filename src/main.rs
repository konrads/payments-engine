use payments_engine::{
    payment_engine::{InMemoryPaymentEngine, PaymentEngine},
    util::{read_csv_file, to_csv_string},
};
use std::fs::File;
use tracing::warn;
use tracing_subscriber::EnvFilter;

/// Main entry point, sets up logger, fetches arguments, creates `PaymentEngine`, reads in transaction events and adds them to the `PaymentEngine`.
fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or(EnvFilter::new("info")))
        .with_writer(std::io::stderr)
        .init();

    let args = std::env::args().collect::<Vec<String>>();
    if args.len() != 2 {
        anyhow::bail!("Usage: {} <input.csv>", args[0]);
    }
    let input_filename = &args[1];

    // Pluggable PaymentEngine reference
    let engine: &mut dyn PaymentEngine = &mut InMemoryPaymentEngine::default();
    for event in read_csv_file(File::open(input_filename)?) {
        match event {
            Ok(event) => {
                if let Err(err) = engine.add_event(event) {
                    warn!(?err, "Error processing event") // Note: skipping errors
                }
            }
            Err(err) => warn!(?err, "Error reading event"), // Note: skipping errors
        }
    }

    let snapshots = engine.snapshots()?;
    println!("{}", to_csv_string(&snapshots)?);
    Ok(())
}
