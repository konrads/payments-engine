use crate::types::TxnEvent;
use csv::WriterBuilder;
use futures::stream::Stream;
use serde::Serialize;
use tokio_util::compat::TokioAsyncReadCompatExt;

// Read in CSV file, return a Stream<Item=Result<TxnEvent>>
pub async fn read_csv_file(
    file: tokio::fs::File,
) -> impl Stream<Item = Result<TxnEvent, csv_async::Error>> {
    let reader = csv_async::AsyncReaderBuilder::new()
        .has_headers(true)
        .trim(csv_async::Trim::All)
        .create_deserializer(file.compat());

    reader.into_deserialize::<TxnEvent>()
}

pub fn to_csv_string<T: Serialize>(values: &[T]) -> anyhow::Result<String> {
    let mut wtr = WriterBuilder::new().has_headers(true).from_writer(vec![]);
    for v in values {
        wtr.serialize(v)?;
    }
    Ok(String::from_utf8(wtr.into_inner()?)?.trim().to_owned())
}

// test helpers
#[cfg(test)]
pub mod test {
    use super::*;
    use crate::payment_engine::PaymentEngine;

    pub fn read_csv_contents(
        contents: &str,
    ) -> impl Iterator<Item = csv::Result<TxnEvent>> + use<'_> {
        let reader = csv::ReaderBuilder::new()
            .has_headers(true)
            .trim(csv::Trim::All)
            .from_reader(contents.as_bytes());
        reader.into_deserialize::<TxnEvent>()
    }

    pub async fn add_csv_events_to_engine<PE: PaymentEngine>(
        engine: &mut PE,
        contents: &str,
    ) -> anyhow::Result<String> {
        for event in read_csv_contents(contents).filter_map(|e| {
            e.map_err(|err| {
                println!("failed to parse: {err:?}");
                err
            })
            .ok()
        }) {
            if let Err(err) = engine.add_event(event).await {
                println!("failed to process: {err:?}");
            };
        }
        to_csv_string(&engine.snapshots().await?)
    }
}
