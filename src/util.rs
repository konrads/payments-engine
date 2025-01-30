use crate::types::TxnEvent;
use csv::{ReaderBuilder, Trim, WriterBuilder};
use serde::Serialize;
use std::fs::File;

// Read in CSV file, return an Iterator<Item=Result<TxnEvent>>
pub fn read_csv_file(file: File) -> impl Iterator<Item = csv::Result<TxnEvent>> {
    let reader = ReaderBuilder::new()
        .has_headers(true)
        .trim(Trim::All)
        .from_reader(file);
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
    use crate::account::AccStore;

    pub fn read_csv_contents(
        contents: &str,
    ) -> impl Iterator<Item = csv::Result<TxnEvent>> + use<'_> {
        let reader = ReaderBuilder::new()
            .has_headers(true)
            .trim(Trim::All)
            .from_reader(contents.as_bytes());
        reader.into_deserialize::<TxnEvent>()
    }

    pub fn add_csv_events_to_accs<TS: AccStore>(
        accs: &mut TS,
        contents: &str,
    ) -> anyhow::Result<String> {
        for event in read_csv_contents(contents).filter_map(|e| {
            e.map_err(|err| {
                println!("failed to parse: {err:?}");
                err
            })
            .ok()
        }) {
            accs.add_event(event);
        }
        to_csv_string(&accs.snapshots())
    }
}
