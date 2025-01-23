# Mutable transactions payments engine

[![build](../../workflows/build/badge.svg)](../../actions/workflows/build.yml)

Payment engine for mutable transactions, facilitating `deposits`, `withdrawals`, `disputes` with `resolve`/`chargeback` outcomes.

**For async implementation, checkout [async branch](../async)**

## Design

The design relies on a `AccStore` service, fronted by a trait of the mentioned name, and `InMemoryAccStore` implementation. The current implementation stores relevant information in-memory, but allows for extensions via new implementation, eg. stores that persist in disk/db/cache. Extensions might require changes to `AccStore` trait itself, to make it more async.

`InMemoryAccStore` accepts deserialized `TxnEvents`, persists transaction data and updates the client snapshots. Awareness of all transactions is required for disputes, but in-memory implementation is non scalable and subject to optimizations.

The current approach reads transactions from a file in a sync way, via `Iterator`.

Issues with ingested transactions are logged to stderr, whilst the snapshot output is pushed to stdout.

Type system is utilized as much as possible for structural integrity, eg. to ensure positive `amounts`, or to make sure that only `deposits`/`withdrawals` accept `amount` field.

## Usage

```sh
RUST_LOG=debug cargo run -- transactions.csv
```

## Assumptions

- Input feed expect the format to include trailing comma for transaction types which do not need the `amount` field: `dispute` | `resolve` | `chargeback`.

```
type,client,tx,amount
deposit,1,101,123.45
dispute,1,101,       -- trailing comma required
```

- transaction amounts are expected as positive decimals, otherwise warning will be logged and the record skipped
- the only accepted transaction on a locked account is `deposit`. Currently, there is no action to unlock an account
- account balances can become negative should a sufficiently large `deposit` is disputed
- multiple transactions of same id are not supported

## Testing

- scenario based testing that accepts csv transaction input and produces csv snapshot output
- test of utils eg. `PositiveDecimal`'s deserialization
- manual testing via `RUST_LOG=debug cargo run -- transactions.csv`

## Error handling

In general, warnings/errors print to stderr at `debug` level, but allow the process to go on.

- `deposit`/`withdrawal` amounts <= 0 issue a warning and are skipped
- `deposits`/`withdrawals` must contain `amount` field, other transactions must not

## Potential optimizations

- switch to db/disk/cache implementation of `AccStore`, reducing the memory footprint
- consider more compact data types, eg. u64 for amounts (after adjustment by 4 decimal places) or `repr(packed)` (ensuring no misalignment issues: https://doc.rust-lang.org/nomicon/other-reprs.html#reprpacked)
- ingest transactions async
  - convert `Iterator` to `Stream`. Consider `futures::stream::select_all()` for joining multiple streams into 1
  - change `AccStore` methods to `async`
  - use `tokio` for runtime
  - consider usage of [dashmap](https://crates.io/crates/dashmap) for in-memory implementation
