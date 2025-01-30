use crate::{
    decimal::PositiveDecimal,
    types::{ClientId, TxnEvent, TxnEventDetail, TxnId},
};
use dashmap::DashMap;
use itertools::Itertools;
use rust_decimal::{Decimal, RoundingStrategy};
use serde::{Serialize, Serializer};
use std::collections::HashMap;
use tracing::debug;

/// Async Account store.
/// Switching to async in anticipation of realistic implementations that persist/lookup externally.
#[async_trait::async_trait]
pub trait AccStore: Send + Sync {
    async fn deposit(&self, client_id: ClientId, txn_id: TxnId, amount: PositiveDecimal);

    async fn withdraw(&self, client_id: ClientId, txn_id: TxnId, amount: PositiveDecimal);

    async fn dispute(&self, client_id: ClientId, txn_id: TxnId);

    async fn resolve(&self, client_id: ClientId, txn_id: TxnId);

    async fn chargeback(&self, client_id: ClientId, txn_id: TxnId);

    async fn snapshots(&self) -> Vec<ClientAccountSnapshot>;

    async fn add_event(&self, event: TxnEvent) {
        match event.detail {
            TxnEventDetail::Deposit { amount } => {
                self.deposit(event.client_id, event.txn_id, amount).await
            }

            TxnEventDetail::Withdrawal { amount } => {
                self.withdraw(event.client_id, event.txn_id, amount).await
            }

            TxnEventDetail::Dispute => self.dispute(event.client_id, event.txn_id).await,

            TxnEventDetail::Resolve => self.resolve(event.client_id, event.txn_id).await,

            TxnEventDetail::Chargeback => self.chargeback(event.client_id, event.txn_id).await,
        }
    }
}

#[derive(Default)]
pub struct InMemoryAccStore {
    accs: DashMap<ClientId, Account>,
}

#[async_trait::async_trait]
impl AccStore for InMemoryAccStore {
    /// Deposits into the account, allowed even if locked.
    /// Note: repeats of the same client/tx will overwrite!
    async fn deposit(&self, client_id: ClientId, txn_id: TxnId, amount: PositiveDecimal) {
        let mut acc = self.accs.entry(client_id).or_default();
        acc.txns.insert(
            txn_id,
            Txn {
                txn_type: TxnType::Deposit,
                amount: *amount,
            },
        );
        acc.snapshot.available += *amount;
    }

    /// Withdrawals from account, disallowed for locked account.
    /// Note: repeats of the same client/tx will overwrite!
    async fn withdraw(&self, client_id: ClientId, txn_id: TxnId, amount: PositiveDecimal) {
        self.accs.entry(client_id).and_modify(|acc| {
            if !acc.snapshot.locked {
                if acc.snapshot.available >= *amount {
                    acc.txns.insert(
                        txn_id,
                        Txn {
                            txn_type: TxnType::Withdrawal,
                            amount: *amount,
                        },
                    );
                    acc.snapshot.available -= *amount;
                } else {
                    debug!(txn_id, "Ignoring withdrawal due to insufficient funds")
                }
            } else {
                debug!(txn_id, "Ignoring withdrawal for locked account")
            }
        });
    }

    async fn dispute(&self, client_id: ClientId, txn_id: TxnId) {
        self.accs.entry(client_id).and_modify(|acc| {
            if !acc.snapshot.locked {
                if let Some(txn) = acc.txns.remove(&txn_id) {
                    let amount = txn.type_adjusted_amount();
                    acc.snapshot.held += amount;
                    acc.snapshot.available -= amount;
                    acc.held_txns.insert(txn_id, txn);
                } else {
                    debug!(txn_id, "Ignoring dispute for non-existent transaction")
                }
            } else {
                debug!(txn_id, "Ignoring dispute for locked account")
            }
        });
    }

    async fn resolve(&self, client_id: ClientId, txn_id: TxnId) {
        self.accs.entry(client_id).and_modify(|acc| {
            if !acc.snapshot.locked {
                if let Some(txn) = acc.held_txns.remove(&txn_id) {
                    let amount = txn.type_adjusted_amount();
                    acc.snapshot.held -= amount;
                    acc.snapshot.available += amount;
                    acc.txns.insert(txn_id, txn);
                } else {
                    debug!(txn_id, "Ignoring resolve for non-existent transaction")
                }
            } else {
                debug!(txn_id, "Ignoring resolve for locked account")
            }
        });
    }

    async fn chargeback(&self, client_id: ClientId, txn_id: TxnId) {
        self.accs.entry(client_id).and_modify(|acc| {
            if !acc.snapshot.locked {
                if let Some(txn) = acc.held_txns.remove(&txn_id) {
                    let amount = txn.type_adjusted_amount();
                    acc.snapshot.held -= amount;
                    acc.snapshot.locked = true;
                } else {
                    debug!(txn_id, "Ignoring chargeback for non-existent transaction")
                }
            } else {
                debug!(txn_id, "Ignoring chargeback for locked account")
            }
        });
    }

    async fn snapshots(&self) -> Vec<ClientAccountSnapshot> {
        self.accs
            .iter()
            .map(|entry| {
                let client_id = *entry.key();
                let acc = entry.value();
                ClientAccountSnapshot {
                    client_id,
                    available: acc.snapshot.available,
                    held: acc.snapshot.held,
                    locked: acc.snapshot.locked,
                    total: acc.snapshot.available + acc.snapshot.held,
                }
            })
            .sorted_unstable_by_key(|x| x.client_id)
            .collect()
    }
}

#[derive(Debug)]
pub enum TxnType {
    Deposit,
    Withdrawal,
}

/// Transaction maintained for disputes.
#[derive(Debug)]
pub struct Txn {
    txn_type: TxnType,
    amount: Decimal,
}

impl Txn {
    pub fn type_adjusted_amount(&self) -> Decimal {
        match self.txn_type {
            TxnType::Deposit => self.amount,
            TxnType::Withdrawal => -self.amount,
        }
    }
}

/// AccountSnapshot summarizes an account at a given point in time.
/// Note: available and held can be -ve in case of dispute involving withdrawals
#[derive(Serialize, Default, Debug, Eq, PartialEq, Clone)]
pub struct AccountSnapshot {
    available: Decimal,
    held: Decimal,
    locked: bool,
}

/// Note: `available` | `held` | `total` can be -ve in case of dispute involving withdrawals
#[derive(Serialize, Debug, Eq, PartialEq)]
pub struct ClientAccountSnapshot {
    #[serde(rename = "client")]
    client_id: ClientId,
    #[serde(serialize_with = "serialize_decimal_4_places")]
    available: Decimal,
    #[serde(serialize_with = "serialize_decimal_4_places")]
    held: Decimal,
    #[serde(serialize_with = "serialize_decimal_4_places")]
    total: Decimal,
    locked: bool,
}

#[derive(Default, Debug)]
struct Account {
    txns: HashMap<TxnId, Txn>,
    held_txns: HashMap<TxnId, Txn>,
    snapshot: AccountSnapshot,
}

fn serialize_decimal_4_places<S>(value: &Decimal, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let mut scaled = value.round_dp_with_strategy(4, RoundingStrategy::MidpointAwayFromZero);
    if scaled.fract().is_zero() {
        scaled = scaled.trunc()
    };
    serializer.serialize_str(&scaled.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::{test::add_csv_events_to_accs, to_csv_string};
    use itertools::Itertools;
    use rust_decimal_macros::dec;

    #[test]
    pub fn test_snapshot_4_decimal_places() {
        let snapshot = ClientAccountSnapshot {
            client_id: 1,
            available: dec!(1.234549), // Note: more than 4 decimal places
            held: dec!(0.0000499),     // Note: more than 4 decimal places
            total: dec!(1.23461779),
            locked: false,
        };
        assert_eq!(
            "client,available,held,total,locked
1,1.2345,0,1.2346,false",
            to_csv_string(&[snapshot]).unwrap()
        );
    }

    #[tokio::test]
    async fn test_deposit() {
        let mut accs = InMemoryAccStore::default();
        let events_csv = "type,client,tx,amount
deposit,1,101,100.456789";

        assert_eq!(
            add_csv_events_to_accs(&mut accs, events_csv).await.unwrap(),
            "client,available,held,total,locked
1,100.4568,0,100.4568,false"
        );
    }

    #[tokio::test]
    async fn test_withdrawal() {
        let mut accs = InMemoryAccStore::default();
        let events_csv = "type,client,tx,amount
deposit,1,101,100.456789
withdrawal,1,102,100
";

        assert_eq!(
            add_csv_events_to_accs(&mut accs, events_csv).await.unwrap(),
            "client,available,held,total,locked
1,0.4568,0,0.4568,false"
        );
    }

    #[tokio::test]
    async fn test_dispute_resolve() {
        let mut accs = InMemoryAccStore::default();
        let events_csv = "type,client,tx,amount
deposit,1,101,100
deposit,1,102,20";

        assert_eq!(
            add_csv_events_to_accs(&mut accs, events_csv).await.unwrap(),
            "client,available,held,total,locked
1,120,0,120,false"
        );

        let events_csv = "type,client,tx,amount
dispute,1,102,";

        assert_eq!(
            add_csv_events_to_accs(&mut accs, events_csv).await.unwrap(),
            "client,available,held,total,locked
1,100,20,120,false"
        );

        let events_csv = "type,client,tx,amount
resolve,1,102,";

        assert_eq!(
            add_csv_events_to_accs(&mut accs, events_csv).await.unwrap(),
            "client,available,held,total,locked
1,120,0,120,false"
        );
    }

    #[tokio::test]
    async fn test_dispute_resolve_withdrawal() {
        let mut accs = InMemoryAccStore::default();
        let events_csv = "type,client,tx,amount
deposit,1,101,100
withdrawal,1,102,20";

        assert_eq!(
            add_csv_events_to_accs(&mut accs, events_csv).await.unwrap(),
            "client,available,held,total,locked
1,80,0,80,false"
        );

        let events_csv = "type,client,tx,amount
dispute,1,102,
";

        assert_eq!(
            add_csv_events_to_accs(&mut accs, events_csv).await.unwrap(),
            "client,available,held,total,locked
1,100,-20,80,false"
        );

        let events_csv = "type,client,tx,amount
resolve,1,102,";

        assert_eq!(
            add_csv_events_to_accs(&mut accs, events_csv).await.unwrap(),
            "client,available,held,total,locked
1,80,0,80,false"
        );
    }

    /// Tests dispute, chargeback, locking of non deposit transactions
    #[tokio::test]
    async fn test_dispute_chargeback() {
        let mut accs = InMemoryAccStore::default();
        let events_csv = "type,client,tx,amount
deposit,1,101,100
deposit,1,102,20";

        assert_eq!(
            add_csv_events_to_accs(&mut accs, events_csv).await.unwrap(),
            "client,available,held,total,locked
1,120,0,120,false"
        );

        let events_csv = "type,client,tx,amount
dispute,1,102,";

        assert_eq!(
            add_csv_events_to_accs(&mut accs, events_csv).await.unwrap(),
            "client,available,held,total,locked
1,100,20,120,false"
        );

        let events_csv = "type,client,tx,amount
chargeback,1,102,";

        assert_eq!(
            add_csv_events_to_accs(&mut accs, events_csv).await.unwrap(),
            "client,available,held,total,locked
1,100,0,100,true"
        );

        let events_csv = "type,client,tx,amount
deposit,1,103,111
withdrawal,1,103,11";

        assert_eq!(
            add_csv_events_to_accs(&mut accs, events_csv).await.unwrap(),
            "client,available,held,total,locked
1,211,0,211,true"
        );
    }

    #[tokio::test]
    async fn test_multi_client() {
        let mut accs = InMemoryAccStore::default();
        let events_csv = "type,client,tx,amount
deposit,1,101,1000
deposit,2,102,100
deposit,3,103,10
withdrawal,1,201,100
withdrawal,2,202,10
withdrawal,3,203,1
";

        assert_eq!(
            add_csv_events_to_accs(&mut accs, events_csv).await.unwrap(),
            "client,available,held,total,locked
1,900,0,900,false
2,90,0,90,false
3,9,0,9,false"
        );
    }

    #[tokio::test]
    async fn test_invalid_records() {
        let mut accs = InMemoryAccStore::default();
        let events_csv = "type,client,tx,amount
deposit,1,101,
deposit,1,102,20,
deposit,1,abc,def
__BOGUS__,1,103,3";

        assert!(add_csv_events_to_accs(&mut accs, events_csv)
            .await
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn test_large_csv_feed() {
        let mut accs = InMemoryAccStore::default();

        let events_csv = vec![
            "type,client,tx,amount",
            "deposit,1,101,123.45",
            "deposit,2,102,77.89",
            "withdrawal,2,103,67.89", // to be charged back
            "__BOGUS__,1,2,3",        // ignored due to invalid type
            "deposit,1,104,123.45",
            "dispute,1,101,",
            "resolve,1,101,",
            "withdrawal,1,105,46.90",
            "deposit,3,3,-5",    // invalid, amount cannot be -ve
            "withdrawal,3,3,-5", // invalid, amount cannot be -ve
        ]
        .into_iter()
        .join("\n");
        assert_eq!(
            add_csv_events_to_accs(&mut accs, &events_csv)
                .await
                .unwrap(),
            "client,available,held,total,locked
1,200,0,200,false
2,10,0,10,false"
        );

        // add few more deposits/withdrawals
        let events_csv = "type,client,tx,amount
withdrawal,2,106,10
deposit,1,107,100";
        assert_eq!(
            add_csv_events_to_accs(&mut accs, events_csv).await.unwrap(),
            "client,available,held,total,locked
1,300,0,300,false
2,0,0,0,false"
        );

        let events_csv = vec![
            "type,client,tx,amount",
            "dispute,2,102,",
            "chargeback,2,102,",      // lock client 2
            "withdrawal,2,105,10000", // will be ignored due to lock
        ]
        .into_iter()
        .join("\n");
        assert_eq!(
            add_csv_events_to_accs(&mut accs, &events_csv)
                .await
                .unwrap(),
            "client,available,held,total,locked
1,300,0,300,false
2,-77.89,0,-77.89,true"
        );

        // ascertain held is populated
        let events_csv = "type,client,tx,amount
deposit,1,201,50
deposit,1,202,60
dispute,1,201,
dispute,1,202,";
        assert_eq!(
            add_csv_events_to_accs(&mut accs, events_csv).await.unwrap(),
            "client,available,held,total,locked
1,300,110,410,false
2,-77.89,0,-77.89,true"
        );

        // ascertain held is added to available on resolve
        let events_csv = "type,client,tx,amount
resolve,1,202,";
        assert_eq!(
            add_csv_events_to_accs(&mut accs, events_csv).await.unwrap(),
            "client,available,held,total,locked
1,360,50,410,false
2,-77.89,0,-77.89,true"
        );

        // ascertain held is depleted on chargeback
        let events_csv = "type,client,tx,amount
chargeback,1,201,";
        assert_eq!(
            add_csv_events_to_accs(&mut accs, events_csv).await.unwrap(),
            "client,available,held,total,locked
1,360,0,360,true
2,-77.89,0,-77.89,true"
        );
    }
}
