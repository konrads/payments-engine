use crate::{
    account::{Account, AccountSnapshot},
    decimal::PositiveDecimal,
    txn::{Txn, TxnType},
    types::{ClientId, TxnEvent, TxnEventDetail, TxnId},
};
use dashmap::DashMap;
use itertools::Itertools;

/// Async Account store.
/// Switching to async in anticipation of realistic implementations that persist/lookup externally.
#[async_trait::async_trait]
pub trait PaymentEngine: Send + Sync {
    async fn deposit(
        &self,
        client_id: ClientId,
        txn_id: TxnId,
        amount: PositiveDecimal,
    ) -> anyhow::Result<()>;

    async fn withdraw(
        &self,
        client_id: ClientId,
        txn_id: TxnId,
        amount: PositiveDecimal,
    ) -> anyhow::Result<()>;

    async fn dispute(&self, client_id: ClientId, txn_id: TxnId) -> anyhow::Result<()>;

    async fn resolve(&self, client_id: ClientId, txn_id: TxnId) -> anyhow::Result<()>;

    async fn chargeback(&self, client_id: ClientId, txn_id: TxnId) -> anyhow::Result<()>;

    async fn snapshots(&self) -> anyhow::Result<Vec<AccountSnapshot>>;

    async fn add_event(&self, event: TxnEvent) -> anyhow::Result<()> {
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
pub struct InMemoryPaymentEngine {
    accs: DashMap<ClientId, Account>,
}

#[async_trait::async_trait]
impl PaymentEngine for InMemoryPaymentEngine {
    /// Deposits into the account, allowed even if locked.
    /// Note: repeats of the same client/tx will overwrite!
    async fn deposit(
        &self,
        client_id: ClientId,
        txn_id: TxnId,
        amount: PositiveDecimal,
    ) -> anyhow::Result<()> {
        let mut acc = self.accs.entry(client_id).or_default();
        acc.txns.insert(
            txn_id,
            Txn {
                txn_type: TxnType::Deposit,
                amount: *amount,
            },
        );
        acc.available += *amount;
        Ok(())
    }

    /// Withdrawals from account, disallowed for locked account.
    /// Note: repeats of the same client/tx will overwrite!
    async fn withdraw(
        &self,
        client_id: ClientId,
        txn_id: TxnId,
        amount: PositiveDecimal,
    ) -> anyhow::Result<()> {
        if let Some(mut acc) = self.accs.get_mut(&client_id) {
            if !acc.locked {
                if acc.available >= *amount {
                    acc.txns.insert(
                        txn_id,
                        Txn {
                            txn_type: TxnType::Withdrawal,
                            amount: *amount,
                        },
                    );
                    acc.available -= *amount;
                    Ok(())
                } else {
                    anyhow::bail!("Cannot withdraw due to insufficient funds")
                }
            } else {
                anyhow::bail!("Cannot withdraw for locked account")
            }
        } else {
            anyhow::bail!("Cannot withdraw from non-existent account")
        }
    }

    async fn dispute(&self, client_id: ClientId, txn_id: TxnId) -> anyhow::Result<()> {
        if let Some(mut acc) = self.accs.get_mut(&client_id) {
            if !acc.locked {
                if let Some(txn) = acc.txns.remove(&txn_id) {
                    let amount = txn.type_adjusted_amount();
                    acc.held += amount;
                    acc.available -= amount;
                    acc.held_txns.insert(txn_id, txn);
                    Ok(())
                } else {
                    anyhow::bail!("Cannot dispute non-existent transaction")
                }
            } else {
                anyhow::bail!("Cannot dispute locked account")
            }
        } else {
            anyhow::bail!("Cannot dispute non-existent account")
        }
    }

    async fn resolve(&self, client_id: ClientId, txn_id: TxnId) -> anyhow::Result<()> {
        if let Some(mut acc) = self.accs.get_mut(&client_id) {
            if !acc.locked {
                if let Some(txn) = acc.held_txns.remove(&txn_id) {
                    let amount = txn.type_adjusted_amount();
                    acc.held -= amount;
                    acc.available += amount;
                    acc.txns.insert(txn_id, txn);
                    Ok(())
                } else {
                    anyhow::bail!("Cannot resolve non-disputed transaction")
                }
            } else {
                anyhow::bail!("Cannot resolve locked account")
            }
        } else {
            anyhow::bail!("Cannot resolve non-existent account")
        }
    }

    async fn chargeback(&self, client_id: ClientId, txn_id: TxnId) -> anyhow::Result<()> {
        if let Some(mut acc) = self.accs.get_mut(&client_id) {
            if !acc.locked {
                if let Some(txn) = acc.held_txns.remove(&txn_id) {
                    let amount = txn.type_adjusted_amount();
                    acc.held -= amount;
                    acc.locked = true;
                    Ok(())
                } else {
                    anyhow::bail!("Cannot chargeback non-disputed transaction")
                }
            } else {
                anyhow::bail!("Cannot chargeback locked account")
            }
        } else {
            anyhow::bail!("Cannot chargeback non-existent account")
        }
    }

    async fn snapshots(&self) -> anyhow::Result<Vec<AccountSnapshot>> {
        let snapshots = self
            .accs
            .iter()
            .map(|entry| {
                let client_id = *entry.key();
                let acc = entry.value();
                AccountSnapshot {
                    client_id,
                    available: acc.available,
                    held: acc.held,
                    locked: acc.locked,
                    total: acc.available + acc.held,
                }
            })
            .sorted_unstable_by_key(|x| x.client_id)
            .collect();
        Ok(snapshots)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::test::add_csv_events_to_engine;
    use itertools::Itertools;

    #[tokio::test]
    async fn test_deposit() {
        let mut engine = InMemoryPaymentEngine::default();
        let events_csv = "type,client,tx,amount
deposit,1,101,100.456789";

        assert_eq!(
            add_csv_events_to_engine(&mut engine, events_csv)
                .await
                .unwrap(),
            "client,available,held,total,locked
1,100.4568,0,100.4568,false"
        );
    }

    #[tokio::test]
    async fn test_withdrawal() {
        let mut engine = InMemoryPaymentEngine::default();
        let events_csv = "type,client,tx,amount
deposit,1,101,100.456789
withdrawal,1,102,100
";

        assert_eq!(
            add_csv_events_to_engine(&mut engine, events_csv)
                .await
                .unwrap(),
            "client,available,held,total,locked
1,0.4568,0,0.4568,false"
        );
    }

    #[tokio::test]
    async fn test_dispute_resolve() {
        let mut engine = InMemoryPaymentEngine::default();
        let events_csv = "type,client,tx,amount
deposit,1,101,100
deposit,1,102,20";

        assert_eq!(
            add_csv_events_to_engine(&mut engine, events_csv)
                .await
                .unwrap(),
            "client,available,held,total,locked
1,120,0,120,false"
        );

        let events_csv = "type,client,tx,amount
dispute,1,102,";

        assert_eq!(
            add_csv_events_to_engine(&mut engine, events_csv)
                .await
                .unwrap(),
            "client,available,held,total,locked
1,100,20,120,false"
        );

        let events_csv = "type,client,tx,amount
resolve,1,102,";

        assert_eq!(
            add_csv_events_to_engine(&mut engine, events_csv)
                .await
                .unwrap(),
            "client,available,held,total,locked
1,120,0,120,false"
        );
    }

    #[tokio::test]
    async fn test_dispute_resolve_withdrawal() {
        let mut engine = InMemoryPaymentEngine::default();
        let events_csv = "type,client,tx,amount
deposit,1,101,100
withdrawal,1,102,20";

        assert_eq!(
            add_csv_events_to_engine(&mut engine, events_csv)
                .await
                .unwrap(),
            "client,available,held,total,locked
1,80,0,80,false"
        );

        let events_csv = "type,client,tx,amount
dispute,1,102,";

        assert_eq!(
            add_csv_events_to_engine(&mut engine, events_csv)
                .await
                .unwrap(),
            "client,available,held,total,locked
1,100,-20,80,false"
        );

        let events_csv = "type,client,tx,amount
resolve,1,102,";

        assert_eq!(
            add_csv_events_to_engine(&mut engine, events_csv)
                .await
                .unwrap(),
            "client,available,held,total,locked
1,80,0,80,false"
        );
    }

    /// Tests dispute, chargeback, locking of non deposit transactions
    #[tokio::test]
    async fn test_dispute_chargeback() {
        let mut engine = InMemoryPaymentEngine::default();
        let events_csv = "type,client,tx,amount
deposit,1,101,100
deposit,1,102,20";

        assert_eq!(
            add_csv_events_to_engine(&mut engine, events_csv)
                .await
                .unwrap(),
            "client,available,held,total,locked
1,120,0,120,false"
        );

        let events_csv = "type,client,tx,amount
dispute,1,102,";

        assert_eq!(
            add_csv_events_to_engine(&mut engine, events_csv)
                .await
                .unwrap(),
            "client,available,held,total,locked
1,100,20,120,false"
        );

        let events_csv = "type,client,tx,amount
chargeback,1,102,";

        assert_eq!(
            add_csv_events_to_engine(&mut engine, events_csv)
                .await
                .unwrap(),
            "client,available,held,total,locked
1,100,0,100,true"
        );

        let events_csv = "type,client,tx,amount
deposit,1,103,111
withdrawal,1,103,11";

        assert_eq!(
            add_csv_events_to_engine(&mut engine, events_csv)
                .await
                .unwrap(),
            "client,available,held,total,locked
1,211,0,211,true"
        );
    }

    #[tokio::test]
    async fn test_multi_client() {
        let mut engine = InMemoryPaymentEngine::default();
        let events_csv = "type,client,tx,amount
deposit,1,101,1000
deposit,2,102,100
deposit,3,103,10
withdrawal,1,201,100
withdrawal,2,202,10
withdrawal,3,203,1
";

        assert_eq!(
            add_csv_events_to_engine(&mut engine, events_csv)
                .await
                .unwrap(),
            "client,available,held,total,locked
1,900,0,900,false
2,90,0,90,false
3,9,0,9,false"
        );
    }

    #[tokio::test]
    async fn test_invalid_records() {
        let mut engine = InMemoryPaymentEngine::default();
        let events_csv = "type,client,tx,amount
deposit,1,101,
deposit,1,102,20,
deposit,1,abc,def
__BOGUS__,1,103,3";

        assert!(add_csv_events_to_engine(&mut engine, events_csv)
            .await
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn test_large_csv_feed() {
        let mut engine = InMemoryPaymentEngine::default();

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
            add_csv_events_to_engine(&mut engine, &events_csv)
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
            add_csv_events_to_engine(&mut engine, events_csv)
                .await
                .unwrap(),
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
            add_csv_events_to_engine(&mut engine, &events_csv)
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
            add_csv_events_to_engine(&mut engine, events_csv)
                .await
                .unwrap(),
            "client,available,held,total,locked
1,300,110,410,false
2,-77.89,0,-77.89,true"
        );

        // ascertain held is added to available on resolve
        let events_csv = "type,client,tx,amount
resolve,1,202,";
        assert_eq!(
            add_csv_events_to_engine(&mut engine, events_csv)
                .await
                .unwrap(),
            "client,available,held,total,locked
1,360,50,410,false
2,-77.89,0,-77.89,true"
        );

        // ascertain held is depleted on chargeback
        let events_csv = "type,client,tx,amount
chargeback,1,201,";
        assert_eq!(
            add_csv_events_to_engine(&mut engine, events_csv)
                .await
                .unwrap(),
            "client,available,held,total,locked
1,360,0,360,true
2,-77.89,0,-77.89,true"
        );
    }
}
