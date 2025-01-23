use crate::decimal::PositiveDecimal;
use serde::{Deserialize, Deserializer};

/// User friendly type aliases
pub type ClientId = u16;
pub type TxnId = u32;

#[derive(Debug, Eq, PartialEq)]
pub struct TxnEvent {
    pub client_id: ClientId,
    pub txn_id: TxnId,
    pub detail: TxnEventDetail,
}

#[derive(Debug, Eq, PartialEq)]
pub enum TxnEventDetail {
    Deposit { amount: PositiveDecimal },
    Withdrawal { amount: PositiveDecimal },
    Dispute,
    Resolve,
    Chargeback,
}

/// Deserialize for TxnEvent, enforcing semantics for every transaction
impl<'de> Deserialize<'de> for TxnEvent {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize, Debug)]
        #[serde(rename_all = "lowercase")]
        enum TxnEventType {
            Deposit,
            Withdrawal,
            Dispute,
            Resolve,
            Chargeback,
        }

        #[derive(Deserialize, Debug)]
        #[serde(rename_all = "lowercase")]
        struct TxnEventPrivate {
            r#type: TxnEventType,
            #[serde(rename = "client")]
            client_id: ClientId,
            #[serde(rename = "tx")]
            txn_id: TxnId,
            amount: Option<PositiveDecimal>,
        }

        let event = TxnEventPrivate::deserialize(deserializer)?;

        let detail = match event.r#type {
            TxnEventType::Deposit => {
                let amount = event
                    .amount
                    .ok_or(serde::de::Error::missing_field("amount"))?;
                Ok(TxnEventDetail::Deposit { amount })
            }
            TxnEventType::Withdrawal => {
                let amount = event
                    .amount
                    .ok_or(serde::de::Error::missing_field("amount"))?;
                Ok(TxnEventDetail::Withdrawal { amount })
            }
            TxnEventType::Dispute => Ok(TxnEventDetail::Dispute),
            TxnEventType::Resolve => Ok(TxnEventDetail::Resolve),
            TxnEventType::Chargeback => Ok(TxnEventDetail::Chargeback),
        }?;
        Ok(TxnEvent {
            client_id: event.client_id,
            txn_id: event.txn_id,
            detail,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::test::read_csv_contents;
    use rust_decimal_macros::dec;

    #[test]
    fn test_deserialize_ok() -> anyhow::Result<()> {
        let events = read_csv_contents(
            "type,client,tx,amount
deposit,1,101,123.45
withdrawal,2,102,67.89
dispute,1,101,
dispute,2,102,
resolve,1,101,
chargeback,2,102,",
        )
        .collect::<Result<Vec<TxnEvent>, _>>()?;

        assert_eq!(
            vec![
                TxnEvent {
                    client_id: 1,
                    txn_id: 101,
                    detail: TxnEventDetail::Deposit {
                        amount: dec!(123.45).try_into()?,
                    },
                },
                TxnEvent {
                    client_id: 2,
                    txn_id: 102,
                    detail: TxnEventDetail::Withdrawal {
                        amount: dec!(67.89).try_into()?,
                    }
                },
                TxnEvent {
                    client_id: 1,
                    txn_id: 101,
                    detail: TxnEventDetail::Dispute
                },
                TxnEvent {
                    client_id: 2,
                    txn_id: 102,
                    detail: TxnEventDetail::Dispute
                },
                TxnEvent {
                    client_id: 1,
                    txn_id: 101,
                    detail: TxnEventDetail::Resolve
                },
                TxnEvent {
                    client_id: 2,
                    txn_id: 102,
                    detail: TxnEventDetail::Chargeback
                },
            ],
            events
        );
        Ok(())
    }

    #[test]
    fn test_deserialize_err_no_headers() {
        let res = read_csv_contents(
            "bogus_headers
deposit,1,101,123.45",
        )
        .collect::<Result<Vec<TxnEvent>, _>>();
        assert!(res
            .unwrap_err()
            .to_string()
            .contains("found record with 4 fields, but the previous record has 1 fields"));
    }

    #[test]
    fn test_deserialize_negative_available() {
        let res = read_csv_contents(
            "type,client,tx,amount
deposit,1,101,-123.45",
        )
        .collect::<Result<Vec<TxnEvent>, _>>();
        assert!(res
            .unwrap_err()
            .to_string()
            .contains("value must be positive and non-zero"));
    }

    #[test]
    fn test_deserialize_invalid_type() {
        let res = read_csv_contents(
            "type,client,tx,amount
BOGUS_TYPE,1,101,123.45",
        )
        .collect::<Result<Vec<TxnEvent>, _>>();
        assert!(res.unwrap_err().to_string().contains("BOGUS_TYPE"));
    }
}
