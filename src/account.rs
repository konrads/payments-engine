use crate::{
    txn::Txn,
    types::{ClientId, TxnId},
};
use rust_decimal::{Decimal, RoundingStrategy};
use serde::{Serialize, Serializer};
use std::collections::HashMap;

#[derive(Default, Debug)]
pub struct Account {
    pub txns: HashMap<TxnId, Txn>,
    pub held_txns: HashMap<TxnId, Txn>,
    pub available: Decimal,
    pub held: Decimal,
    pub locked: bool,
}
/// AccountSnapshot summarizes an account at a given point in time.
/// Note: available and held can be -ve in case of dispute involving withdrawals
#[derive(Serialize, Debug, Eq, PartialEq)]
pub struct AccountSnapshot {
    #[serde(rename = "client")]
    pub client_id: ClientId,
    #[serde(serialize_with = "serialize_decimal_4_places")]
    pub available: Decimal,
    #[serde(serialize_with = "serialize_decimal_4_places")]
    pub held: Decimal,
    #[serde(serialize_with = "serialize_decimal_4_places")]
    pub total: Decimal,
    pub locked: bool,
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
    use crate::util::to_csv_string;
    use rust_decimal_macros::dec;

    #[test]
    fn test_snapshot_4_decimal_places() {
        let snapshot = AccountSnapshot {
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
}
