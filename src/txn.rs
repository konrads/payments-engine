use rust_decimal::Decimal;

#[derive(Debug)]
pub enum TxnType {
    Deposit,
    Withdrawal,
}

/// Transaction maintained for disputes.
#[derive(Debug)]
pub struct Txn {
    pub txn_type: TxnType,
    pub amount: Decimal,
}

impl Txn {
    pub fn type_adjusted_amount(&self) -> Decimal {
        match self.txn_type {
            TxnType::Deposit => self.amount,
            TxnType::Withdrawal => -self.amount,
        }
    }
}
