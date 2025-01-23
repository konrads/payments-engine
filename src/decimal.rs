use rust_decimal::Decimal;
use serde::de;
use serde::{Deserialize, Deserializer};
use std::ops::Deref;

/// Positive only decimal, restricted to numbers > 0,
/// Does not expose mutable references to the inner value, to avoid opportunity to change inner to < 0
#[derive(Debug, PartialEq, Eq)]
pub struct PositiveDecimal(Decimal);

impl<'de> Deserialize<'de> for PositiveDecimal {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = <Decimal as Deserialize>::deserialize(deserializer)?;
        value.try_into().map_err(de::Error::custom)
    }
}

impl AsRef<Decimal> for PositiveDecimal {
    fn as_ref(&self) -> &Decimal {
        &self.0
    }
}

impl Deref for PositiveDecimal {
    type Target = Decimal;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl TryInto<PositiveDecimal> for Decimal {
    type Error = anyhow::Error;

    fn try_into(self) -> Result<PositiveDecimal, Self::Error> {
        if self.is_sign_positive() && !self.is_zero() {
            Ok(PositiveDecimal(self))
        } else {
            anyhow::bail!("value must be positive and non-zero")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn test_deserialize_ok() {
        let res: PositiveDecimal = serde_json::from_value(serde_json::json!("1.23")).unwrap();
        assert_eq!(res, PositiveDecimal(dec!(1.23)));
    }

    #[test]
    fn deserialize_positive_decimal_fail() {
        let res: Result<PositiveDecimal, _> = serde_json::from_value(serde_json::json!("-1"));
        assert!(res.is_err());
        let res: Result<PositiveDecimal, _> = serde_json::from_value(serde_json::json!("0"));
        assert!(res.is_err());
    }
}
