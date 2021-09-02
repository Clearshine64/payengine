use std::path::Path;

use anyhow::Context;
use serde::Deserialize;
use tokio::sync::mpsc::Sender;

use super::engine::Amount;
use super::engine::ClientId;
use super::engine::Tx;
use super::engine::TxId;
use super::engine::TxInner;

pub async fn fetch_csv_data(filename: impl AsRef<Path>, sender: Sender<Tx>) -> anyhow::Result<()> {
    let mut csv_reader = csv::Reader::from_path(filename)?;
    for record in csv_reader.records() {
        let mut record = record.context("getting CSV Record")?;
        // We trim whitespaces so that serde will be able to Deserialize
        // our record into a Tx struct
        csv::StringRecord::trim(&mut record);
        let parsed_tx = record
            .deserialize(None)
            .context("Deserializing record into Tx")?;
        let tx = FromParsedTx::from_parsed(parsed_tx)?;

        sender.send(tx).await?;
    }
    Ok(())
}

#[derive(Debug, PartialEq, Deserialize)]
#[serde(rename_all = "snake_case")]
enum TxType {
    Deposit,
    Withdrawal,
    Dispute,
    Resolve,
    Chargeback,
}

#[derive(Debug, PartialEq, Deserialize)]
struct ParsedTx {
    #[serde(rename = "type")]
    tx_type: TxType,
    #[serde(rename = "client")]
    client_id: ClientId,
    #[serde(rename = "tx")]
    tx_id: TxId,
    amount: Option<Amount>,
}

trait FromParsedTx {
    fn from_parsed(tx: ParsedTx) -> anyhow::Result<Self>
    where
        Self: std::marker::Sized;
}

impl FromParsedTx for Tx {
    fn from_parsed(tx: ParsedTx) -> anyhow::Result<Self> {
        let inner = match tx.tx_type {
            TxType::Withdrawal => {
                let amount = tx.amount.ok_or_else(|| {
                    anyhow::anyhow!("transaction {} is a withdrawal without an amount", tx.tx_id)
                })?;
                TxInner::Withdrawal { amount }
            }
            TxType::Deposit => {
                let amount = tx.amount.ok_or_else(|| {
                    anyhow::anyhow!("transaction {} is a withdrawal without an amount", tx.tx_id)
                })?;
                TxInner::Deposit { amount }
            }
            TxType::Chargeback => TxInner::Chargeback,
            TxType::Dispute => TxInner::Dispute,
            TxType::Resolve => TxInner::Resolve,
        };
        Ok(Self {
            client_id: tx.client_id,
            tx_id: tx.tx_id,
            inner,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn deserialize_record() {
        let record = csv::StringRecord::from(vec!["deposit", "1", "1", "1.0"]);
        let transaction: ParsedTx = record.deserialize(None).expect("failed to deserialize");
        assert_eq!(
            transaction,
            ParsedTx {
                tx_type: TxType::Deposit,
                client_id: 1,
                tx_id: 1,
                amount: Some(1.0)
            }
        )
    }
}
