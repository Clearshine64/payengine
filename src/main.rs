use std::collections::HashMap;
use std::env;
use std::path::Path;

use anyhow::Context;
use serde::Deserialize;

use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::Sender;
use tokio_stream::StreamExt;

type ClientId = u16;
type TxId = u32;

// TODO Warnings as errors

// TODO 4 decimal places
// TODO is this correct?
type Amount = f32;

#[derive(Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
enum TxType {
    Deposit,
    Withdrawal,
    Dispute,
    Chargeback,
    Resolve,
}

#[derive(Debug, PartialEq, Deserialize)]
struct Tx {
    #[serde(rename = "type")]
    tx_type: TxType,
    #[serde(rename = "client")]
    client_id: ClientId,
    #[serde(rename = "tx")]
    tx_id: TxId,
    amount: Amount,
}

async fn process_tx_stream(mut txs: impl StreamExt<Item = Tx> + std::marker::Unpin) {
    while let Some(tx) = txs.next().await {
        println!("process_tx_stream: processing {:?}", tx)
    }
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() > 2 {
        // FIXME stderr?
        println!("Too Many arguments")
    } else if let Some(filename) = args.get(1) {
        PaymentsEngine::new().process_txs_from_file(filename).await
    } else {
        // FIXME stderr?
        println!("expected one argument")
    }
}

async fn fetch_csv_data(filename: impl AsRef<Path>, sender: Sender<Tx>) -> anyhow::Result<()> {
    let mut csv_reader = csv::Reader::from_path(filename)?;
    for record in csv_reader.records() {
        let mut record = record.context("getting CSV Record")?;
        // We trim whitespaces so that serde will be able to Deserialize
        // our record into a Tx struct
        csv::StringRecord::trim(&mut record);
        let tx = record
            .deserialize(None)
            .context("Deserializing record into Tx")?;
        sender.send(tx).await?;
    }
    Ok(())
}

struct PaymentsEngine {
    clients: HashMap<ClientId, ClientAccount>,
}

impl PaymentsEngine {
    fn new() -> Self {
        Self {
            clients: HashMap::default(),
        }
    }

    async fn process_txs_from_file(mut self, filename: impl ToString) {
        const CHANNEL_SIZE: usize = 10000;
        let (sender, receiver): (Sender<Tx>, Receiver<Tx>) =
            tokio::sync::mpsc::channel(CHANNEL_SIZE);
        let receiver = tokio_stream::wrappers::ReceiverStream::new(receiver);
        let filename = filename.to_string();
        tokio::spawn(async move {
            if let Err(e) = fetch_csv_data(&filename, sender).await {
                println!("Error {}", e)
            }
        });
        process_tx_stream(receiver).await
    }
}

struct ClientAccount {
    available: Amount,
    held: Amount,
    total: Amount,
    locked: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn deserialize_record() {
        let record = csv::StringRecord::from(vec!["deposit", "1", "1", "1.0"]);
        let transaction: Tx = record.deserialize(None).expect("failed to deserialize");
        assert_eq!(
            transaction,
            Tx {
                tx_type: TxType::Deposit,
                client_id: 1,
                tx_id: 1,
                amount: 1.0
            }
        )
    }
}
