use std::env;

use tokio::sync::mpsc::channel;
use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::Sender;
use tokio_stream::wrappers::ReceiverStream;

mod engine;
mod reader;

use engine::PaymentsEngine;
use engine::Tx;

use reader::fetch_csv_data;
const CHANNEL_SIZE: usize = 10000;

#[tokio::main]
async fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() > 2 {
        eprintln!("Too many arguments.")
    } else if let Some(filename) = args.get(1) {
        let (sender, receiver): (Sender<Tx>, Receiver<Tx>) = channel(CHANNEL_SIZE);
        let receiver = ReceiverStream::new(receiver);
        let filename = filename.to_string();
        let mut engine = PaymentsEngine::new(receiver);
        tokio::spawn(async move {
            if let Err(e) = engine.process_txs().await {
                eprintln!("Error processing txs: {}", e);
                return;
            };
            engine.print_report()
        });
        if let Err(e) = fetch_csv_data(&filename, sender).await {
            eprintln!("Error fetching csv data {}", e)
        }
    } else {
        eprintln!("Expected to receive a file name as input")
    }
}
