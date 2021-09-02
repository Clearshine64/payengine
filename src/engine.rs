use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

use parking_lot::Mutex;

use rust_decimal_macros::dec;
use tokio_stream::StreamExt;

pub type ClientId = u16;
pub type TxId = u32;
pub type Amount = rust_decimal::Decimal;

struct ClientAccount {
    available: Amount,
    held: Amount,
    locked: bool,
}

impl ClientAccount {
    fn new() -> Self {
        Self {
            locked: false,
            held: dec!(0.0),
            available: dec!(0.0),
        }
    }

    fn deposit(&mut self, amount: Amount) {
        self.available += amount;
    }

    fn withdrawal(&mut self, amount: Amount) {
        self.available -= amount;
    }

    fn dispute(&mut self, amount: Amount) {
        self.available -= amount;
        self.held += amount;
    }

    fn resolve(&mut self, amount: Amount) {
        self.available += amount;
        self.held -= amount;
    }

    fn chargeback(&mut self, amount: Amount) {
        self.available -= amount;
        self.held -= amount;
        self.locked = true;
    }
}

#[derive(Debug, PartialEq)]
pub struct Tx {
    pub client_id: ClientId,
    pub tx_id: TxId,
    pub inner: TxInner,
}

#[derive(Debug, PartialEq)]
pub enum TxInner {
    Deposit { amount: Amount },
    Withdrawal { amount: Amount },
    Dispute,
    Resolve,
    Chargeback,
}

#[derive(Default)]
pub struct PaymentsEngine<T> {
    client_accounts: HashMap<ClientId, Arc<Mutex<ClientAccount>>>,
    done_txs: Arc<Mutex<HashMap<TxId, Tx>>>,
    disputed_txs: Arc<Mutex<HashSet<TxId>>>,
    input_source: T,
}

impl<T: StreamExt<Item = Tx> + std::marker::Unpin> PaymentsEngine<T> {
    pub fn new(input_source: T) -> Self {
        Self {
            client_accounts: HashMap::default(),
            input_source,
            done_txs: Arc::new(Mutex::new(HashMap::default())),
            disputed_txs: Arc::new(Mutex::new(HashSet::default())),
        }
    }

    pub fn print_report(&self) {
        Self::print_header();
        for (id, account) in &self.client_accounts {
            let account = account.lock();
            println!(
                "{},{},{},{},{}",
                id,
                account.available,
                account.held,
                account.available + account.held,
                account.locked
            );
        }
    }

    pub async fn process_txs(&mut self) -> anyhow::Result<()> {
        while let Some(tx) = self.input_source.next().await {
            self.update(tx)?
        }
        Ok(())
    }

    fn print_header() {
        println!("client,available,held,total,locked");
    }

    fn update(&mut self, tx: Tx) -> anyhow::Result<()> {
        if self.can_process_tx(&tx) {
            self.update_client_accounts(&tx);
            self.update_tx_history(tx)
        } else {
            Ok(())
        }
    }

    fn can_process_tx(&self, tx: &Tx) -> bool {
        !self.client_account_frozen(tx) && self.sufficient_funds(tx)
    }

    fn client_account_frozen(&self, tx: &Tx) -> bool {
        matches!(self.client_accounts.get(&tx.client_id),
            Some(account) if account.lock().locked)
    }

    fn sufficient_funds(&self, tx: &Tx) -> bool {
        if let TxInner::Withdrawal { amount } = tx.inner {
            match self.client_accounts.get(&tx.client_id) {
                Some(account) => account.lock().available >= amount,
                None => false,
            }
        } else {
            true
        }
    }

    fn update_tx_history(&mut self, tx: Tx) -> anyhow::Result<()> {
        if let std::collections::hash_map::Entry::Vacant(e) = self.done_txs.lock().entry(tx.tx_id) {
            e.insert(tx);
            Ok(())
        } else {
            Err(anyhow::anyhow!("tx_id {} already exists!"))
        }
    }

    fn dispute(&mut self, tx: &Tx) {
        // We don't throw errors if something goes wrong
        // Simply ignore the dispute
        if let Some(disputed_tx) = self.done_txs.lock().get(&tx.tx_id) {
            if let Some(amount) = match disputed_tx.inner {
                TxInner::Withdrawal { amount } => Some(amount),
                TxInner::Deposit { amount } => Some(amount),
                _ => None,
            } {
                if let Some(client) = self.client_accounts.get_mut(&tx.client_id) {
                    client.lock().dispute(amount);
                    self.disputed_txs.lock().insert(tx.tx_id);
                }
            }
        }
    }

    fn deposit(&mut self, client_id: ClientId, amount: Amount) {
        self.client_accounts
            .entry(client_id)
            .or_insert_with(|| Arc::new(Mutex::new(ClientAccount::new())))
            .lock()
            .deposit(amount)
    }

    fn withdrawal(&mut self, client_id: ClientId, amount: Amount) {
        self.client_accounts
            .entry(client_id)
            .or_insert_with(|| Arc::new(Mutex::new(ClientAccount::new())))
            .lock()
            .withdrawal(amount)
    }

    fn resolve(&mut self, tx: &Tx) {
        if let Some(disputed_tx) = self.done_txs.lock().get(&tx.tx_id) {
            if let Some(amount) = match disputed_tx.inner {
                TxInner::Withdrawal { amount } => Some(amount),
                TxInner::Deposit { amount } => Some(amount),
                _ => None,
            } {
                if let Some(client) = self.client_accounts.get_mut(&tx.client_id) {
                    client.lock().resolve(amount);
                    self.disputed_txs.lock().remove(&tx.tx_id);
                }
            }
        }
    }

    fn chargeback(&mut self, tx: &Tx) {
        if let Some(disputed_tx) = self.done_txs.lock().get(&tx.tx_id) {
            if let Some(amount) = match disputed_tx.inner {
                TxInner::Withdrawal { amount } => Some(amount),
                TxInner::Deposit { amount } => Some(amount),
                _ => None,
            } {
                if let Some(client) = self.client_accounts.get_mut(&tx.client_id) {
                    client.lock().chargeback(amount);
                }
            }
        }
    }

    fn update_client_accounts(&mut self, tx: &Tx) {
        match tx.inner {
            TxInner::Deposit { amount } => self.deposit(tx.client_id, amount),
            TxInner::Withdrawal { amount } => self.withdrawal(tx.client_id, amount),
            TxInner::Dispute => self.dispute(tx),
            TxInner::Resolve => self.resolve(tx),
            TxInner::Chargeback => self.chargeback(tx),
        }
    }
}
