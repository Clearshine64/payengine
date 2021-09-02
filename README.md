# payengine
A toy payment processing engine.

Takes an input a CSV of transactions and outputs a list of client account data.
The supported transactions are Deposit, Withdrawal, Resolve, Dispute, and Chargeback.

The functionality relating to reading the CSV and updating client data is split into separate modules, "reader" and "engine", so one could easily input from a CSV file by , say, input from TCP streams.

The engine module can be readily adapted for in concurrent situations.

Precision of at least four decimal places is guaranteed by the use of the rust_decimal crate.

Note:
- Any transactions on a frozen account will be ignored.
- A withdrawal with amount greater than a client's available funds will be ignored.
# payengine
