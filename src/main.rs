//! Money Transfer — saga pattern with durable compensation.
//!
//! A workflow that moves funds between two accounts in a SQLite ledger.
//!
//! The transfer runs as two checkpointed steps: debit the source, credit
//! the target. If the credit fails, a compensating debit-reversal runs to
//! restore the source balance — the saga pattern, but written as straight-
//! line code because Resonate makes the steps durable.
//!
//! Each step is idempotent: it inserts a ledger row keyed by a deterministic
//! operation id, so replaying the workflow after a crash never double-applies
//! an entry.

use std::sync::Mutex;

use resonate::prelude::*;
use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};

const DB_PATH: &str = "./transfers.db";

// ── Ledger setup ────────────────────────────────────────────────────────

/// Open the SQLite ledger and create the transfers table if needed.
fn setup_database(path: &str) -> Connection {
    let conn = Connection::open(path).expect("open sqlite ledger");
    conn.execute(
        "CREATE TABLE IF NOT EXISTS transfers (
            uuid       TEXT PRIMARY KEY,
            account    TEXT NOT NULL,
            amount     REAL NOT NULL,
            note       TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )",
        [],
    )
    .expect("create transfers table");
    conn
}

/// Read a balance directly from the ledger (used by the demo driver).
fn get_balance(conn: &Connection, account: &str) -> f64 {
    conn.query_row(
        "SELECT COALESCE(SUM(amount), 0) FROM transfers WHERE account = ?1",
        params![account],
        |row| row.get::<_, f64>(0),
    )
    .unwrap_or(0.0)
}

// ── Ledger operations (durable leaf functions) ──────────────────────────

#[derive(Serialize, Deserialize, Debug, Clone)]
struct EntryArgs {
    op_id: String,
    account: String,
    amount: f64,
    note: String,
}

/// Apply a single ledger entry. Idempotent on `op_id`.
///
/// The `INSERT OR IGNORE` clause means a replay of this step after a crash
/// is a no-op — the row is already there, the balance is already correct.
#[resonate::function]
async fn apply_entry(info: &Info, args: EntryArgs) -> Result<String> {
    let db = info.get_dependency::<Mutex<Connection>>();
    let conn = db.lock().map_err(|e| Error::Application {
        message: format!("db lock poisoned: {e}"),
    })?;

    let rows = conn
        .execute(
            "INSERT OR IGNORE INTO transfers (uuid, account, amount, note) VALUES (?1, ?2, ?3, ?4)",
            params![args.op_id, args.account, args.amount, args.note],
        )
        .map_err(|e| Error::Application {
            message: format!("ledger insert failed: {e}"),
        })?;

    if rows == 0 {
        println!("  [ledger] {} already applied (idempotent no-op)", args.op_id);
    } else {
        let sign = if args.amount >= 0.0 { "+" } else { "" };
        println!(
            "  [ledger] {}: {} {}{}  // {}",
            args.op_id, args.account, sign, args.amount, args.note
        );
    }

    Ok(args.op_id)
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct CreditArgs {
    op_id: String,
    target: String,
    amount: f64,
    fail: bool,
}

/// Credit the target account. Pass `fail = true` to simulate a failure.
///
/// On `fail`, returns an `Error::Application` so the workflow can catch
/// it and run the compensating reversal.
#[resonate::function]
async fn credit_target(info: &Info, args: CreditArgs) -> Result<String> {
    if args.fail {
        return Err(Error::Application {
            message: format!("target account {:?} rejected the credit", args.target),
        });
    }

    let db = info.get_dependency::<Mutex<Connection>>();
    let conn = db.lock().map_err(|e| Error::Application {
        message: format!("db lock poisoned: {e}"),
    })?;

    let rows = conn
        .execute(
            "INSERT OR IGNORE INTO transfers (uuid, account, amount, note) VALUES (?1, ?2, ?3, ?4)",
            params![args.op_id, args.target, args.amount, "credit"],
        )
        .map_err(|e| Error::Application {
            message: format!("ledger insert failed: {e}"),
        })?;

    if rows == 0 {
        println!("  [ledger] {} already applied (idempotent no-op)", args.op_id);
    } else {
        println!(
            "  [ledger] {}: {} +{}  // credit",
            args.op_id, args.target, args.amount
        );
    }

    Ok(args.op_id)
}

// ── The saga workflow ──────────────────────────────────────────────────

#[derive(Serialize, Deserialize, Debug)]
struct TransferArgs {
    transfer_id: String,
    source: String,
    target: String,
    amount: f64,
    simulate_credit_failure: bool,
}

#[derive(Serialize, Deserialize, Debug)]
struct TransferResult {
    transfer_id: String,
    status: String,
    error: Option<String>,
}

/// Move `amount` from `source` to `target` as a saga.
///
/// Steps:
///   1. Debit the source account.
///   2. Credit the target account.
///   3. If (2) fails, run a compensating debit-reversal on the source.
///
/// Each step is durable. If the worker crashes after step (1) but before
/// step (2), Resonate replays the workflow, sees the source debit is
/// already in the ledger (idempotent insert), and continues from there.
#[resonate::function]
async fn transfer_money(ctx: &Context, args: TransferArgs) -> Result<TransferResult> {
    println!(
        "\n[saga] transfer {}: {} -> {}  ${}",
        args.transfer_id, args.source, args.target, args.amount
    );

    let debit_id = format!("{}-debit", args.transfer_id);
    let credit_id = format!("{}-credit", args.transfer_id);
    let reversal_id = format!("{}-reversal", args.transfer_id);

    // Step 1 — debit the source (durable checkpoint).
    ctx.run(
        apply_entry,
        EntryArgs {
            op_id: debit_id,
            account: args.source.clone(),
            amount: -args.amount,
            note: "debit".to_string(),
        },
    )
    .await?;

    // Step 2 — credit the target (durable checkpoint). On failure,
    // compensate by reversing the debit, then return an aborted result
    // so the caller sees the saga did not commit.
    //
    // This saga's compensation IS the response to a credit-side failure.
    // In production you might allow a few retries first (network blips
    // happen) and only compensate once the upstream has clearly rejected
    // the credit.
    let credit_outcome = ctx
        .run(
            credit_target,
            CreditArgs {
                op_id: credit_id,
                target: args.target.clone(),
                amount: args.amount,
                fail: args.simulate_credit_failure,
            },
        )
        .await;

    if let Err(err) = credit_outcome {
        println!("[saga] credit failed: {err}. Compensating...");

        // Compensating action — also durable + idempotent.
        ctx.run(
            apply_entry,
            EntryArgs {
                op_id: reversal_id,
                account: args.source.clone(),
                amount: args.amount,
                note: "reversal".to_string(),
            },
        )
        .await?;

        return Ok(TransferResult {
            transfer_id: args.transfer_id,
            status: "compensated".to_string(),
            error: Some(err.to_string()),
        });
    }

    println!("[saga] transfer {} committed", args.transfer_id);
    Ok(TransferResult {
        transfer_id: args.transfer_id,
        status: "committed".to_string(),
        error: None,
    })
}

// ── Demo driver ────────────────────────────────────────────────────────

#[tokio::main]
async fn main() {
    // Open the ledger (this connection is handed to Resonate as a dependency).
    let workflow_db = setup_database(DB_PATH);
    workflow_db
        .execute(
            "INSERT OR IGNORE INTO transfers (uuid, account, amount, note) VALUES (?1, ?2, ?3, ?4)",
            params!["seed-alice", "alice", 200.0_f64, "seed"],
        )
        .expect("seed alice");

    // A separate read-only handle so the demo driver can print balances
    // after the workflows have run. SQLite handles concurrent connections
    // to the same file fine.
    let read_db = Connection::open(DB_PATH).expect("open read connection");

    // Local mode — no Resonate Server required to run the demo.
    // For a server-backed deployment, replace `Resonate::local()` with
    // `Resonate::new(ResonateConfig { url: Some(..), .. })` and run
    // `resonate dev` alongside this process.
    let resonate = Resonate::local().with_dependency(Mutex::new(workflow_db));

    resonate.register(transfer_money).expect("register transfer_money");
    resonate.register(apply_entry).expect("register apply_entry");
    resonate.register(credit_target).expect("register credit_target");

    println!(
        "opening balances: alice={} bob={}",
        get_balance(&read_db, "alice"),
        get_balance(&read_db, "bob"),
    );

    // --- happy path -----------------------------------------------------
    let result: TransferResult = resonate
        .run(
            "transfer-001",
            transfer_money,
            TransferArgs {
                transfer_id: "transfer-001".to_string(),
                source: "alice".to_string(),
                target: "bob".to_string(),
                amount: 50.0,
                simulate_credit_failure: false,
            },
        )
        .await
        .expect("happy path transfer failed");
    println!("result: {result:?}");

    // --- failure path: credit rejected, saga compensates ----------------
    let result: TransferResult = resonate
        .run(
            "transfer-002",
            transfer_money,
            TransferArgs {
                transfer_id: "transfer-002".to_string(),
                source: "alice".to_string(),
                target: "bob".to_string(),
                amount: 75.0,
                simulate_credit_failure: true,
            },
        )
        .await
        .expect("compensated transfer should still resolve");
    println!("result: {result:?}");

    println!(
        "\nclosing balances: alice={} bob={}",
        get_balance(&read_db, "alice"),
        get_balance(&read_db, "bob"),
    );
    println!("(transfer-002 was compensated, so alice ends at 200 - 50 = 150)");
}
