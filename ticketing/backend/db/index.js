const path = require("path");
const Database = require("better-sqlite3");

const dbPath = path.join(__dirname, "tickets.db");
const db = new Database(dbPath);

db.exec(`
  CREATE TABLE IF NOT EXISTS tickets (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    email TEXT NOT NULL,
    ticket_type TEXT NOT NULL,
    amount_cents INTEGER NOT NULL,
    ticket_id TEXT NOT NULL UNIQUE,
    status TEXT NOT NULL DEFAULT 'not_checked_in',
    payment_id TEXT NOT NULL UNIQUE,
    checkin_url TEXT NOT NULL,
    qr_code_data_url TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    checked_in_at TEXT
  );

  CREATE INDEX IF NOT EXISTS idx_tickets_ticket_id ON tickets(ticket_id);
  CREATE INDEX IF NOT EXISTS idx_tickets_email ON tickets(email);
`);

module.exports = db;
