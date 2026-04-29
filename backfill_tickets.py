import os
import requests
import sqlite3

SQUARE_TOKEN = os.getenv("SQUARE_ACCESS_TOKEN")
LOCATION_ID = os.getenv("SQUARE_LOCATION_ID")

DB_PATH = os.getenv(
    "DATABASE_PATH",
    os.path.join(os.path.dirname(__file__), "database.db"),
)

headers = {
    "Authorization": f"Bearer {SQUARE_TOKEN}",
    "Content-Type": "application/json"
}


def fetch_payments():
    url = "https://connect.squareup.com/v2/payments"
    params = {
        "location_id": LOCATION_ID,
        "limit": 100
    }

    r = requests.get(url, headers=headers, params=params)
    data = r.json()

    return data.get("payments", [])


def ticket_exists(cursor, payment_id):
    cursor.execute("SELECT id FROM event_tickets WHERE payment_id = ?", (payment_id,))
    return cursor.fetchone() is not None


def main():
    if not os.path.exists(DB_PATH):
        raise Exception(f"Database not found: {DB_PATH}")

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    print("[DB TABLES]", cursor.fetchall())
    cursor.execute("SELECT name FROM sqlite_master WHERE name='event_tickets'")
    if not cursor.fetchone():
        raise Exception("event_tickets table not found in selected DB")

    payments = fetch_payments()

    print(f"[SYNC] Found {len(payments)} payments")

    for p in payments:
        payment_id = p.get("id")
        status = p.get("status")
        amount = p.get("amount_money", {}).get("amount", 0)
        email = p.get("buyer_email_address")

        if status != "COMPLETED":
            continue

        if ticket_exists(cursor, payment_id):
            print(f"[SKIP] Already exists: {payment_id}")
            continue

        print(f"[DRY RUN] Would create ticket for {payment_id} | {email} | {amount}")

    conn.close()
    print("[DONE] Backfill complete")


if __name__ == "__main__":
    main()
