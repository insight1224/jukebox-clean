import os
import requests
import sqlite3
import secrets
import urllib.parse

ACCESS_TOKEN = os.getenv("SQUARE_ACCESS_TOKEN")
DRY_RUN = False

DB_PATH = os.getenv(
    "DATABASE_PATH",
    os.path.join(os.path.dirname(__file__), "database.db"),
)

def fetch_payments():
    url = "https://connect.squareup.com/v2/payments"
    headers = {
        "Authorization": f"Bearer {ACCESS_TOKEN}",
        "Content-Type": "application/json"
    }
    params = {
        "sort_order": "DESC",
        "limit": 100
    }
    response = requests.get(url, headers=headers, params=params)
    data = response.json()
    print("[RAW RESPONSE]", data)
    payments = data.get("payments", [])
    print(f"[SYNC] Found {len(payments)} payments")
    return payments


def ticket_exists(cursor, payment_id):
    cursor.execute("SELECT id FROM event_tickets WHERE payment_id = ?", (payment_id,))
    return cursor.fetchone() is not None


def insert_ticket(cursor, payment_id, email, amount):
    ticket_id = f"TICKET_{secrets.token_hex(8).upper()}"
    checkin_url = f"{os.getenv('CHECKIN_BASE_URL', 'http://localhost:5050').rstrip('/')}/checkin/{ticket_id}"
    qr_url = "https://api.qrserver.com/v1/create-qr-code/?size=260x260&data=" + urllib.parse.quote(checkin_url, safe="")
    cursor.execute(
        """
        INSERT INTO event_tickets (
            name, email, ticket_type, amount_cents, ticket_id, status, payment_id, checkin_url, qr_url
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        ("Guest", (email or "no-email@example.com").lower(), "General Admission", int(amount or 0), ticket_id, "not_checked_in", payment_id, checkin_url, qr_url),
    )
    return ticket_id


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

    for payment in payments:
        print("\n--- PAYMENT ---")
        print("ID:", payment.get("id"))
        print("STATUS:", payment.get("status"))
        print("AMOUNT:", payment.get("amount_money", {}).get("amount"))
        print("EMAIL:", payment.get("buyer_email_address"))

        payment_id = payment.get("id")
        status = payment.get("status")
        amount = payment.get("amount_money", {}).get("amount", 0)
        email = payment.get("buyer_email_address")

        if status != "COMPLETED":
            print("SKIPPING PAYMENT:", payment_id, "| Reason: not completed")
            continue

        print("Attempting ticket mapping...")
        mapped_ticket = "General Admission"
        print("Mapped ticket:", mapped_ticket)

        print("Checking if ticket exists...")
        exists = ticket_exists(cursor, payment_id)
        print("Exists:", exists)
        if exists:
            print("SKIPPING PAYMENT:", payment_id, "| Reason: no mapping or already exists")
            continue

        print(f"[INSERT] Creating ticket for {payment_id}")
        insert_ticket(cursor, payment_id, email, amount)
        conn.commit()

    conn.close()
    print("[DONE] Backfill complete")


if __name__ == "__main__":
    main()
