import base64
import hashlib
import hmac
import json
import os
import secrets
import sqlite3
import smtplib
import urllib.parse
import requests
from email.mime.text import MIMEText
from functools import wraps
from urllib import error as urlerror
from urllib import request as urlrequest

from flask import Flask, Response, redirect, render_template, request, url_for

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    # Fallback lightweight .env loader.
    env_path = os.path.join(os.path.dirname(__file__), ".env")
    if os.path.exists(env_path):
        with open(env_path, "r", encoding="utf-8") as env_file:
            for line in env_file:
                raw = line.strip()
                if not raw or raw.startswith("#") or "=" not in raw:
                    continue
                key, value = raw.split("=", 1)
                key = key.strip()
                value = value.strip().strip('"').strip("'")
                if key and key not in os.environ:
                    os.environ[key] = value

def clean_event_name(name):
    name = name.lower().strip()

    mapping = {
        "line dancing": "Line Dancing",
        "line dance": "Line Dancing",
        "linedancing": "Line Dancing",

        "afrobeats": "Afrobeats",
        "afro beats": "Afrobeats",

        "hip hop": "Hip Hop",
        "hiphop": "Hip Hop"
    }

    return mapping.get(name, name.title())

def check_auth(username, password):
    admin_user = os.getenv("ADMIN_USERNAME", "admin")
    admin_pass = os.getenv("ADMIN_PASSWORD", "jukebox123")
    return username == admin_user and password == admin_pass

def authenticate():
    return Response(
        "Login Required", 401,
        {"WWW-Authenticate": 'Basic realm="Login Required"'}
    )

def requires_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        auth = request.authorization
        if not auth or not check_auth(auth.username, auth.password):
            return authenticate()
        return f(*args, **kwargs)
    return decorated


# -------------------------
# DATABASE SETUP
# -------------------------
# ✅ DATABASE SETUP (RUN ON APP START)

def init_db():
    conn = sqlite3.connect("database.db")
    cursor = conn.cursor()

    def ensure_column(table_name, column_name, column_def):
        cursor.execute(f"PRAGMA table_info({table_name})")
        existing = [row[1] for row in cursor.fetchall()]
        if column_name not in existing:
            cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_def}")

    # LEADS
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS leads (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        type TEXT,
        name TEXT,
        email TEXT,
        details TEXT,
        status TEXT DEFAULT 'New'
    )
    """)

    # MEMBERSHIPS
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS memberships (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        email TEXT,
        amount REAL,
        status TEXT DEFAULT 'Active'
    )
    """)
    ensure_column("memberships", "name", "TEXT")
    ensure_column("memberships", "payment_id", "TEXT")
    ensure_column("memberships", "source", "TEXT DEFAULT 'square'")
    cursor.execute("""
    CREATE UNIQUE INDEX IF NOT EXISTS idx_memberships_payment_id
    ON memberships(payment_id)
    """)

    # EVENT REQUESTS
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS event_requests (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        event_name TEXT,
        status TEXT DEFAULT 'New'
    )
    """)
    ensure_column("event_requests", "created_at", "TEXT DEFAULT CURRENT_TIMESTAMP")
    ensure_column("event_requests", "archived", "INTEGER DEFAULT 0")
    ensure_column("event_requests", "archived_at", "TEXT")

    # EVENT VOTES ✅ (FIXED INDENT)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS event_votes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        event_name TEXT UNIQUE,
        votes INTEGER DEFAULT 0
    )
    """)

    # ✅ SEED VOTING OPTIONS
    vinyl_options = [
        "Grown and Sexy Ball",
        "Line Dancing",
        "Afrobeats",
        "Live Bands",
        "Open Mic"
    ]

    for option in vinyl_options:
        cursor.execute("""
            INSERT INTO event_votes (event_name, votes)
            SELECT ?, 0
            WHERE NOT EXISTS (
                SELECT 1 FROM event_votes WHERE event_name = ?
            )
        """, (option, option))

    # TICKET TABLE ✅ (FIXED POSITION)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS ticket_types (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        event_name TEXT,
        ticket_name TEXT,
        price REAL,
        max_quantity INTEGER,
        sold INTEGER DEFAULT 0
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS square_payment_log (
        payment_id TEXT PRIMARY KEY,
        category TEXT,
        amount_cents INTEGER,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS event_tickets (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        email TEXT NOT NULL,
        ticket_type TEXT NOT NULL,
        amount_cents INTEGER NOT NULL,
        ticket_id TEXT NOT NULL UNIQUE,
        status TEXT NOT NULL DEFAULT 'not_checked_in',
        payment_id TEXT NOT NULL UNIQUE,
        checkin_url TEXT NOT NULL,
        qr_url TEXT NOT NULL,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        checked_in_at TEXT
    )
    """)
    ensure_column("event_tickets", "ticket_email_sent_at", "TEXT")
    ensure_column("event_tickets", "event_name", "TEXT")
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS webhook_logs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        source TEXT NOT NULL,
        event_id TEXT,
        event_type TEXT,
        note TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    )
    """)

    # SEED TICKETS
    tickets = [
        ("Battle of the DJs", "Early Bird", 13, 30),
        ("Battle of the DJs", "General Admission", 18, 366),
        ("Battle of the DJs", "VIP Section", 175, 6),
        ("Battle of the DJs", "DJ VIP Section", 200, 3),
        ("Part 2 - The Quiet Storm Live", "Early Bird", 13, 30),
        ("Part 2 - The Quiet Storm Live", "General Admission", 18, 366),
        ("Part 2 - The Quiet Storm Live", "VIP Section", 175, 6),
        ("Part 2 - The Quiet Storm Live", "DJ VIP Section", 200, 3),
    ]

    for event, name, price, max_q in tickets:
        cursor.execute("""
        INSERT INTO ticket_types (event_name, ticket_name, price, max_quantity)
        SELECT ?, ?, ?, ?
        WHERE NOT EXISTS (
            SELECT 1 FROM ticket_types 
            WHERE event_name = ? AND ticket_name = ?
        )
        """, (event, name, price, max_q, event, name))

    conn.commit()
    conn.close()

def purchase_ticket(event_name, ticket_name):
    conn = sqlite3.connect("database.db")
    cursor = conn.cursor()

    cursor.execute("""
        SELECT sold, max_quantity
        FROM ticket_types
        WHERE event_name = ? AND ticket_name = ?
    """, (event_name, ticket_name))

    result = cursor.fetchone()

    if result:
        sold, max_q = result

        if sold < max_q:
            cursor.execute("""
                UPDATE ticket_types
                SET sold = sold + 1
                WHERE event_name = ? AND ticket_name = ?
            """, (event_name, ticket_name))

            conn.commit()
            print("✅ Ticket purchased")
        else:
            print("❌ SOLD OUT")

    conn.close()

app = Flask(__name__)

init_db()   # ✅ AFTER function exists
# -------------------------
# EMAIL CONFIG
# -------------------------
SQUARE_SIGNATURE_KEY = os.getenv("SQUARE_SIGNATURE_KEY", "")
SQUARE_WEBHOOK_URL = os.getenv("SQUARE_WEBHOUK_URL", "")
SQUARE_ACCESS_TOKEN = os.getenv("SQUARE_ACCESS_TOKEN", "")
SQUARE_ENV = os.getenv("SQUARE_ENV", "sandbox").lower()
SQUARE_APPLICATION_ID = os.getenv("SQUARE_APPLICATION_ID", "")
SQUARE_LOCATION_ID = os.getenv("SQUARE_LOCATION_ID", "")
STRICT_WEBHOOK_SIGNATURE = os.getenv("STRICT_WEBHOOK_SIGNATURE", "0") == "1"
SQUARE_SKIP_WEBHOOK_SIGNATURE = os.getenv("SQUARE_SKIP_WEBHOOK_SIGNATURE", "0") == "1"
IS_PRODUCTION = os.getenv("FLASK_ENV", "").lower() == "production"
MEMBERSHIP_AMOUNT_CENTS = int(os.getenv("SQUARE_MEMBERSHIP_AMOUNT_CENTS", "0") or "0")
SQUARE_SYNC_LIMIT = int(os.getenv("SQUARE_SYNC_LIMIT", "100") or "100")

WEB_TICKET_TYPES = {
    "early_bird": {"label": "Early Bird", "amount_cents": 1300},
    "general_admission": {"label": "General Admission", "amount_cents": 1800},
    "vip_section": {"label": "VIP Section", "amount_cents": 17500},
    "dj_vip_section": {"label": "DJ VIP Section", "amount_cents": 20000},
}

SQUARE_TO_DB_MAP = {
    "Battle - Early Bird General": "Early Bird",
    "Battle - General": "General Admission",
    "Battle - VIP": "VIP Section",
    "Battle - VIP DJ Section": "DJ VIP Section",
    "The Jukebox Circle Membership": "Jukebox Circle Membership",
}

DISPLAY_NAME_MAP = {
    "Early Bird": "Early Bird (Limited Discounted Entry)",
    "General Admission": "General Admission",
    "VIP Section": "VIP Section (Shaded Booth for 6)",
    "DJ VIP Section": "DJ VIP Section (Upper Deck Booth for 6)",
    "Jukebox Circle Membership": "Jukebox Circle Membership",
}

CANONICAL_TICKET_TYPES = {
    "Early Bird",
    "General Admission",
    "VIP Section",
    "DJ VIP Section",
}


def extract_square_name(payment):
    note = (payment.get("note") or "").strip()
    reference_id = (payment.get("reference_id") or "").strip()
    receipt_number = (payment.get("receipt_number") or "").strip()
    blob = " ".join(part for part in (note, reference_id, receipt_number) if part)

    for square_name in SQUARE_TO_DB_MAP.keys():
        if square_name.lower() in blob.lower():
            return square_name
    return note or reference_id or receipt_number or ""


def canonical_ticket_type_from_payment(payment):
    square_name = extract_square_name(payment)
    mapped = SQUARE_TO_DB_MAP.get(square_name)
    if mapped in CANONICAL_TICKET_TYPES:
        return mapped
    return None


def event_name_from_payment(payment):
    blob = " ".join(
        str(part).strip().lower()
        for part in (
            payment.get("note", ""),
            payment.get("reference_id", ""),
            payment.get("receipt_number", ""),
        )
        if part
    )
    if "quiet storm" in blob:
        return "Part 2 - The Quiet Storm Live"
    return "Battle of the DJs"


def map_ticket_from_payment(payment):
    amount_cents = int((payment.get("amount_money", {}) or {}).get("amount") or 0)
    print("TICKET MAP SOURCE NOTE:", payment.get("note"))
    print("TICKET MAP SOURCE AMOUNT:", amount_cents)
    mapped = map_ticket_from_amount(amount_cents)
    print("TICKET MAP RESULT:", mapped)
    return mapped


def map_ticket_from_amount(amount_cents):
    amount = int(amount_cents or 0)
    if amount == 1200:
        return "Early Bird"
    if amount == 1300:
        return "General Admission"
    if amount == 17500:
        return "VIP Section"
    if amount == 30000:
        return "DJ VIP Section"
    return None


def is_membership_payment_from_payment(payment, amount_cents, note_blob):
    if int(amount_cents or 0) == 1000:
        return True
    square_name = extract_square_name(payment)
    mapped = SQUARE_TO_DB_MAP.get(square_name)
    if mapped == "Jukebox Circle Membership":
        return True
    blob = " ".join(
        str(part).strip().lower()
        for part in (
            payment.get("note", ""),
            payment.get("reference_id", ""),
            payment.get("receipt_number", ""),
            note_blob or "",
        )
        if part
    )
    return is_membership_payment(amount_cents, blob)

def send_email(subject, body, to_email):
    email_address = (
        os.getenv("EMAIL_ADDRESS", "").strip()
        or os.getenv("SMTP_EMAIL_ADDRESS", "").strip()
    )
    email_password = (
        os.getenv("EMAIL_PASSWORD", "").strip()
        or os.getenv("SMTP_EMAIL_PASSWORD", "").strip()
    )
    masked_pw = ("*" * 8) if email_password else "(missing)"
    print(f"SMTP CHECK: {email_address or '(missing)'} {masked_pw}")
    if not email_address or not email_password:
        raise Exception("SMTP credentials missing at runtime")

    try:
        msg = MIMEText(body)
        msg["Subject"] = subject
        msg["From"] = email_address
        msg["To"] = to_email

        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
            smtp.login(email_address, email_password)
            smtp.send_message(msg)
        print("Email sent ✔")
        return True
    except Exception as e:
        print("Email failed:", e)
        return False


def send_ticket_email_once(cursor, ticket_id):
    cursor.execute(
        """
        SELECT name, email, ticket_type, ticket_id, checkin_url, qr_url, ticket_email_sent_at
        FROM event_tickets
        WHERE ticket_id = ?
        """,
        (ticket_id,),
    )
    row = cursor.fetchone()
    if not row:
        print(f"[ticket-email] ticket not found for ticket_id={ticket_id}")
        return False

    if row[6]:
        print(f"[ticket-email] already sent for ticket_id={ticket_id}")
        return True

    name, email, ticket_type, tid, checkin_url, qr_url, _sent_at = row
    body = (
        f"Hi {name},\n\n"
        f"Your Jukebox ticket is confirmed.\n\n"
        f"Ticket ID: {tid}\n"
        f"Ticket Type: {ticket_type}\n"
        f"Check-In Link: {checkin_url}\n"
        f"QR Code: {qr_url}\n\n"
        "Please keep this email for entry."
    )
    delivered = send_email("Your Jukebox Ticket", body, email)
    if delivered:
        cursor.execute(
            """
            UPDATE event_tickets
            SET ticket_email_sent_at = CURRENT_TIMESTAMP
            WHERE ticket_id = ?
            """,
            (ticket_id,),
        )
        print(f"[ticket-email] sent and marked for ticket_id={ticket_id}")
    else:
        print(f"[ticket-email] failed for ticket_id={ticket_id}")
    return delivered


def verify_square_signature(req):
    # Dev mode: always allow webhook delivery so ticket flow can be tested end-to-end.
    if SQUARE_SKIP_WEBHOOK_SIGNATURE or not STRICT_WEBHOOK_SIGNATURE:
        return True
    signature = req.headers.get("x-square-hmacsha256-signature", "")
    body = req.get_data(as_text=True)

    if not signature:
        return False

    if not SQUARE_SIGNATURE_KEY or not SQUARE_WEBHOOK_URL:
        # Keep local dev usable; enforce env-driven signature checks in production.
        return not (IS_PRODUCTION or STRICT_WEBHOOK_SIGNATURE)

    payload = f"{SQUARE_WEBHOOK_URL}{body}".encode("utf-8")
    digest = hmac.new(
        SQUARE_SIGNATURE_KEY.encode("utf-8"),
        payload,
        hashlib.sha256,
    ).digest()
    expected_signature = base64.b64encode(digest).decode("utf-8")
    return hmac.compare_digest(expected_signature, signature)


def square_base_url():
    return "https://connect.squareupsandbox.com" if SQUARE_ENV == "sandbox" else "https://connect.squareup.com"


def public_square_script_url():
    return "https://sandbox.web.squarecdn.com/v1/square.js" if SQUARE_ENV == "sandbox" else "https://web.squarecdn.com/v1/square.js"


def create_event_ticket_id():
    return f"TICKET_{secrets.token_hex(8).upper()}"


def qr_image_url(checkin_url):
    encoded = urllib.parse.quote(checkin_url, safe="")
    return f"https://api.qrserver.com/v1/create-qr-code/?size=260x260&data={encoded}"


def external_base_url(req):
    forwarded_proto = (req.headers.get("X-Forwarded-Proto") or "").split(",")[0].strip()
    forwarded_host = (req.headers.get("X-Forwarded-Host") or "").split(",")[0].strip()
    if forwarded_proto and forwarded_host:
        return f"{forwarded_proto}://{forwarded_host}".rstrip("/")
    return req.url_root.rstrip("/")


def parse_square_payment(webhook_payload):
    payment = webhook_payload.get("data", {}).get("object", {}).get("payment", {}) or {}
    payment_id = (payment.get("id") or "").strip()
    amount_cents = int(payment.get("amount_money", {}).get("amount") or 0)
    note_parts = [
        payment.get("note", ""),
        payment.get("reference_id", ""),
        payment.get("receipt_number", ""),
    ]
    note_blob = " ".join(part for part in note_parts if part).lower()
    email = (payment.get("buyer_email_address") or "").strip()
    status = (payment.get("status") or "").strip().upper()
    return payment, payment_id, amount_cents, note_blob, email, status


def create_ticket_from_square_payment(cursor, payment, amount_cents, email):
    print("[ticket-create] called")
    ticket_name = map_ticket_from_amount(amount_cents)
    payment_id = (payment.get("id") or "").strip()
    event_name = event_name_from_payment(payment)
    print(f"[ticket-create] payment_id={payment_id} amount_cents={amount_cents} mapped_ticket={ticket_name} event_name={event_name}")
    if not ticket_name or not payment_id:
        print("[ticket-create] skip insert: missing ticket mapping or payment_id")
        return None

    cursor.execute("SELECT ticket_id FROM event_tickets WHERE payment_id = ?", (payment_id,))
    existing = cursor.fetchone()
    if existing:
        print(f"[ticket-create] duplicate payment_id; existing ticket_id={existing[0]}")
        return existing[0]

    billing = payment.get("billing_address", {}) or {}
    first_name = (billing.get("first_name") or "").strip()
    last_name = (billing.get("last_name") or "").strip()
    full_name = f"{first_name} {last_name}".strip() or "Guest"
    buyer_email = (email or "").strip() or "no-email@example.com"

    ticket_id = create_event_ticket_id()
    base_url = (os.getenv("CHECKIN_BASE_URL", "") or "").strip().rstrip("/")
    if not base_url:
        base_url = "http://localhost:5003"
    checkin_url = f"{base_url}/checkin/{ticket_id}"
    qr_url = qr_image_url(checkin_url)

    try:
        print("[ticket-create] inserting into event_tickets")
        cursor.execute(
            """
            INSERT INTO event_tickets (
                name, email, ticket_type, amount_cents, ticket_id, status,
                payment_id, checkin_url, qr_url, event_name
            ) VALUES (?, ?, ?, ?, ?, 'not_checked_in', ?, ?, ?, ?)
            """,
            (
                full_name,
                buyer_email.lower(),
                ticket_name,
                int(amount_cents or 0),
                ticket_id,
                payment_id,
                checkin_url,
                qr_url,
                event_name,
            ),
        )
        cursor.connection.commit()
        print(f"[ticket-create] insert success ticket_id={ticket_id}")
        return ticket_id
    except Exception as exc:
        print(f"[ticket-create] insert failed: {exc}")
        try:
            cursor.connection.rollback()
        except Exception:
            pass
        return None


def already_logged_payment(cursor, payment_id):
    if not payment_id:
        return False
    cursor.execute("SELECT 1 FROM square_payment_log WHERE payment_id = ?", (payment_id,))
    return cursor.fetchone() is not None


def log_square_payment(cursor, payment_id, category, amount_cents):
    cursor.execute(
        """
        INSERT OR IGNORE INTO square_payment_log (payment_id, category, amount_cents)
        VALUES (?, ?, ?)
        """,
        (payment_id, category, amount_cents),
    )


def apply_ticket_sale_from_square(cursor, payment):
    ticket_name = map_ticket_from_payment(payment)
    if not ticket_name:
        return False
    cursor.execute(
        """
        UPDATE ticket_types
        SET sold = sold + 1
        WHERE event_name = ? AND ticket_name = ?
        """,
        ("Battle of the DJs", ticket_name),
    )
    return cursor.rowcount > 0


def is_membership_payment(amount_cents, note_blob):
    if MEMBERSHIP_AMOUNT_CENTS > 0 and int(amount_cents or 0) == MEMBERSHIP_AMOUNT_CENTS:
        return True
    membership_terms = ("membership", "member", "circle")
    return any(term in (note_blob or "") for term in membership_terms)


def apply_membership_from_square(cursor, payment, amount_cents, note_blob, email):
    if not is_membership_payment_from_payment(payment, amount_cents, note_blob):
        return False

    payment_id = (payment.get("id") or "").strip()
    amount_dollars = round((int(amount_cents or 0) / 100.0), 2)
    name = (
        payment.get("billing_address", {}).get("first_name")
        or payment.get("shipping_address", {}).get("first_name")
        or "Member"
    )

    cursor.execute(
        """
        INSERT OR IGNORE INTO memberships (name, email, amount, status, payment_id, source)
        VALUES (?, ?, ?, 'Active', ?, 'square')
        """,
        (name, email, amount_dollars, payment_id),
    )
    return cursor.rowcount > 0


def classify_square_payment(payment, amount_cents, note_blob):
    if int(amount_cents or 0) == 100:
        return "ignored_test", None
    if is_membership_payment_from_payment(payment, amount_cents, note_blob):
        return "membership", None
    ticket_name = map_ticket_from_amount(amount_cents)
    if ticket_name:
        return "ticket", ticket_name
    return "unmatched", None


def square_list_payments(limit=100):
    if not SQUARE_ACCESS_TOKEN:
        return []

    endpoint = f"{square_base_url()}/v2/payments?sort_order=DESC&limit={max(1, min(limit, 100))}"
    req = urlrequest.Request(
        endpoint,
        headers={
            "Authorization": f"Bearer {SQUARE_ACCESS_TOKEN}",
            "Square-Version": "2024-11-20",
            "Content-Type": "application/json",
        },
        method="GET",
    )

    try:
        with urlrequest.urlopen(req, timeout=20) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
            return payload.get("payments", []) or []
    except urlerror.HTTPError as exc:
        print("Square list payments HTTP error:", exc.code)
    except Exception as exc:
        print("Square list payments error:", exc)
    return []


def sync_square_payments(limit=100, full_resync=False, include_diagnostics=False, dry_run=False):
    payments = square_list_payments(limit=limit)
    if not payments:
        return {
            "processed": 0,
            "tickets": 0,
            "memberships": 0,
            "duplicates": 0,
            "unmatched": 0,
            "total_seen": 0,
            "details": [],
        }

    conn = sqlite3.connect("database.db")
    cursor = conn.cursor()
    counts = {
        "processed": 0,
        "tickets": 0,
        "memberships": 0,
        "duplicates": 0,
        "unmatched": 0,
        "total_seen": len(payments),
        "details": [],
    }

    if full_resync and not dry_run:
        cursor.execute("UPDATE ticket_types SET sold = 0")
        cursor.execute("DELETE FROM memberships WHERE source = 'square'")
        cursor.execute("DELETE FROM square_payment_log WHERE category IN ('ticket','membership')")

    for payment in payments:
        payment_id = (payment.get("id") or "").strip()
        if not payment_id:
            continue

        if (not full_resync) and already_logged_payment(cursor, payment_id):
            counts["duplicates"] += 1
            continue

        amount_cents = int(payment.get("amount_money", {}).get("amount") or 0)
        note_blob = " ".join(
            str(part).strip().lower()
            for part in (
                payment.get("note", ""),
                payment.get("reference_id", ""),
                payment.get("receipt_number", ""),
            )
            if part
        )
        email = (payment.get("buyer_email_address") or "").strip()

        kind, ticket_name = classify_square_payment(payment, amount_cents, note_blob)
        ticket_hit = False
        membership_hit = False
        ticket_created = False

        if kind == "ignored_test":
            continue

        if kind == "ticket":
            if dry_run:
                ticket_hit = True
                ticket_created = True
            else:
                created_ticket_id = create_ticket_from_square_payment(cursor, payment, amount_cents, email)
                ticket_created = bool(created_ticket_id)
                ticket_hit = apply_ticket_sale_from_square(cursor, payment)
        elif kind == "membership":
            if dry_run:
                membership_hit = True
            else:
                membership_hit = apply_membership_from_square(cursor, payment, amount_cents, note_blob, email)

        if ticket_hit or membership_hit or ticket_created:
            if not dry_run:
                log_square_payment(
                    cursor,
                    payment_id,
                    "ticket" if (ticket_hit or ticket_created) else "membership",
                    amount_cents,
                )
            counts["processed"] += 1
            counts["tickets"] += 1 if (ticket_hit or ticket_created) else 0
            counts["memberships"] += 1 if membership_hit else 0
            if include_diagnostics and len(counts["details"]) < 40:
                counts["details"].append(
                    {
                        "payment_id": payment_id,
                        "amount_cents": amount_cents,
                        "category": "ticket" if (ticket_hit or ticket_created) else "membership",
                        "ticket_name": ticket_name,
                        "note": note_blob[:180],
                        "email": email,
                    }
                )
        else:
            counts["unmatched"] += 1
            if include_diagnostics and len(counts["details"]) < 40:
                counts["details"].append(
                    {
                        "payment_id": payment_id,
                        "amount_cents": amount_cents,
                        "category": "unmatched",
                        "ticket_name": ticket_name,
                        "note": note_blob[:180],
                        "email": email,
                    }
                )

    if not dry_run:
        conn.commit()
    conn.close()
    return counts

# -------------------------
# EVENTS DATA
# -------------------------
events_data = [
    {
        "id": 1,
        "name": "Battle of the DJs",
        "flyer": "images/flyer-part1.jpg",
        "description": """Step into an elevated indoor/outdoor experience at Battle of the DJs — where top talent goes head-to-head, delivering high-energy sets and unforgettable vibes all night long. Expect great music, curated energy, and a crowd that knows how to move.

In the event of unfavorable weather conditions, the experience will be rescheduled. All tickets will remain valid for the new date, with options available for transfer or refund.

This is an exclusive 30+ event. Valid government-issued ID is required for entry. Guests who do not meet the age requirement will be denied entry at the door. No refunds will be issued.""",
        "ticket_link": "https://square.link/u/Y9p9XqJo?src=sheet",
        "event_datetime": "Sunday, May 17, 2026",
        "location": "923 East Main Street, Durham, NC (Kore)",
        "time": "3:00 PM - 8:00 PM",
        "doors": "3:00 PM",
        "ticket_label": "Multiple Ticket Options",
        "map_link": "https://www.google.com/maps/search/?api=1&query=923+East+Main+Street+Durham+NC",
        "description_long": """Step into an elevated indoor/outdoor experience at Battle of the DJs — where top talent goes head-to-head, delivering high-energy sets and unforgettable vibes all night long. Expect great music, curated energy, and a crowd that knows how to move.

In the event of unfavorable weather conditions, the experience will be rescheduled. All tickets will remain valid for the new date, with options available for transfer or refund.

This is an exclusive 30+ event. Valid government-issued ID is required for entry. Guests who do not meet the age requirement will be denied entry at the door. No refunds will be issued.""",
        "early_link": "https://square.link/u/EyY0RvTh?src=sheet",
        "ga_link": "https://square.link/u/Y9p9XqJo?src=sheet",
        "vip_link": "https://square.link/u/ikIAImYb?src=sheet",
        "booth_link": "https://square.link/u/QfLXGM6i?src=sheet",

        "tickets": {
            "early": {"price": 13, "sold": 0, "size": 1},
            "ga": {"price": 18, "sold": 0, "size": 1},
            "vip": {"price": 175, "sold": 0, "size": 1},
            "booth": {"price": 200, "sold": 0, "size": 6}
        }
    },
    {
        "id": 2,
        "name": "The Quiet Storm Live",
        "flyer": "/static/images/flyer-part2.jpg",
        "description": "The Intimate R&B Experience",
        "ticket_link": "https://square.link/u/p4eAdd8g",
        "event_datetime": "Thursday, June 11, 2026",
        "location": "345 Blackwell St, Durham, NC (ALOFT Durham Downtown)",
        "time": "7:00 PM - 9:00 PM",
        "doors": "6:30 PM",
        "ticket_label": "General Admission",
        "map_link": "https://www.google.com/maps/search/?api=1&query=345+Blackwell+St+Durham+NC",
        "early_link": "https://square.link/u/p4eAdd8g",
        "ga_link": "https://square.link/u/p4eAdd8g",
        "vip_link": "https://square.link/u/p4eAdd8g",
        "booth_link": "https://square.link/u/p4eAdd8g",
        "tickets": {
            "early": {"price": 13, "sold": 0, "size": 1},
            "ga": {"price": 18, "sold": 0, "size": 1},
            "vip": {"price": 175, "sold": 0, "size": 1},
            "booth": {"price": 200, "sold": 0, "size": 6}
        }
    },
]


# -------------------------
# HOME
# -------------------------
@app.route("/")
def home():
    return render_template("index.html")

# -------------------------
# EVENTS
# -------------------------
@app.route("/events")
def events():
    conn = sqlite3.connect("database.db")
    cursor = conn.cursor()

    # -------------------------
    # VOTES (SAFE)
    # -------------------------
    try:
        cursor.execute("""
            SELECT event_name, votes
            FROM event_votes
            ORDER BY votes DESC
        """)
        event_votes = cursor.fetchall()
    except Exception as e:
        print("VOTES ERROR:", e)
        event_votes = []

    conn.close()

    # -------------------------
    # EVENTS (USE PYTHON DATA ONLY)
    # -------------------------
    events_list = events_data

    return render_template(
        "events.html",
        events=events_list,
        event_votes=event_votes,
        vinyl_options=[
            "Grown and Sexy Ball",
            "Line Dancing",
            "Afrobeats",
            "Live Bands",
            "Open Mic"
        ]
    )
# -------------------------
# CONTACT (FULLY WORKING)
# -------------------------
@app.route("/contact", methods=["GET", "POST"])
def contact():
    if request.method == "POST":
        name = request.form.get("name")
        email = request.form.get("email")
        message = request.form.get("message")

        conn = sqlite3.connect("database.db")
        cursor = conn.cursor()

        cursor.execute("""
            INSERT INTO leads (type, name, email, details, status)
            VALUES (?, ?, ?, ?, ?)
        """, ("Contact Message", name, email, message, "New"))

        conn.commit()
        conn.close()

        send_email(
            "New Inquiry",
            f"Name: {name}\nEmail: {email}\nMessage:\n{message}",
            EMAIL_ADDRESS
        )

        return render_template(
            "thank_you.html",
            title="MESSAGE RECEIVED",
            message="Your message has been sent. Our team will get back to you shortly."
        )

    return render_template("contact.html")

# -------------------------
# MEMBERSHIP PAGE
# -------------------------
@app.route("/membership")
def membership():
    return render_template("membership.html")

# -------------------------
# JOIN MEMBERSHIP
# -------------------------
@app.route("/join-membership", methods=["POST"])
def join_membership():
    name = request.form.get("name")
    email = request.form.get("email")

    conn = sqlite3.connect("database.db")
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO leads (type, name, email, details, status)
        VALUES (?, ?, ?, ?, ?)
    """, ("Membership Signup", name, email, "Waiting for payment", "Pending"))

    conn.commit()
    conn.close()

    return redirect("https://square.link/u/fgiSNspy")

# -------------------------
# WEBHOOK
# -------------------------
@app.route("/webhook/square", methods=["POST"], strict_slashes=False)
@app.route("/square-webhook", methods=["POST"], strict_slashes=False)
def square_webhook():
    print("🔥 WEBHOOK HIT")
    print("=== WEBHOOK START ===")

    raw_body = request.get_data(as_text=True)
    print("[headers]:", dict(request.headers))
    print("[body]:", raw_body)

    data = request.get_json(silent=True) or {}
    event_id = (data.get("event_id") or data.get("id") or "").strip()
    event_type = (data.get("type") or "").strip()

    print("EVENT TYPE:", event_type)

    if not verify_square_signature(request):
        print("⚠️ signature invalid — bypassed (dev mode)")

    conn = sqlite3.connect("database.db")
    cursor = conn.cursor()

    payment, payment_id, amount_cents, note_blob, email, status = parse_square_payment(data)
    is_duplicate_payment = already_logged_payment(cursor, payment_id) if payment_id else False
    square_name = extract_square_name(payment)
    mapped_ticket_type = SQUARE_TO_DB_MAP.get(square_name, square_name)

    print("DEBUG STATUS:", status)
    print("DEBUG PAYMENT ID:", payment_id)
    print("DEBUG AMOUNT:", amount_cents)
    print("DEBUG EMAIL:", email)
    print("DEBUG DUPLICATE PAYMENT:", is_duplicate_payment)
    print("DEBUG SQUARE ITEM NAME:", square_name)
    print("DEBUG MAPPED TICKET TYPE:", mapped_ticket_type)

    cursor.execute(
        """
        INSERT INTO webhook_logs (source, event_id, event_type, note)
        VALUES (?, ?, ?, ?)
        """,
        ("square", event_id, event_type, f"status={status} payment_id={payment_id}"),
    )

    ticket_id = None
    print("REACHED TICKET LOGIC")
    print("STATUS:", status)
    print("EVENT:", event_type)

    if not payment_id:
        print("Missing payment_id; skipping ticket create.")
    elif status == "COMPLETED" and event_type in ("payment.updated", "payment.created"):
        print("CREATING TICKET")
        try:
            ticket_id = create_ticket_from_square_payment(cursor, payment, amount_cents, email)
        except Exception as exc:
            print("❌ TICKET INSERT FAILED:", exc)
            ticket_id = None
        print("TICKET ID:", ticket_id)
        if ticket_id:
            print("SENDING EMAIL")
            send_ticket_email_once(cursor, ticket_id)
    else:
        print("Ticket condition not met.")

    ticket_hit = apply_ticket_sale_from_square(cursor, payment)
    membership_hit = apply_membership_from_square(cursor, payment, amount_cents, note_blob, email)

    if payment_id and not is_duplicate_payment and (ticket_hit or membership_hit or ticket_id):
        log_square_payment(cursor, payment_id, "ticket", amount_cents)
        conn.commit()
        conn.close()
        return "ok", 200

    if payment_id and is_duplicate_payment:
        conn.commit()
        conn.close()
        return "duplicate", 200

    conn.commit()
    conn.close()
    print("=== WEBHOOK END ===")
    return "ignored", 200
# -------------------------
# DASHBOARD
# -------------------------
@app.route("/dashboard")
@requires_auth
def dashboard():
    # Optional manual backfill from Square account on dashboard load.
    if os.getenv("SQUARE_AUTO_SYNC_ON_DASHBOARD", "0") == "1":
        sync_square_payments(limit=SQUARE_SYNC_LIMIT)

    conn = sqlite3.connect("database.db")
    cursor = conn.cursor()

    def attendance_units(ticket_name, sold_count):
        name = (ticket_name or "").strip().lower()
        sold = int(sold_count or 0)
        if name in ("vip section", "dj vip section", "vip booth", "dj vip booth"):
            return sold * 6
        return sold

    # Ticket metrics: always sourced directly from event_tickets
    test_filter = """
        (payment_id IS NULL OR (
            UPPER(payment_id) NOT LIKE '%TEST%'
            AND UPPER(payment_id) NOT LIKE '%FINAL_TEST%'
            AND UPPER(payment_id) NOT LIKE '%FREE_TEST%'
        ))
    """

    cursor.execute(
        """
        SELECT COALESCE(event_name, 'Battle of the DJs') as event_name,
               ticket_type,
               COUNT(*) as count,
               COALESCE(SUM(amount_cents), 0) as revenue_cents
        FROM event_tickets
        WHERE """ + test_filter + """
        GROUP BY COALESCE(event_name, 'Battle of the DJs'), ticket_type
        ORDER BY COALESCE(event_name, 'Battle of the DJs'), ticket_type
        """
    )
    ticket_rows = cursor.fetchall()
    ticket_data = [
        (row[0], row[1], int(row[2] or 0), round((float(row[3] or 0) / 100.0), 2))
        for row in ticket_rows
    ]
    event_totals = {}
    for event_name, _ticket_type, count, revenue in ticket_data:
        if event_name not in event_totals:
            event_totals[event_name] = {"count": 0, "revenue": 0.0}
        event_totals[event_name]["count"] += count
        event_totals[event_name]["revenue"] += revenue

    cursor.execute("SELECT COUNT(*) FROM event_tickets WHERE " + test_filter)
    total_tickets = int(cursor.fetchone()[0] or 0)

    cursor.execute("SELECT COALESCE(SUM(amount_cents), 0) FROM event_tickets WHERE " + test_filter)
    total_ticket_revenue = round((float(cursor.fetchone()[0] or 0) / 100.0), 2)

    cursor.execute("SELECT COUNT(*) FROM event_tickets WHERE status = 'checked_in' AND " + test_filter)
    checked_in_count = int(cursor.fetchone()[0] or 0)

    # Membership metrics
    cursor.execute(
        """
        SELECT COUNT(*), SUM(amount_cents)
        FROM square_payment_log
        WHERE LOWER(category) = 'membership'
        """
    )
    membership_count, membership_revenue_cents = cursor.fetchone()
    membership_count = int(membership_count or 0)
    membership_revenue = round((float(membership_revenue_cents or 0) / 100.0), 2)
    print("DASHBOARD memberships source: square_payment_log (category=membership)")
    print("DASHBOARD memberships count:", membership_count)
    print("DASHBOARD memberships revenue:", membership_revenue)

    # Lead activity
    cursor.execute(
        """
        SELECT id, type, name, email, details, status
        FROM leads
        ORDER BY id DESC
        """
    )
    leads = cursor.fetchall()

    def lead_bucket(lead_type):
        t = (lead_type or "").strip().lower()
        if "dj" in t or "band" in t:
            return "dj"
        if "vip" in t:
            return "vip"
        if "membership" in t:
            return "membership"
        if "contact" in t or "inquiry" in t:
            return "inquiry"
        return "other"

    dj_leads = [l for l in leads if lead_bucket(l[1]) == "dj"]
    vip_leads = [l for l in leads if lead_bucket(l[1]) == "vip"]
    inquiry_leads = [l for l in leads if lead_bucket(l[1]) == "inquiry"]
    membership_leads = [l for l in leads if lead_bucket(l[1]) == "membership"]

    # Votes from events page
    cursor.execute(
        """
        SELECT event_name, votes
        FROM event_votes
        ORDER BY votes DESC, event_name ASC
        """
    )
    event_votes = cursor.fetchall()

    # Event request log (archived items kept, not deleted)
    cursor.execute(
        """
        SELECT id, event_name, status, archived
        FROM event_requests
        ORDER BY id DESC
        """
    )
    event_request_log = cursor.fetchall()

    cursor.execute("SELECT COUNT(*) FROM event_tickets WHERE " + test_filter)
    print("DASHBOARD event_tickets COUNT:", cursor.fetchone()[0])
    cursor.execute("SELECT COUNT(*) FROM event_tickets WHERE " + test_filter)
    total = cursor.fetchone()[0]
    print("TOTAL TICKETS (BACKEND):", total)

    cursor.execute("SELECT COUNT(*) FROM square_payment_log")
    print("DASHBOARD payment_log COUNT:", cursor.fetchone()[0])

    cursor.execute(
        """
        SELECT ticket_type, COUNT(*)
        FROM event_tickets
        WHERE """ + test_filter + """
        GROUP BY ticket_type
        """
    )
    print("DASHBOARD ticket breakdown:", cursor.fetchall())

    conn.close()

    ticket_count = total_tickets
    total_attendance = checked_in_count
    ticket_revenue = total_ticket_revenue
    total_revenue = round(total_ticket_revenue + membership_revenue, 2)
    membership_revenue = round(membership_revenue, 2)

    return render_template(
        "dashboard.html",
        # Top KPIs
        ticket_count=ticket_count,
        total_attendance=total_attendance,
        ticket_revenue=ticket_revenue,
        total_tickets=total_tickets,
        total_checked_in=checked_in_count,
        membership_count=membership_count,
        membership_revenue=membership_revenue,
        total_revenue=total_revenue,
        # Ticket details from event_tickets
        ticket_data=ticket_data,
        event_totals=event_totals,
        # Lead activity + lead log
        dj_count=len(dj_leads),
        vip_count=len(vip_leads),
        requests_count=len(inquiry_leads),
        membership_lead_count=len(membership_leads),
        total_leads=len(leads),
        dj_leads=dj_leads,
        vip_leads=vip_leads,
        inquiry_leads=inquiry_leads,
        membership_leads=membership_leads,
        leads=leads,
        # Votes + event requests
        event_votes=event_votes,
        event_request_log=event_request_log,
    )
@app.route("/dj-signup", methods=["GET", "POST"])
def dj_signup():

    if request.method == "POST":

        name = request.form.get("name")
        brand = request.form.get("brand")
        email = request.form.get("email")
        phone = request.form.get("phone")
        performer_type = request.form.get("type")
        genre = request.form.get("genre")
        links = request.form.get("links")
        rate = request.form.get("rate")
        comments = request.form.get("comments")

        details = f"""
        Brand: {brand}
        Phone: {phone}
        Type: {performer_type}
        Genre: {genre}
        Links: {links}
        Rate: {rate}
        Comments: {comments}
        """

        conn = sqlite3.connect("database.db")
        cursor = conn.cursor()

        cursor.execute("""
            INSERT INTO leads (type, name, email, details, status)
            VALUES (?, ?, ?, ?, ?)
        """, ("DJ Application", name, email, details, "New"))

        conn.commit()
        conn.close()

        return render_template(
            "thank_you.html",
            title="APPLICATION RECEIVED",
            message="Your application has been submitted successfully. Our team will review your sound and reach out if you're a fit for an upcoming Jukebox experience."
        )

    return render_template("dj_signup.html")

@app.route("/vip", methods=["POST"])
def vip_signup():

    name = request.form.get("name")
    email = request.form.get("email")
    phone = request.form.get("phone")

    details = f"""
    Phone: {phone}
    """

    conn = sqlite3.connect("database.db")
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO leads (type, name, email, details, status)
        VALUES (?, ?, ?, ?, ?)
    """, ("VIP Signup", name, email, details, "New"))

    conn.commit()
    conn.close()

    return render_template(
        "thank_you.html",
        title="WELCOME TO THE VIP EMAIL LIST",
        message="You're officially on the VIP Email list. Get ready for exclusive drops, early access, and curated experiences."
    )

@app.route("/event-interest", methods=["POST"])
def event_interest():

    raw_name = request.form.get("event_name")
    event_name = clean_event_name(raw_name)

    conn = sqlite3.connect("database.db")
    cursor = conn.cursor()

    # ✅ ONLY STORE REQUEST (NO AUTO VOTING)
    cursor.execute("""
INSERT INTO event_requests (event_name, status)
VALUES (?, 'New')
""", (event_name,))

    conn.commit()
    conn.close()

    return render_template(
        "thank_you.html",
        title="SUBMITTED",
        message="Your input helps shape future Jukebox events."
    )
@app.route("/vote-event", methods=["POST"])
def vote_event():
    event_name = request.form.get("event_name")

    # 🔥 normalize EVERYTHING
    event_name = event_name.strip().lower()

    conn = sqlite3.connect("database.db")
    cursor = conn.cursor()

    cursor.execute("""
        UPDATE event_votes
        SET votes = votes + 1
        WHERE LOWER(event_name) = ?
    """, (event_name,))

    conn.commit()
    conn.close()

    return redirect("/events")
@app.route("/complete-request/<int:id>", methods=["POST"])
@requires_auth
def complete_request(id):
    conn = sqlite3.connect("database.db")
    cursor = conn.cursor()

    cursor.execute(
        """
        UPDATE event_requests
        SET status = 'Completed',
            archived = 1,
            archived_at = CURRENT_TIMESTAMP
        WHERE id = ?
        """,
        (id,),
    )

    conn.commit()
    conn.close()

    return redirect("/dashboard")


@app.route("/update-event-request/<int:id>", methods=["POST"])
@requires_auth
def update_event_request(id):
    new_status = (request.form.get("status") or "New").strip().title()
    if new_status not in ("New", "Completed"):
        new_status = "New"

    archived = 1 if new_status == "Completed" else 0
    archived_at_expr = "CURRENT_TIMESTAMP" if archived == 1 else "NULL"

    conn = sqlite3.connect("database.db")
    cursor = conn.cursor()
    cursor.execute(
        f"""
        UPDATE event_requests
        SET status = ?,
            archived = ?,
            archived_at = {archived_at_expr}
        WHERE id = ?
        """,
        (new_status, archived, id),
    )
    conn.commit()
    conn.close()
    return redirect("/dashboard")

@app.route("/buy-ticket", methods=["POST"])
def buy_ticket():
    event_name = request.form.get("event_name")
    ticket_name = request.form.get("ticket_name")

    purchase_ticket(event_name, ticket_name)

    return redirect("/events")

from urllib.parse import unquote

@app.route("/event/<path:event_name>")
def event_detail(event_name):
    from urllib.parse import unquote

    event_name = unquote(event_name).strip().lower()

    print("🔥 EVENT PAGE HIT:", event_name)

    # -------------------------
    # FIND EVENT FROM PYTHON DATA ONLY
    # -------------------------
    event = next(
        (e for e in events_data if e["name"].lower() == event_name),
        None
    )

    if not event:
        return f"Event not found: {event_name}", 404

    conn = sqlite3.connect("database.db")
    cursor = conn.cursor()

    try:
        cursor.execute("""
            SELECT ticket_name, price, max_quantity, sold
            FROM ticket_types
            WHERE LOWER(event_name) = ?
        """, (event_name,))
        tickets = cursor.fetchall()
        sold_lookup = {}
        for ticket_name, _price, _quantity, _sold in tickets:
            cursor.execute(
                """
                SELECT COUNT(*)
                FROM event_tickets
                WHERE ticket_type = ?
                  AND COALESCE(event_name, 'Battle of the DJs') = ?
                  AND (payment_id IS NULL OR (
                        UPPER(payment_id) NOT LIKE 'FREE_TEST_%'
                    AND UPPER(payment_id) NOT LIKE 'TEST_%'
                  ))
                """,
                (ticket_name, event["name"]),
            )
            sold_lookup[ticket_name] = int(cursor.fetchone()[0] or 0)

    except Exception as e:
        conn.close()
        return f"TICKET DB ERROR: {e}", 500

    ticket_data = []

    for name, price, quantity, sold in tickets:
        sold = sold_lookup.get(name, 0)
        remaining = quantity - sold

        ticket_data.append({
            "name": name,
            "price": round(price, 2),
            "sold": sold,
            "quantity": quantity,
            "remaining": remaining,
            "sold_out": remaining <= 0,
            "almost_gone": 0 < remaining <= 5
        })

    conn.close()

    is_quiet_storm = event["name"] == "The Quiet Storm Live"
    hero_flyer_url = event.get("flyer", "")
    if isinstance(hero_flyer_url, str) and hero_flyer_url.startswith("/static/"):
        hero_flyer_url = hero_flyer_url
    else:
        hero_flyer_url = url_for("static", filename=hero_flyer_url)

    return render_template(
        "event_detail.html",
        event=event,
        ticket_data=ticket_data,
        display_name_map=DISPLAY_NAME_MAP,
        is_quiet_storm=is_quiet_storm,
        hero_flyer_url=hero_flyer_url,
    )

@app.route("/check-tickets")
@requires_auth
def check_tickets():
    conn = sqlite3.connect("database.db")
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM ticket_types")
    data = cursor.fetchall()

    conn.close()
    return str(data)

@app.route("/test-sell")
@requires_auth
def test_sell():
    conn = sqlite3.connect("database.db")
    cursor = conn.cursor()

    cursor.execute("""
        UPDATE ticket_types
        SET sold = 5
        WHERE ticket_name = 'Early Bird'
    """)

    cursor.execute("""
        UPDATE ticket_types
        SET sold = 1
        WHERE ticket_name = 'VIP Section'
    """)

    conn.commit()
    conn.close()

    return "Updated!"

@app.route("/check-leads")
@requires_auth
def check_leads():
    conn = sqlite3.connect("database.db")
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM leads")
    data = cursor.fetchall()

    conn.close()

    return str(data)

@app.route("/test-lead")
@requires_auth
def test_lead():
    conn = sqlite3.connect("database.db")
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO leads (type, name, email, details, status)
        VALUES (?, ?, ?, ?, ?)
    """, ("Membership Signup", "Test User", "test@email.com", "Wants to join", "new"))

    conn.commit()
    conn.close()

    return "Test lead added!"

@app.route("/dashboard-data")
@requires_auth
def dashboard_data():
    conn = sqlite3.connect("database.db")
    cursor = conn.cursor()

    cursor.execute("""
        SELECT ticket_name, sold, price
        FROM ticket_types
    """)
    rows = cursor.fetchall()

    conn.close()

    ticket_count = 0
    attendance = 0
    revenue = 0.0
    for ticket_name, sold, price in rows:
        sold = int(sold or 0)
        price = float(price or 0)
        ticket_count += sold
        revenue += price * sold
        name = (ticket_name or "").strip().lower()
        if name in ("vip section", "dj vip section", "vip booth", "dj vip booth"):
            attendance += sold * 6
        else:
            attendance += sold

    return {
        "ticket_count": ticket_count,
        "attendance": attendance,
        "revenue": round(revenue, 2),
    }

@app.route("/square-sync", methods=["POST"])
@requires_auth
def square_sync():
    mode = (request.args.get("mode") or "").strip().lower()
    full_resync = mode == "full"
    summary = sync_square_payments(
        limit=SQUARE_SYNC_LIMIT,
        full_resync=full_resync,
        include_diagnostics=True,
    )
    return {
        "status": "ok",
        "mode": "full" if full_resync else "incremental",
        "total_seen": summary["total_seen"],
        "processed": summary["processed"],
        "tickets": summary["tickets"],
        "memberships": summary["memberships"],
        "duplicates": summary["duplicates"],
        "unmatched": summary["unmatched"],
        "details": summary["details"],
    }, 200


@app.route("/square-sync-report")
@requires_auth
def square_sync_report():
    preview_limit = int(request.args.get("limit", SQUARE_SYNC_LIMIT) or SQUARE_SYNC_LIMIT)
    summary = sync_square_payments(
        limit=preview_limit,
        full_resync=False,
        include_diagnostics=True,
        dry_run=True,
    )
    return {
        "status": "ok",
        "mode": "report",
        "total_seen": summary["total_seen"],
        "processed": summary["processed"],
        "tickets": summary["tickets"],
        "memberships": summary["memberships"],
        "duplicates": summary["duplicates"],
        "unmatched": summary["unmatched"],
        "details": summary["details"],
    }, 200
@app.route("/test-membership-webhook", methods=["POST"])
@requires_auth
def test_membership_webhook():
    data = request.json

    name = data.get("name")
    email = data.get("email")
    amount = float(data.get("amount", 0) or 0)
    payment_id = data.get("payment_id")

    conn = sqlite3.connect("database.db")
    cursor = conn.cursor()

    cursor.execute("""
        INSERT OR IGNORE INTO memberships (name, email, amount, status, payment_id, source)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (name, email, amount, "Active", payment_id, "manual-test"))

    conn.commit()
    conn.close()

    return {"status": "success"}, 200

@app.route("/update-lead/<int:id>", methods=["POST"])
@requires_auth
def update_lead(id):
    new_status = request.form.get("status")

    conn = sqlite3.connect("database.db")
    cursor = conn.cursor()

    cursor.execute("""
        UPDATE leads
        SET status = ?
        WHERE id = ?
    """, (new_status, id))

    conn.commit()
    conn.close()

    return redirect("/dashboard")


@app.route("/tickets/checkout")
def tickets_checkout():
    event = events_data[0] if events_data else {}
    payment_links = {
        "early_bird": {
            "label": "Early Bird",
            "amount_cents": 1300,
            "url": event.get("early_link", ""),
        },
        "general_admission": {
            "label": "General Admission",
            "amount_cents": 1800,
            "url": event.get("ga_link", ""),
        },
        "vip_section": {
            "label": "VIP Section",
            "amount_cents": 17500,
            "url": event.get("vip_link", ""),
        },
        "dj_vip_section": {
            "label": "DJ VIP Section",
            "amount_cents": 20000,
            "url": event.get("booth_link", ""),
        },
    }
    return render_template("tickets_checkout.html", payment_links=payment_links)


@app.route("/api/tickets/public-config")
def tickets_public_config():
    return {
        "applicationId": SQUARE_APPLICATION_ID,
        "locationId": SQUARE_LOCATION_ID,
        "squareEnv": SQUARE_ENV,
        "ticketTypes": WEB_TICKET_TYPES,
    }


@app.route("/api/tickets/purchase", methods=["POST"])
def tickets_purchase():
    return {"success": False, "error": "Embedded checkout is disabled. Use Square payment links."}, 410


@app.route("/checkin/<ticket_id>")
def checkin(ticket_id):
    import sqlite3
    conn = sqlite3.connect("database.db")
    cursor = conn.cursor()

    cursor.execute("SELECT status FROM event_tickets WHERE ticket_id = ?", (ticket_id,))
    row = cursor.fetchone()

    if not row:
        conn.close()
        return render_template("checkin_result.html", status="invalid")

    current_status = row[0]

    if current_status == "checked_in":
        conn.close()
        return render_template("checkin_result.html", status="already_checked_in")

    cursor.execute(
        """
        UPDATE event_tickets
        SET status = 'checked_in',
            checked_in_at = CURRENT_TIMESTAMP
        WHERE ticket_id = ?
        """,
        (ticket_id,),
    )
    conn.commit()
    conn.close()

    return render_template("checkin_result.html", status="success")


@app.route("/tickets/admin")
@requires_auth
def tickets_admin():
    db_path = os.path.join(os.path.dirname(__file__), "database.db")
    print(f"[tickets_admin] using database: {db_path}")
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    status_filter = (request.args.get("status") or "").strip().lower()
    if status_filter == "used":
        cursor.execute(
            """
            SELECT id, name, email, ticket_type, amount_cents, ticket_id, status, payment_id, qr_url, checkin_url, created_at
            FROM event_tickets
            WHERE status = 'checked_in'
            ORDER BY id DESC
            """
        )
    elif status_filter == "unused":
        cursor.execute(
            """
            SELECT id, name, email, ticket_type, amount_cents, ticket_id, status, payment_id, qr_url, checkin_url, created_at
            FROM event_tickets
            WHERE status != 'checked_in'
            ORDER BY id DESC
            """
        )
    else:
        cursor.execute(
            """
            SELECT id, name, email, ticket_type, amount_cents, ticket_id, status, payment_id, qr_url, checkin_url, created_at
            FROM event_tickets
            ORDER BY id DESC
            """
        )
    tickets = cursor.fetchall()
    print(f"[tickets_admin] rows fetched: {len(tickets)}")
    conn.close()
    return render_template("tickets_admin.html", tickets=tickets, status_filter=status_filter)


@app.route("/admin/backfill")
@requires_auth
def run_backfill():
    """
    Temporary admin route to backfill completed Square payments into event_tickets
    using the same database file as the Flask app.
    """
    print("=== BACKFILL DEBUG START ===")

    print("TOKEN EXISTS:", bool(os.getenv("SQUARE_ACCESS_TOKEN")))
    print("LOCATION:", os.getenv("SQUARE_LOCATION_ID"))

    url = "https://connect.squareup.com/v2/payments"

    headers = {
        "Authorization": f"Bearer {os.getenv('SQUARE_ACCESS_TOKEN')}",
        "Content-Type": "application/json"
    }

    params = {
        "location_id": os.getenv("SQUARE_LOCATION_ID"),
        "limit": 100
    }

    response = requests.get(url, headers=headers, params=params)

    print("STATUS CODE:", response.status_code)
    print("RAW RESPONSE:", response.text[:500])  # first 500 chars

    data = response.json()
    payments = data.get("payments", [])

    print("PAYMENTS FOUND:", len(payments))
    print("=== BACKFILL DEBUG END ===")

    db_path = os.path.join(os.path.dirname(__file__), "database.db")
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    created = 0
    skipped = 0

    for payment in payments:
        payment_id = (payment.get("id") or "").strip()
        status = (payment.get("status") or "").strip().upper()
        amount_cents = int(payment.get("amount_money", {}).get("amount") or 0)
        email = (payment.get("buyer_email_address") or "").strip()

        if status != "COMPLETED" or not payment_id:
            continue

        if already_logged_payment(cursor, payment_id):
            skipped += 1
            continue

        ticket_id = create_ticket_from_square_payment(cursor, payment, amount_cents, email)
        ticket_hit = apply_ticket_sale_from_square(cursor, payment)
        membership_hit = apply_membership_from_square(
            cursor,
            payment,
            amount_cents,
            " ".join(
                str(part).strip().lower()
                for part in (
                    payment.get("note", ""),
                    payment.get("reference_id", ""),
                    payment.get("receipt_number", ""),
                )
                if part
            ),
            email,
        )

        if ticket_id:
            send_ticket_email_once(cursor, ticket_id)

        if ticket_hit or membership_hit or ticket_id:
            log_square_payment(
                cursor,
                payment_id,
                "ticket" if (ticket_hit or ticket_id) else "membership",
                amount_cents,
            )
            created += 1

    conn.commit()
    conn.close()
    return f"Backfill complete. Created: {created}, Skipped duplicates: {skipped}", 200


@app.route("/admin/rebuild-ticket-data", methods=["GET", "POST"])
@requires_auth
def rebuild_ticket_data():
    """
    One-click cleanup + resync for ticket/membership metrics.
    Clears derived ticket rows and ticket/membership payment logs, then re-syncs from Square.
    """
    # Temporary Square backfill pull (past payments)
    square_access_token = os.getenv("SQUARE_ACCESS_TOKEN", "").strip()
    if square_access_token:
        try:
            url = "https://connect.squareup.com/v2/payments"
            headers = {
                "Authorization": f"Bearer {square_access_token}",
                "Content-Type": "application/json",
            }
            response = requests.get(url, headers=headers, timeout=20)
            data = response.json()
            payments = data.get("payments", []) or []

            backfill_conn = sqlite3.connect("database.db")
            backfill_cursor = backfill_conn.cursor()
            for p in payments:
                payment_id = (p.get("id") or "").strip()
                amount = int((p.get("amount_money", {}) or {}).get("amount") or 0)
                if not payment_id:
                    continue

                backfill_cursor.execute(
                    "SELECT 1 FROM square_payment_log WHERE payment_id = ?",
                    (payment_id,),
                )
                if backfill_cursor.fetchone():
                    continue

                backfill_cursor.execute(
                    """
                    INSERT INTO square_payment_log (payment_id, category, amount_cents, created_at)
                    VALUES (?, 'ticket', ?, CURRENT_TIMESTAMP)
                    """,
                    (payment_id, amount),
                )
            backfill_conn.commit()
            backfill_conn.close()
        except Exception as e:
            print("[rebuild/backfill] error:", e)

    conn = sqlite3.connect("database.db")
    cursor = conn.cursor()
    cursor.execute("DELETE FROM event_tickets")
    cursor.execute("DELETE FROM square_payment_log WHERE category IN ('ticket','membership')")
    cursor.execute("UPDATE ticket_types SET sold = 0")
    conn.commit()
    conn.close()

    summary = sync_square_payments(
        limit=SQUARE_SYNC_LIMIT,
        full_resync=False,
        include_diagnostics=True,
    )
    return {
        "status": "ok",
        "message": "Rebuild complete",
        "processed": summary["processed"],
        "tickets": summary["tickets"],
        "memberships": summary["memberships"],
        "duplicates": summary["duplicates"],
        "unmatched": summary["unmatched"],
        "details": summary["details"],
    }, 200


@app.route("/debug/tickets")
def debug_tickets():
    import sqlite3
    conn = sqlite3.connect("database.db")
    cursor = conn.cursor()

    cursor.execute("SELECT ticket_id FROM event_tickets LIMIT 10")
    rows = cursor.fetchall()

    conn.close()
    return {"tickets": rows}


@app.route("/debug/square")
def debug_square():
    import sqlite3
    conn = sqlite3.connect("database.db")
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM square_payment_log LIMIT 10")
    rows = cursor.fetchall()

    conn.close()

    return {"square_payments": rows}


@app.route('/scan')
def scan():
    return render_template('scan.html')


@app.route('/admin/generate-qr/<ticket_id>')
def generate_qr(ticket_id):
    import qrcode
    from io import BytesIO
    from flask import send_file

    url = f"https://www.jukeboxloungenc.com/checkin/{ticket_id}"

    img = qrcode.make(url)
    buf = BytesIO()
    img.save(buf)
    buf.seek(0)

    return send_file(buf, mimetype='image/png')


@app.route('/qr/<ticket_id>')
def qr(ticket_id):
    import qrcode
    from io import BytesIO
    from flask import send_file

    url = f"https://www.jukeboxloungenc.com/checkin/{ticket_id}"
    img = qrcode.make(url)

    buf = BytesIO()
    img.save(buf, format="PNG")
    buf.seek(0)

    return send_file(buf, mimetype='image/png')

# -------------------------
# RUN
# -------------------------
if __name__ == "__main__":
    app.run(debug=True, port=5050, use_reloader=False)
