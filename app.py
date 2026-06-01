import base64
import csv
import hashlib
import hmac
import io
import json
import os
import secrets
import sqlite3
import smtplib
import traceback
import urllib.parse
import requests
from email import encoders
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.mime.image import MIMEImage
from email.utils import make_msgid
from functools import wraps
from urllib import error as urlerror
from urllib import request as urlrequest

from flask import Flask, Response, redirect, render_template, request, url_for
try:
    from google.oauth2.credentials import Credentials as GoogleCredentials
    from google.auth.transport.requests import Request as GoogleRequest
    from googleapiclient.discovery import build as google_build
except Exception:
    GoogleCredentials = None
    GoogleRequest = None
    google_build = None

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

DB_PATH = os.getenv("DATABASE_PATH", "database.db").strip() or "database.db"


def seed_vip_signups_from_csv(cursor):
    seed_path = os.path.join(os.path.dirname(__file__), "data", "vip_seed.csv")
    if not os.path.exists(seed_path):
        return

    with open(seed_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            email = (row.get("email") or "").strip().lower()
            if not email:
                continue
            name = (row.get("name") or "").strip() or "VIP Member"
            details = (row.get("details") or "Phone: ").strip()
            status = (row.get("status") or "Active").strip() or "Active"
            if status not in ("Active", "Inactive"):
                status = "Active"

            cursor.execute(
                """
                SELECT id FROM leads
                WHERE type = 'VIP Signup'
                  AND LOWER(COALESCE(email, '')) = ?
                LIMIT 1
                """,
                (email,),
            )
            existing = cursor.fetchone()
            if existing:
                cursor.execute(
                    """
                    UPDATE leads
                    SET name = ?,
                        details = ?,
                        status = ?,
                        archived = CASE WHEN ? = 'Inactive' THEN 1 ELSE 0 END,
                        archived_at = CASE WHEN ? = 'Inactive' THEN CURRENT_TIMESTAMP ELSE NULL END
                    WHERE id = ?
                    """,
                    (name, details, status, status, status, existing[0]),
                )
            else:
                cursor.execute(
                    """
                    INSERT INTO leads (type, name, email, details, status, archived, archived_at, created_at)
                    VALUES ('VIP Signup', ?, ?, ?, ?, CASE WHEN ?='Inactive' THEN 1 ELSE 0 END,
                            CASE WHEN ?='Inactive' THEN CURRENT_TIMESTAMP ELSE NULL END, CURRENT_TIMESTAMP)
                    """,
                    (name, email, details, status, status, status),
                )

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

def render_thank_you_safe(title, message):
    try:
        return render_template("thank_you.html", title=title, message=message)
    except Exception as exc:
        print("[thank-you] fallback render used:", exc)
        traceback.print_exc()
        safe_title = (title or "Thank You")
        safe_message = (message or "Submission received.")
        return (
            f"""
            <!doctype html>
            <html>
              <head>
                <meta charset="utf-8">
                <meta name="viewport" content="width=device-width, initial-scale=1.0">
                <title>{safe_title}</title>
                <style>
                  body {{
                    margin: 0;
                    background: #0a0a0a;
                    color: #f7f7f7;
                    font-family: Arial, sans-serif;
                    display: grid;
                    place-items: center;
                    min-height: 100vh;
                  }}
                  .card {{
                    width: min(640px, 92vw);
                    border: 1px solid rgba(212, 175, 55, 0.45);
                    border-radius: 14px;
                    background: #141414;
                    padding: 28px;
                    text-align: center;
                  }}
                  h1 {{ color: #D4AF37; margin: 0 0 10px; }}
                  p {{ margin: 0 0 16px; line-height: 1.6; }}
                  a {{
                    display: inline-block;
                    margin-top: 8px;
                    color: #111;
                    background: #D4AF37;
                    text-decoration: none;
                    font-weight: 700;
                    border-radius: 8px;
                    padding: 10px 14px;
                  }}
                </style>
              </head>
              <body>
                <div class="card">
                  <h1>{safe_title}</h1>
                  <p>{safe_message}</p>
                  <a href="/">Back Home</a>
                </div>
              </body>
            </html>
            """,
            200,
            {"Content-Type": "text/html; charset=utf-8"},
        )

# -------------------------
# DATABASE SETUP
# -------------------------
# ✅ DATABASE SETUP (RUN ON APP START)

def init_db():
    conn = sqlite3.connect(DB_PATH)
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
    ensure_column("leads", "created_at", "TEXT")
    ensure_column("leads", "archived", "INTEGER DEFAULT 0")
    ensure_column("leads", "archived_at", "TEXT")
    ensure_column("leads", "notes", "TEXT")
    cursor.execute(
        """
        UPDATE leads
        SET created_at = COALESCE(created_at, CURRENT_TIMESTAMP)
        WHERE created_at IS NULL OR TRIM(created_at) = ''
        """
    )

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
    ensure_column("event_tickets", "checked_in", "INTEGER DEFAULT 0")
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

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS attendees (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        event_name TEXT,
        name TEXT,
        customer_name TEXT,
        ticket_type TEXT,
        quantity INTEGER DEFAULT 1,
        checked_in_count INTEGER DEFAULT 0,
        status TEXT DEFAULT 'Not Checked In',
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    )
    """)
    ensure_column("attendees", "customer_name", "TEXT")
    ensure_column("attendees", "quantity", "INTEGER DEFAULT 1")
    ensure_column("attendees", "checked_in_count", "INTEGER DEFAULT 0")
    ensure_column("attendees", "created_at", "TEXT")
    cursor.execute(
        """
        UPDATE attendees
        SET customer_name = COALESCE(NULLIF(TRIM(customer_name), ''), name)
        WHERE COALESCE(TRIM(customer_name), '') = ''
        """
    )
    cursor.execute(
        """
        UPDATE attendees
        SET quantity = CASE
            WHEN quantity IS NULL OR quantity < 1 THEN 1
            ELSE quantity
        END
        """
    )
    cursor.execute(
        """
        UPDATE attendees
        SET checked_in_count = CASE
            WHEN checked_in_count IS NULL OR checked_in_count < 0 THEN
                CASE WHEN LOWER(COALESCE(status, '')) = 'checked in' THEN 1 ELSE 0 END
            WHEN checked_in_count > quantity THEN quantity
            ELSE checked_in_count
        END
        """
    )
    cursor.execute(
        """
        UPDATE attendees
        SET status = CASE
            WHEN checked_in_count <= 0 THEN 'Not Checked In'
            WHEN checked_in_count >= quantity THEN 'Checked In'
            ELSE 'Partially Checked In'
        END
        """
    )
    cursor.execute(
        """
        UPDATE attendees
        SET created_at = COALESCE(NULLIF(TRIM(created_at), ''), CURRENT_TIMESTAMP)
        WHERE created_at IS NULL OR TRIM(created_at) = ''
        """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS mass_email_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            category TEXT,
            subject TEXT,
            recipients_count INTEGER DEFAULT 0,
            attachments_count INTEGER DEFAULT 0,
            sent_count INTEGER DEFAULT 0,
            failed_count INTEGER DEFAULT 0,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """
    )

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

    seed_vip_signups_from_csv(cursor)

    # Ensure historical membership record exists on deploy.
    cursor.execute(
        """
        INSERT INTO leads (type, name, email, details, status, archived, archived_at, created_at)
        SELECT 'Membership Signup', 'Keeva Nichols', 'keevanichols@gmail.com',
               'Imported historical membership purchase', 'Active', 0, NULL, CURRENT_TIMESTAMP
        WHERE NOT EXISTS (
            SELECT 1 FROM leads
            WHERE type = 'Membership Signup'
              AND LOWER(COALESCE(email, '')) = 'keevanichols@gmail.com'
        )
        """
    )
    cursor.execute(
        """
        UPDATE leads
        SET name = 'Keeva Nichols',
            status = 'Active',
            archived = 0,
            archived_at = NULL
        WHERE type = 'Membership Signup'
          AND LOWER(COALESCE(email, '')) = 'keevanichols@gmail.com'
        """
    )

    cursor.execute(
        """
        INSERT INTO memberships (name, email, amount, status, source)
        SELECT 'Keeva Nichols', 'keevanichols@gmail.com', 10.0, 'Active', 'manual-import'
        WHERE NOT EXISTS (
            SELECT 1 FROM memberships
            WHERE LOWER(COALESCE(email, '')) = 'keevanichols@gmail.com'
        )
        """
    )
    cursor.execute(
        """
        UPDATE memberships
        SET name = 'Keeva Nichols',
            status = 'Active'
        WHERE LOWER(COALESCE(email, '')) = 'keevanichols@gmail.com'
        """
    )

    conn.commit()
    conn.close()

def purchase_ticket(event_name, ticket_name):
    conn = sqlite3.connect(DB_PATH)
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

@app.errorhandler(500)
def handle_internal_server_error(err):
    try:
        failing_path = request.path or ""
    except Exception:
        failing_path = ""
    print(f"[error-500] path={failing_path} err={err}")
    traceback.print_exc()

    form_paths = {
        "/contact",
        "/vip",
        "/join-membership",
        "/dj-signup",
        "/vendor-signup",
        "/event-interest",
    }
    if failing_path in form_paths:
        return render_thank_you_safe(
            "SUBMISSION RECEIVED",
            "Thanks! We received your form and our team will follow up shortly.",
        )
    return ("Internal Server Error", 500)

init_db()   # ✅ AFTER function exists
# -------------------------
# EMAIL CONFIG
# -------------------------
SQUARE_SIGNATURE_KEY = os.getenv("SQUARE_SIGNATURE_KEY", "")
SQUARE_WEBHOOK_URL = os.getenv("SQUARE_WEBHOUK_URL", "")
SQUARE_ACCESS_TOKEN = os.getenv("SQUARE_ACCESS_TOKEN", "").strip()
SQUARE_ENV = os.getenv("SQUARE_ENV", "sandbox").strip()
if SQUARE_ENV == "sandbox":
    SQUARE_BASE_URL = "https://connect.squareupsandbox.com"
else:
    SQUARE_BASE_URL = "https://connect.squareup.com"
SQUARE_APPLICATION_ID = os.getenv("SQUARE_APPLICATION_ID", "")
SQUARE_LOCATION_ID = os.getenv("SQUARE_LOCATION_ID", "")
BASE_URL = "https://www.jukeboxloungenc.com"
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

TICKET_CAPACITY = {
    "Early Bird": 30,
    "General Admission": 366,
    "VIP Section": 6,
    "DJ VIP Section": 6,
    "Custom VIP": 6,
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


def normalize_event_name(value):
    raw = (value or "").strip().lower()
    if "quiet storm" in raw:
        return "Quiet Storm"
    if raw in ("the quiet storm live", "part 2 - the quiet storm live"):
        return "Quiet Storm"
    return "Battle of the DJs"


def event_name_from_payment(payment):
    base_blob = " ".join(
        str(part).strip().lower()
        for part in (
            payment.get("note", ""),
            payment.get("reference_id", ""),
            payment.get("receipt_number", ""),
        )
        if part
    )
    order_blob = ""
    order_id = (payment.get("order_id") or "").strip()
    if order_id:
        order = square_retrieve_order(order_id)
        line_items = order.get("line_items", []) if isinstance(order, dict) else []
        order_blob = " ".join(str((item or {}).get("name", "")).strip().lower() for item in line_items if item)
    blob = f"{base_blob} {order_blob}".strip()
    mapped = normalize_event_name(blob)
    print("[event-map] source:", blob)
    print("[event-map] mapped:", mapped)
    return mapped


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
        print("Email failed: SMTP credentials missing at runtime")
        return False

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


def send_email_with_attachments(subject, body, to_email, attachments=None, flyer_inline=None, cta_text="", cta_url="", require_gmail_api=False):
    email_address = (
        os.getenv("GMAIL_SENDER_EMAIL", "").strip()
        or os.getenv("EMAIL_ADDRESS", "").strip()
        or os.getenv("SMTP_EMAIL_ADDRESS", "").strip()
        or "thejukeboxloungenc@gmail.com"
    )

    try:
        msg = MIMEMultipart("mixed")
        msg["Subject"] = subject
        msg["From"] = email_address
        msg["To"] = to_email

        related_part = MIMEMultipart("related")
        alternative_part = MIMEMultipart("alternative")
        related_part.attach(alternative_part)

        # Keep a short plain-text fallback (clients should prefer HTML alternative).
        alternative_part.attach(MIMEText(body, "plain", "utf-8"))

        attachment_count = len(attachments or [])
        safe_cta_text = (cta_text or "").strip()
        safe_cta_url = (cta_url or "").strip()
        attachments_html = ""
        if attachment_count > 0:
            attachments_html = f"""
            <div style="margin-top:16px;padding:12px;border:1px solid rgba(212,175,55,0.25);border-radius:10px;background:#0c0c0c;color:#d9d9d9;">
              📎 {attachment_count} attachment(s) included with this email.
            </div>
            """

        flyer_html = ""
        flyer_cid = None
        if flyer_inline and flyer_inline.get("content"):
            flyer_cid = make_msgid(domain="jukeboxloungenc.com")[1:-1]
            flyer_html = f"""
            <div style="margin-top:14px;">
              <img src="cid:{flyer_cid}" alt="Flyer" style="max-width:100%;height:auto;border-radius:10px;border:1px solid rgba(212,175,55,0.3);" />
            </div>
            """

        cta_html = ""
        if safe_cta_text and safe_cta_url:
            cta_html = f"""
            <div style="margin-top:16px;">
              <a href="{safe_cta_url}" target="_blank" style="display:inline-block;padding:11px 16px;border-radius:8px;background:#D4AF37;color:#111;text-decoration:none;font-weight:700;">
                {safe_cta_text}
              </a>
            </div>
            """

        signature_html = """
        <br><br>
        <p style="color:#d9d9d9;line-height:1.6;">
          See you soon,<br>
          <strong style="color:#D4AF37;">The Jukebox Lounge NC</strong>
        </p>
        """
        html_body = f"""
        <html>
          <body style="margin:0;padding:22px;background:#0a1610;font-family:Arial,sans-serif;color:#fff;">
            <div style="max-width:640px;margin:0 auto;background:linear-gradient(160deg,#102217,#0d130f);border:1px solid rgba(212,175,55,0.55);border-radius:16px;overflow:hidden;box-shadow:0 10px 30px rgba(0,0,0,0.45);">
              <div style="padding:14px 18px;background:linear-gradient(120deg,#1f4d34,#123625 45%,#1b1b1b);border-bottom:1px solid rgba(212,175,55,0.35);text-align:center;">
                <img src="https://www.jukeboxloungenc.com/static/images/hero.jpg" alt="The Jukebox Lounge NC" style="width:120px;max-width:100%;height:auto;border-radius:8px;border:1px solid rgba(212,175,55,0.65);" />
              </div>
              <div style="padding:20px 22px;color:#f4efe2;line-height:1.7;">
                <p style="margin:0 0 12px;color:#ffe7a8;font-size:17px;font-weight:700;">Jukebox Lounge Update</p>
                <p style="margin:0;">{body.replace(chr(10), '<br>')}</p>
                {flyer_html}
                {cta_html}
                {attachments_html}
                {signature_html}
              </div>
              <div style="height:8px;background:linear-gradient(90deg,#D4AF37,#f2d06b,#D4AF37);"></div>
            </div>
          </body>
        </html>
        """
        alternative_part.attach(MIMEText(html_body, "html", "utf-8"))

        if flyer_cid:
            flyer_filename = (flyer_inline.get("filename") or "flyer.jpg").strip()
            flyer_content = flyer_inline.get("content") or b""
            flyer_mime = (flyer_inline.get("mimetype") or "image/jpeg").strip().lower()
            if "/" in flyer_mime:
                flyer_main, flyer_sub = flyer_mime.split("/", 1)
            else:
                flyer_main, flyer_sub = "image", "jpeg"
            if flyer_main == "image":
                img_part = MIMEImage(flyer_content, _subtype=flyer_sub)
            else:
                img_part = MIMEApplication(flyer_content, Name=flyer_filename)
            img_part.add_header("Content-ID", f"<{flyer_cid}>")
            img_part.add_header("Content-Disposition", f'inline; filename="{flyer_filename}"')
            related_part.attach(img_part)

        # Attach the composed related/alternative body first.
        msg.attach(related_part)

        for item in (attachments or []):
            filename = (item.get("filename") or "attachment").strip()
            content = item.get("content") or b""
            if not content:
                continue
            part = MIMEApplication(content, Name=filename)
            part["Content-Disposition"] = f'attachment; filename="{filename}"'
            msg.attach(part)

        if not send_via_gmail_api(msg):
            if require_gmail_api:
                print("Email failed: Gmail API send required but unavailable.")
                return False
            # Fallback only if Gmail API is not configured yet.
            email_password = (
                os.getenv("EMAIL_PASSWORD", "").strip()
                or os.getenv("SMTP_EMAIL_PASSWORD", "").strip()
            )
            if not email_password:
                print("Email failed: Gmail API and SMTP credentials missing.")
                return False
            with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
                smtp.login(email_address, email_password)
                smtp.send_message(msg)
        print(f"[mass-email] sent to {to_email} with {len(attachments or [])} attachments")
        return True
    except Exception as e:
        print("Email failed:", e)
        return False


@app.route("/admin/leads/mass-email-send", methods=["POST"])
@requires_auth
def mass_email_send_locked():
    # Safety lock: keep production stable by forcing Gmail compose workflow.
    return {"ok": False, "error": "Backend mass send is disabled. Use Open in Gmail from VIP/Membership logs."}, 200


def send_via_gmail_api(mime_msg):
    """
    Sends message through Gmail API using OAuth credentials.
    Env required:
      GMAIL_CLIENT_ID
      GMAIL_CLIENT_SECRET
      GMAIL_REFRESH_TOKEN
      GMAIL_SENDER_EMAIL (optional, defaults to me)
    """
    if not (GoogleCredentials and GoogleRequest and google_build):
        print("[gmail-api] google client libraries not installed.")
        return False

    client_id = os.getenv("GMAIL_CLIENT_ID", "").strip()
    client_secret = os.getenv("GMAIL_CLIENT_SECRET", "").strip()
    refresh_token = os.getenv("GMAIL_REFRESH_TOKEN", "").strip()
    if not client_id or not client_secret or not refresh_token:
        print("[gmail-api] missing OAuth env vars.")
        return False

    try:
        creds = GoogleCredentials(
            token=None,
            refresh_token=refresh_token,
            token_uri="https://oauth2.googleapis.com/token",
            client_id=client_id,
            client_secret=client_secret,
            scopes=["https://www.googleapis.com/auth/gmail.send"],
        )
        creds.refresh(GoogleRequest())
        service = google_build("gmail", "v1", credentials=creds, cache_discovery=False)
        raw_message = base64.urlsafe_b64encode(mime_msg.as_bytes()).decode("utf-8")
        service.users().messages().send(userId="me", body={"raw": raw_message}).execute()
        return True
    except Exception as exc:
        print("[gmail-api] send failed:", exc)
        return False


def send_html_email(subject, to_email, plain_body, html_body):
    email_address = (
        os.getenv("EMAIL_ADDRESS", "").strip()
        or os.getenv("SMTP_EMAIL_ADDRESS", "").strip()
    )
    email_password = (
        os.getenv("EMAIL_PASSWORD", "").strip()
        or os.getenv("SMTP_EMAIL_PASSWORD", "").strip()
    )
    if not email_address or not email_password:
        print("Email failed: SMTP credentials missing at runtime")
        return False

    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = email_address
        msg["To"] = to_email
        msg.attach(MIMEText(plain_body, "plain"))
        msg.attach(MIMEText(html_body, "html"))

        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
            smtp.login(email_address, email_password)
            smtp.send_message(msg)
        return True
    except Exception as e:
        print("Email failed:", e)
        return False


def send_membership_welcome_email(name, email):
    recipient = (email or "").strip()
    if not recipient:
        return False

    plain = (
        f"Hi {name or 'Member'},\n\n"
        "Welcome to The Jukebox Lounge Circle Membership.\n"
        "You’re now in line for curated invites, members-only updates, and premium experiences.\n\n"
        "Complete your membership checkout to activate full benefits.\n\n"
        "The Jukebox Lounge NC"
    )

    html = f"""
    <html>
      <body style="margin:0;padding:0;background:#070707;font-family:Arial,sans-serif;color:#fff;">
        <div style="max-width:620px;margin:24px auto;background:#101010;border:1px solid rgba(212,175,55,0.4);border-radius:14px;overflow:hidden;">
          <div style="padding:22px 24px;background:linear-gradient(135deg,#1d1d1d,#090909);border-bottom:1px solid rgba(212,175,55,0.25);text-align:center;">
            <img src="https://www.jukeboxloungenc.com/static/images/hero.jpg" alt="The Jukebox Lounge NC" style="max-width:100%;height:auto;border-radius:8px;" />
          </div>
          <div style="padding:26px 24px;">
            <h2 style="margin:0 0 10px;color:#D4AF37;letter-spacing:0.6px;">Welcome to Jukebox Circle Membership</h2>
            <p style="margin:0 0 12px;color:#f1f1f1;font-size:15px;">Hi {name or 'Member'},</p>
            <p style="margin:0 0 12px;color:#d9d9d9;line-height:1.6;">
              Thank you for joining. Your membership puts you at the front of the line for
              elevated events, insider updates, and premium member-only experiences.
            </p>
            <div style="margin:18px 0;padding:14px;border:1px solid rgba(212,175,55,0.3);border-radius:10px;background:#0c0c0c;">
              <p style="margin:0 0 8px;color:#D4AF37;font-weight:bold;">Member Perks</p>
              <p style="margin:0;color:#d9d9d9;line-height:1.6;">• Priority event updates<br>• Curated member announcements<br>• Exclusive membership experiences</p>
            </div>
            <p style="margin:14px 0 0;color:#d9d9d9;line-height:1.6;">
              Complete your checkout to activate your full membership benefits.
            </p>
            <p style="margin:18px 0 0;color:#D4AF37;font-weight:bold;">The Jukebox Lounge NC</p>
          </div>
        </div>
      </body>
    </html>
    """
    return send_html_email("Welcome to Jukebox Circle Membership", recipient, plain, html)


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


def send_tickets_email_bundle(cursor, payment_id, customer_email, event_title="The Jukebox Lounge NC"):
    pid = (payment_id or "").strip()
    recipient = (customer_email or "").strip().lower()
    if not pid or not recipient:
        return False

    cursor.execute(
        """
        SELECT ticket_id, ticket_type
        FROM event_tickets
        WHERE (payment_id = ? OR payment_id LIKE ?)
          AND (ticket_email_sent_at IS NULL OR ticket_email_sent_at = '')
        ORDER BY ticket_id ASC
        """,
        (pid, f"{pid}:%"),
    )
    rows = cursor.fetchall()
    if not rows:
        return True

    print("SENDING TICKETS TO:", recipient)
    print("QR COUNT:", len(rows))

    email_address = (
        os.getenv("EMAIL_ADDRESS", "").strip()
        or os.getenv("SMTP_EMAIL_ADDRESS", "").strip()
    )
    email_password = (
        os.getenv("EMAIL_PASSWORD", "").strip()
        or os.getenv("SMTP_EMAIL_PASSWORD", "").strip()
    )
    if not email_address or not email_password:
        print("[ticket-email-bundle] missing SMTP credentials")
        return False

    sections = []
    ticket_ids = []
    for ticket_id, ticket_type in rows:
        ticket_ids.append(ticket_id)
        qr_url = f"https://www.jukeboxloungenc.com/qr/{ticket_id}"
        sections.append(
            f"""
            <div style="margin-bottom:24px;padding:14px;border:1px solid #ddd;border-radius:8px;">
              <p><strong>Ticket ID:</strong> {ticket_id}</p>
              <p><strong>Ticket Type:</strong> {ticket_type}</p>
              <img src="{qr_url}" width="250" alt="QR for {ticket_id}" />
            </div>
            """
        )

    html_body = f"""
    <html>
      <body style="font-family:Arial,sans-serif;color:#111;">
        <h2>{event_title}</h2>
        <p>Thank you for your purchase. Your tickets are below.</p>
        {''.join(sections)}
      </body>
    </html>
    """

    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = "Your Jukebox Lounge Tickets"
        msg["From"] = email_address
        msg["To"] = recipient
        msg.attach(MIMEText("Your tickets are attached in HTML format.", "plain"))
        msg.attach(MIMEText(html_body, "html"))

        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
            smtp.login(email_address, email_password)
            smtp.sendmail(email_address, [recipient], msg.as_string())

        for ticket_id in ticket_ids:
            cursor.execute(
                """
                UPDATE event_tickets
                SET ticket_email_sent_at = CURRENT_TIMESTAMP
                WHERE ticket_id = ?
                """,
                (ticket_id,),
            )
        return True
    except Exception as e:
        print("[ticket-email-bundle] failed:", e)
        return False


def send_tickets_email_for_customer(email, tickets, subject="Your Jukebox Lounge QR Tickets"):
    recipient = (email or "").strip().lower()
    if not recipient or not tickets:
        return False

    print("SENDING TO:", recipient)
    print("TICKETS:", [t.get("ticket_id") for t in tickets])

    email_address = (
        os.getenv("EMAIL_ADDRESS", "").strip()
        or os.getenv("SMTP_EMAIL_ADDRESS", "").strip()
    )
    email_password = (
        os.getenv("EMAIL_PASSWORD", "").strip()
        or os.getenv("SMTP_EMAIL_PASSWORD", "").strip()
    )
    if not email_address or not email_password:
        print("[resend-email] missing SMTP credentials")
        return False

    sections = []
    for t in tickets:
        ticket_id = t.get("ticket_id")
        ticket_type = t.get("ticket_type", "")
        qr_url = t.get("qr_url") or f"https://www.jukeboxloungenc.com/qr/{ticket_id}"
        sections.append(
            f"""
            <div style="margin-bottom:24px;padding:14px;border:1px solid #ddd;border-radius:8px;">
              <p><strong>Ticket ID:</strong> {ticket_id}</p>
              <p><strong>Ticket Type:</strong> {ticket_type}</p>
              <img src="{qr_url}" width="250" alt="QR for {ticket_id}" />
            </div>
            """
        )

    html_body = f"""
    <html>
      <body style="font-family:Arial,sans-serif;color:#111;">
        <h2>The Jukebox Lounge NC</h2>
        <p>Thank you for your support. Your ticket QR codes are below.</p>
        {''.join(sections)}
      </body>
    </html>
    """
    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = email_address
        msg["To"] = recipient
        msg.attach(MIMEText("Your tickets are attached in HTML format.", "plain"))
        msg.attach(MIMEText(html_body, "html"))

        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
            smtp.login(email_address, email_password)
            smtp.sendmail(email_address, [recipient], msg.as_string())
        return True
    except Exception as e:
        print("[resend-email] failed:", e)
        return False


LEAD_STATUS_MAP = {
    "DJ Application": ("New", "Contacted", "Booked", "Declined"),
    "Vendor Application": ("New", "Contacted", "Booked", "Declined"),
    "Contact Message": ("New", "Replied", "Closed"),
    "VIP Signup": ("Active", "Inactive"),
    "Membership Signup": ("Active", "Inactive"),
}


def lead_category(lead_type):
    t = (lead_type or "").strip().lower()
    if "dj" in t or "band" in t:
        return "DJ Application"
    if "vendor" in t:
        return "Vendor Application"
    if "vip" in t:
        return "VIP Signup"
    if "membership" in t:
        return "Membership Signup"
    return "Contact Message"


def normalize_lead_status(lead_type, status=None):
    category = lead_category(lead_type)
    allowed = LEAD_STATUS_MAP.get(category, ("New",))
    if category == "Contact Message" and (status or "").strip().lower() == "responded":
        status = "Replied"
    if status in allowed:
        return status
    return allowed[0]


def lead_is_archived_status(status):
    s = (status or "").strip().lower()
    return s in ("closed", "declined")


def notify_admin_new_lead(lead_type, name, email, status, details=""):
    try:
        admin_email = (
            os.getenv("ADMIN_NOTIFICATION_EMAIL", "").strip()
            or os.getenv("ADMIN_EMAIL", "").strip()
            or "thejukeboxloungenc@gmail.com"
        )
        subject = f"New {lead_category(lead_type)} Received"
        body = (
            f"Lead Type: {lead_type}\n"
            f"Name: {name}\n"
            f"Email: {email}\n"
            f"Status: {status}\n"
            f"Details:\n{details}\n"
        )
        sent = send_email(subject, body, admin_email)
        if sent:
            print(f"[lead-notify] sent to {admin_email}")
        else:
            print(f"[lead-notify] failed to send to {admin_email}")
    except Exception as exc:
        print("[lead-notify] failed:", exc)


def create_lead_record(lead_type, name, email, details, status=None):
    status_value = normalize_lead_status(lead_type, status)
    archived = 1 if lead_is_archived_status(status_value) else 0
    clean_name = (name or "").strip()
    clean_email = (email or "").strip().lower()
    clean_details = (details or "").strip()

    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        try:
            cursor.execute(
                """
                INSERT INTO leads (type, name, email, details, status, archived, archived_at)
                VALUES (?, ?, ?, ?, ?, ?, CASE WHEN ? = 1 THEN CURRENT_TIMESTAMP ELSE NULL END)
                """,
                (
                    lead_type,
                    clean_name,
                    clean_email,
                    clean_details,
                    status_value,
                    archived,
                    archived,
                ),
            )
        except sqlite3.OperationalError as exc:
            # Backward-compatible fallback for local DBs that do not have archive columns yet.
            print("[lead-save] schema fallback:", exc)
            cursor.execute(
                """
                INSERT INTO leads (type, name, email, details, status)
                VALUES (?, ?, ?, ?, ?)
                """,
                (lead_type, clean_name, clean_email, clean_details, status_value),
            )

        conn.commit()
        conn.close()
    except Exception as exc:
        print("[lead-save] failed:", exc)
        traceback.print_exc()
        return False

    # Notification is best-effort and must never break submit flow.
    try:
        notify_admin_new_lead(lead_type, clean_name, clean_email, status_value, clean_details)
    except Exception as exc:
        print("[lead-notify] non-fatal error:", exc)
    return True


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
    return SQUARE_BASE_URL


def public_square_script_url():
    return "https://sandbox.web.squarecdn.com/v1/square.js" if SQUARE_ENV == "sandbox" else "https://web.squarecdn.com/v1/square.js"


def generate_ticket(payment_id, index=0):
    base = (payment_id or "").strip()[:12]
    return f"TICKET_{base}_{int(index) + 1}"


def create_event_ticket_id(payment_id=None):
    pid = (payment_id or "").strip()
    if pid:
        return generate_ticket(pid, 0)
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
    payment_id = (payment.get("id") or "").strip()
    event_name = normalize_event_name(event_name_from_payment(payment))
    if not event_name:
        event_name = "Battle of the DJs"
    if not payment_id:
        print("[ticket-create] skip insert: missing payment_id")
        return []

    billing = payment.get("billing_address", {}) or {}
    first_name = (billing.get("first_name") or "").strip()
    last_name = (billing.get("last_name") or "").strip()
    full_name = f"{first_name} {last_name}".strip() or "Guest"
    buyer_email = (email or "").strip() or "no-email@example.com"

    base_url = BASE_URL
    print("QR BASE URL:", BASE_URL)
    created_ticket_ids = []

    order = square_retrieve_order((payment.get("order_id") or "").strip())
    line_items = order.get("line_items", []) if isinstance(order, dict) else []

    if line_items:
        next_index = 0
        for item in line_items:
            ticket_name = line_item_ticket_name(item)
            if not ticket_name:
                continue
            quantity = parse_line_item_quantity(item)
            print(f"Creating {quantity} tickets for payment {payment_id}")
            for _ in range(quantity):
                ticket_id = generate_ticket(payment_id, next_index)
                payment_ref = f"{payment_id}:{next_index}"
                next_index += 1

                cursor.execute("SELECT 1 FROM event_tickets WHERE ticket_id = ?", (ticket_id,))
                if cursor.fetchone():
                    continue

                checkin_url = f"{base_url}/checkin/{ticket_id}"
                qr_url = f"{base_url}/qr/{ticket_id}"
                print("QR GENERATED:", qr_url)
                try:
                    print("CREATING TICKET:", ticket_id)
                    print("EMAIL:", buyer_email.lower())
                    print("PAYMENT:", payment_id)
                    cursor.execute(
                        """
                        INSERT INTO event_tickets (
                            name, email, ticket_type, amount_cents, ticket_id, status,
                            payment_id, checkin_url, qr_url, event_name, checked_in
                        ) VALUES (?, ?, ?, ?, ?, 'not_checked_in', ?, ?, ?, ?, 0)
                        """,
                        (
                            full_name,
                            buyer_email.lower(),
                            ticket_name,
                            int(amount_cents or 0),
                            ticket_id,
                            payment_ref,
                            checkin_url,
                            qr_url,
                            event_name,
                        ),
                    )
                    created_ticket_ids.append(ticket_id)
                except Exception as exc:
                    print(f"[ticket-create] insert failed: {exc}")

        if created_ticket_ids:
            cursor.connection.commit()
            print(f"[ticket-create] insert success ticket_ids={created_ticket_ids}")
            return created_ticket_ids

    ticket_name = parse_ticket_from_note(payment.get("note")) or map_ticket_from_amount(amount_cents)
    print(f"[ticket-create] payment_id={payment_id} amount_cents={amount_cents} mapped_ticket={ticket_name} event_name={event_name}")
    if not ticket_name:
        print("[ticket-create] skip insert: missing ticket mapping")
        return []
    quantity = extract_quantity_from_payment(payment)
    print(f"Creating {quantity} tickets for payment {payment_id}")
    created = []
    for i in range(quantity):
        ticket_id = generate_ticket(payment_id, i)
        cursor.execute("SELECT 1 FROM event_tickets WHERE ticket_id = ?", (ticket_id,))
        if cursor.fetchone():
            continue
        checkin_url = f"{base_url}/checkin/{ticket_id}"
        qr_url = f"{base_url}/qr/{ticket_id}"
        print("QR GENERATED:", qr_url)
        try:
            print("CREATING TICKET:", ticket_id)
            print("EMAIL:", buyer_email.lower())
            print("PAYMENT:", payment_id)
            cursor.execute(
                """
                INSERT INTO event_tickets (
                    name, email, ticket_type, amount_cents, ticket_id, status,
                    payment_id, checkin_url, qr_url, event_name, checked_in
                ) VALUES (?, ?, ?, ?, ?, 'not_checked_in', ?, ?, ?, ?, 0)
                """,
                (
                    full_name,
                    buyer_email.lower(),
                    ticket_name,
                    int(amount_cents or 0),
                    ticket_id,
                    f"{payment_id}:{i}",
                    checkin_url,
                    qr_url,
                    event_name,
                ),
            )
            created.append(ticket_id)
        except Exception as exc:
            print(f"[ticket-create] insert failed: {exc}")
    if created:
        cursor.connection.commit()
        print(f"[ticket-create] insert success ticket_ids={created}")
    return created


def recover_missing_tickets(cursor):
    created_total = 0
    cursor.execute(
        """
        SELECT payment_id
        FROM square_payment_log
        WHERE LOWER(category) = 'ticket'
        ORDER BY created_at DESC
        """
    )
    payment_rows = cursor.fetchall()
    for (logged_payment_id,) in payment_rows:
        if not logged_payment_id:
            continue
        payment_id = str(logged_payment_id).split(":", 1)[0].strip()
        if not payment_id:
            continue
        cursor.execute(
            "SELECT COUNT(*) FROM event_tickets WHERE payment_id = ? OR payment_id LIKE ?",
            (payment_id, f"{payment_id}:%"),
        )
        existing_count = int(cursor.fetchone()[0] or 0)
        if existing_count > 0:
            continue

        payment = square_retrieve_payment(payment_id)
        if not payment:
            continue
        status = (payment.get("status") or "").strip().upper()
        if status != "COMPLETED":
            continue
        amount_cents = int(payment.get("amount_money", {}).get("amount") or 0)
        email = (payment.get("buyer_email_address") or "").strip()
        created = create_ticket_from_square_payment(cursor, payment, amount_cents, email) or []
        created_total += len(created)
    return created_total


def load_customer_tickets(cursor, include_checked_in=False, target_email=None):
    where = []
    params = []
    if not include_checked_in:
        where.append("COALESCE(checked_in, 0) = 0 AND COALESCE(status, 'not_checked_in') != 'checked_in'")
    if target_email:
        where.append("LOWER(email) = ?")
        params.append(target_email.lower())
    where_clause = ("WHERE " + " AND ".join(where)) if where else ""
    cursor.execute(
        f"""
        SELECT email, ticket_id, ticket_type, qr_url, payment_id
        FROM event_tickets
        {where_clause}
        ORDER BY email ASC, id ASC
        """,
        tuple(params),
    )
    rows = cursor.fetchall()
    grouped = {}
    for email, ticket_id, ticket_type, qr_url, payment_id in rows:
        email_key = (email or "").strip().lower()
        if not email_key or not ticket_id:
            continue
        grouped.setdefault(email_key, []).append(
            {
                "ticket_id": ticket_id,
                "ticket_type": ticket_type,
                "qr_url": qr_url or f"https://www.jukeboxloungenc.com/qr/{ticket_id}",
                "payment_id": payment_id,
            }
        )
    return grouped


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
    # Source-of-truth lock: sold counters are derived from event_tickets rows only.
    return False


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


def square_retrieve_order(order_id):
    if not SQUARE_ACCESS_TOKEN or not order_id:
        return {}

    endpoint = f"{square_base_url()}/v2/orders/{order_id}"
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
            return payload.get("order", {}) or {}
    except urlerror.HTTPError as exc:
        print("Square retrieve order HTTP error:", exc.code)
    except Exception as exc:
        print("Square retrieve order error:", exc)
    return {}


def square_retrieve_payment(payment_id):
    if not SQUARE_ACCESS_TOKEN or not payment_id:
        return {}

    endpoint = f"{square_base_url()}/v2/payments/{payment_id}"
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
            return payload.get("payment", {}) or {}
    except Exception as exc:
        print("Square retrieve payment error:", exc)
    return {}


def parse_line_item_quantity(item):
    raw = (item or {}).get("quantity", 1)
    try:
        qty = int(str(raw))
    except Exception:
        qty = 1
    return max(1, qty)


def line_item_ticket_name(item):
    name = ((item or {}).get("name") or "").strip()
    mapped = SQUARE_TO_DB_MAP.get(name, name)
    if mapped in CANONICAL_TICKET_TYPES:
        return mapped
    return None


def parse_qty_from_note(note):
    raw = str(note or "")
    if "qty:" not in raw:
        return 1
    try:
        value = raw.split("qty:", 1)[1].split(";", 1)[0].strip()
        qty = int(value)
        return max(1, qty)
    except Exception:
        return 1


def parse_ticket_from_note(note):
    raw = str(note or "")
    if "ticket:" not in raw:
        return None
    try:
        value = raw.split("ticket:", 1)[1].split(";", 1)[0].strip()
    except Exception:
        value = ""
    mapped = SQUARE_TO_DB_MAP.get(value, value)
    if mapped in CANONICAL_TICKET_TYPES:
        return mapped
    return None


def extract_quantity_from_payment(payment):
    # 1) Direct quantity field (if upstream provided)
    raw = (payment or {}).get("quantity", None)
    if raw not in (None, ""):
        try:
            return max(1, int(str(raw)))
        except Exception:
            pass

    # 2) Sum Square order line item quantities when available
    order = square_retrieve_order((payment or {}).get("order_id", ""))
    line_items = order.get("line_items", []) if isinstance(order, dict) else []
    if line_items:
        total = 0
        for item in line_items:
            total += parse_line_item_quantity(item)
        if total > 0:
            return total

    # 3) qty:<n> encoded in note fallback
    return parse_qty_from_note((payment or {}).get("note"))



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

    conn = sqlite3.connect(DB_PATH)
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

        if kind in ("ticket", "unmatched"):
            if dry_run:
                ticket_created = (kind == "ticket")
                ticket_hit = (kind == "ticket")
            else:
                created_ticket_id = create_ticket_from_square_payment(cursor, payment, amount_cents, email)
                ticket_created = bool(created_ticket_id)
                if kind == "ticket":
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
        "early_link": "https://square.link/u/tad1OGER",
        "ga_link": "https://square.link/u/Q20QaK53",
        "vip_link": "https://square.link/u/2stAPuXv",
        "booth_link": "https://square.link/u/TsOHYIEp",

        "tickets": {
            "early": {"price": 13, "sold": 0, "size": 1},
            "ga": {"price": 18, "sold": 0, "size": 366},
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
        "ga_link": "https://square.link/u/wd0IDb7U",
        "vip_link": "https://square.link/u/p4eAdd8g",
        "booth_link": "https://square.link/u/p4eAdd8g",
        "tickets": {
            "early": {"price": 13, "sold": 0, "size": 1},
            "ga": {"price": 18, "sold": 0, "size": 1},
            "vip": {"price": 175, "sold": 0, "size": 1},
            "booth": {"price": 200, "sold": 0, "size": 6}
        }
    },
    {
        "id": 3,
        "name": "Juneteenth Celebration",
        "flyer": "/static/images/flyer-juneteenth-finale.png",
        "description": "The Jukebox Lounge NC presents Grown & Sexy: Melanin — the official finale of our 3-Part Grand Opening Series and Juneteenth Weekend Celebration. Join us June 20th at West End Social for a classy 30+ experience celebrating culture, confidence, music, and all shades of beautiful. Dressy casual to upscale attire encouraged. No athletic wear or ball caps.",
        "ticket_link": "https://square.link/u/51KF7WKE",
        "event_datetime": "June 20, 2026",
        "location": "West End Social",
        "time": "8:00 PM - Late",
        "doors": "8:00 PM",
        "ticket_label": "Grown & Sexy",
        "map_link": "https://www.google.com/maps/search/?api=1&query=West+End+Social",
        "early_link": "https://square.link/u/51KF7WKE",
        "ga_link": "https://square.link/u/51KF7WKE",
        "vip_link": "https://square.link/u/51KF7WKE",
        "booth_link": "https://square.link/u/51KF7WKE",
        "tickets": {
            "early": {"price": 35, "sold": 0, "size": 100},
            "ga": {"price": 35, "sold": 0, "size": 100},
            "vip": {"price": 35, "sold": 0, "size": 100},
            "booth": {"price": 35, "sold": 0, "size": 100}
        }
    },
]


# -------------------------
# HOME
# -------------------------
@app.route("/")
def home():
    return render_template("index.html")

@app.route("/dashboard")
@requires_auth
def dashboard():
    single_tickets = 187
    vip_tickets = 6
    total_tickets_sold = single_tickets + vip_tickets
    estimated_attendance = total_tickets_sold
    active_memberships = 1

    ticket_revenue = 4515.15
    membership_revenue = 10.00
    total_revenue = 4525.15

    metrics = {
        "single_tickets": single_tickets,
        "total_tickets_sold": total_tickets_sold,
        "vip_tickets": vip_tickets,
        "estimated_attendance": estimated_attendance,
        "ticket_revenue": ticket_revenue,
        "active_memberships": active_memberships,
        "membership_revenue": membership_revenue,
        "total_revenue": total_revenue,
    }

    events = [
        {
            "name": "Battle of the DJs",
            "tickets": [
                {"name": "Early Bird", "quantity": 22, "price": 14.0204545455},
                {"name": "General Admissions", "quantity": 37, "price": 19.0459459459},
                {"name": "VIP Section", "quantity": 6, "price": 158.3333333333},
                {"name": "DJ VIP", "quantity": 0, "price": 0},
                {
                    "name": "Door Sales",
                    "quantity": 127,
                    "price": 0,
                    "revenue_override": 2540.00,
                    "cash_amount": 1320.00,
                    "square_amount": 1220.00,
                },
            ],
        },
        {
            "name": "Quiet Storm Live",
            "tickets": [
                {"name": "General Admission", "quantity": 1, "price": 12},
            ],
        },
    ]

    for event in events:
        event_total_tickets = 0
        event_total_revenue = 0
        for ticket in event["tickets"]:
            ticket_revenue_generated = ticket.get("revenue_override", ticket["quantity"] * ticket["price"])
            ticket["revenue"] = ticket_revenue_generated
            event_total_tickets += ticket["quantity"]
            event_total_revenue += ticket_revenue_generated
        event["total_tickets_sold"] = event_total_tickets
        event["total_revenue"] = event_total_revenue

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT COUNT(*)
        FROM leads
        WHERE type = 'Membership Signup'
          AND status = 'Active'
        """
    )
    membership_log_count = int(cursor.fetchone()[0] or 0)

    cursor.execute(
        """
        SELECT DISTINCT LOWER(TRIM(email))
        FROM leads
        WHERE type = 'VIP Signup'
          AND LOWER(COALESCE(status, '')) = 'active'
          AND email IS NOT NULL
          AND TRIM(email) <> ''
        ORDER BY LOWER(TRIM(email))
        """
    )
    vip_recipients = [r[0] for r in cursor.fetchall() if r and r[0]]

    cursor.execute(
        """
        SELECT DISTINCT LOWER(TRIM(email))
        FROM leads
        WHERE type = 'Membership Signup'
          AND LOWER(COALESCE(status, '')) = 'active'
          AND email IS NOT NULL
          AND TRIM(email) <> ''
        ORDER BY LOWER(TRIM(email))
        """
    )
    membership_recipients = [r[0] for r in cursor.fetchall() if r and r[0]]
    conn.close()

    active_members = []
    vip_members = []
    membership_log_members = []

    event_demand_votes = []
    total_demand_votes = 0

    active_suggestions = []
    archived_suggestions = []

    return render_template(
        "dashboard.html",
        metrics=metrics,
        events=events,
        vip_members=vip_members,
        membership_log_members=membership_log_members,
        vip_count=len(vip_members),
        membership_count=membership_log_count,
        membership_log_count=membership_log_count,
        event_demand_votes=event_demand_votes,
        total_demand_votes=total_demand_votes,
        active_suggestions=active_suggestions,
        archived_suggestions=archived_suggestions,
        vip_recipients=vip_recipients,
        membership_recipients=membership_recipients,
        square_connected=False,
    )


@app.route("/bookkeeping")
@requires_auth
def bookkeeping():
    door_sales = [
        {
            "date": "2026-05-17",
            "event_name": "Battle of the DJs",
            "ticket_type": "General Admission",
            "quantity": 5,
            "payment_method": "Cash",
            "amount": 100.00,
            "staff_member": "Ashley",
            "notes": "Front door walk-ups",
        },
        {
            "date": "2026-05-17",
            "event_name": "Battle of the DJs",
            "ticket_type": "General Admission",
            "quantity": 2,
            "payment_method": "Cash App",
            "amount": 40.00,
            "staff_member": "Keeva",
            "notes": "Late arrivals",
        },
    ]

    door_sales_revenue = round(sum(item["amount"] for item in door_sales), 2)
    ticket_revenue = round(1769.75 + door_sales_revenue, 2)
    membership_revenue = 10.00
    total_revenue = round(ticket_revenue + membership_revenue, 2)
    total_expenses = 642.90
    net_profit = round(total_revenue - total_expenses, 2)

    summary = {
        "total_revenue": total_revenue,
        "total_expenses": total_expenses,
        "net_profit": net_profit,
        "ticket_revenue": ticket_revenue,
        "membership_revenue": membership_revenue,
        "door_sales_revenue": door_sales_revenue,
    }

    transactions = [
        {
            "date": "2026-05-17",
            "type": "Income",
            "category": "Ticket Sales",
            "description": "Battle of the DJs online + door ticket sales",
            "amount": 1769.75,
            "payment_method": "Square / Cash",
            "related_event": "Battle of the DJs",
            "notes": "Includes Early Bird, GA, VIP.",
        },
        {
            "date": "2026-05-17",
            "type": "Expense",
            "category": "Talent",
            "description": "DJ performance payout",
            "amount": 350.00,
            "payment_method": "Zelle",
            "related_event": "Battle of the DJs",
            "notes": "Main set payment.",
        },
        {
            "date": "2026-05-17",
            "type": "Expense",
            "category": "Venue",
            "description": "Venue usage fee",
            "amount": 180.00,
            "payment_method": "Card",
            "related_event": "Battle of the DJs",
            "notes": "Night-of event fee.",
        },
        {
            "date": "2026-05-17",
            "type": "Expense",
            "category": "Marketing",
            "description": "Flyer + ad spend",
            "amount": 112.90,
            "payment_method": "Card",
            "related_event": "Battle of the DJs",
            "notes": "Social promotion and print.",
        },
        {
            "date": "2026-05-16",
            "type": "Income",
            "category": "Membership",
            "description": "Jukebox Circle monthly membership",
            "amount": 10.00,
            "payment_method": "Square",
            "related_event": "N/A",
            "notes": "Recurring member payment.",
        },
    ]

    return render_template(
        "bookkeeping.html",
        summary=summary,
        transactions=transactions,
        door_sales=door_sales,
    )


# -------------------------
# EVENTS
# -------------------------
@app.route("/events")
def events():
    conn = sqlite3.connect(DB_PATH)
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
        try:
            name = request.form.get("name")
            email = request.form.get("email")
            message = request.form.get("message")
            create_lead_record("Contact Message", name, email, message, "New")
        except Exception as exc:
            print("[contact] submit failed:", exc)
            traceback.print_exc()
        return render_thank_you_safe(
            "MESSAGE RECEIVED",
            "Your message has been sent. Our team will get back to you shortly.",
        )

    return render_template("contact.html")


@app.route("/checkin")
def checkin_page():
    message = request.args.get("msg", "").strip()
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    enforce_vip_group_capacity(cursor)
    conn.commit()
    cursor.execute(
        """
        SELECT
            id,
            event_name,
            COALESCE(NULLIF(TRIM(customer_name), ''), name) AS name,
            ticket_type,
            quantity,
            checked_in_count,
            status
        FROM attendees
        ORDER BY COALESCE(NULLIF(TRIM(customer_name), ''), name) COLLATE NOCASE ASC
        """
    )
    attendees = cursor.fetchall()
    conn.close()
    normalized_attendees = []
    for row in attendees:
        item = dict(row)
        ticket_type = (item.get("ticket_type") or "").strip()
        quantity = max(1, int(item.get("quantity") or 1))
        checked_in_count = max(0, int(item.get("checked_in_count") or 0))

        # VIP rows are group tickets by default.
        if "vip" in ticket_type.lower() and quantity < 6:
            quantity = 6

        checked_in_count = min(checked_in_count, quantity)
        if checked_in_count <= 0:
            status = "Not Checked In"
        elif checked_in_count >= quantity:
            status = "Checked In"
        else:
            status = "Partially Checked In"

        item["quantity"] = quantity
        item["checked_in_count"] = checked_in_count
        item["status"] = status
        normalized_attendees.append(item)

    return render_template("checkin.html", attendees=normalized_attendees, message=message)


@app.route("/checkin/add", methods=["POST"])
def add_attendee_manual():
    event_name = (request.form.get("event_name") or "").strip() or "Battle of the DJs"
    customer_name = (request.form.get("customer_name") or "").strip()
    ticket_type = (request.form.get("ticket_type") or "").strip()
    raw_quantity = (request.form.get("quantity") or "1").strip()

    if not customer_name or not ticket_type:
        return redirect("/checkin?msg=Name and ticket type are required.")

    try:
        quantity = int(float(raw_quantity))
    except Exception:
        quantity = 1
    if quantity < 1:
        quantity = 1
    if "vip" in ticket_type.lower() and quantity < 6:
        quantity = 6

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO attendees (event_name, name, customer_name, ticket_type, quantity, checked_in_count, status)
        VALUES (?, ?, ?, ?, ?, 0, 'Not Checked In')
        """,
        (event_name, customer_name, customer_name, ticket_type, quantity),
    )
    conn.commit()
    conn.close()
    return redirect("/checkin?msg=Ticket added successfully.")


@app.route("/checkin/<int:id>")
def checkin_attendee(id):
    update_attendee_checkin_count(id, 1)
    return redirect("/checkin")


@app.route("/reset-checkin/<int:id>")
def reset_checkin(id):
    update_attendee_checkin_count(id, -1)
    return redirect("/checkin")


def update_attendee_checkin_count(attendee_id, delta):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT quantity, checked_in_count, ticket_type
        FROM attendees
        WHERE id = ?
        """,
        (attendee_id,),
    )
    row = cursor.fetchone()
    if not row:
        conn.close()
        return None

    quantity = max(1, int(row[0] or 1))
    checked_in_count = max(0, int(row[1] or 0))
    ticket_type = str(row[2] or "").strip()
    if "vip" in ticket_type.lower() and quantity < 6:
        quantity = 6

    next_count = checked_in_count + int(delta)
    if next_count < 0:
        next_count = 0
    if next_count > quantity:
        next_count = quantity

    if next_count <= 0:
        status = "Not Checked In"
    elif next_count >= quantity:
        status = "Checked In"
    else:
        status = "Partially Checked In"

    cursor.execute(
        """
        UPDATE attendees
        SET quantity = ?,
            checked_in_count = ?,
            status = ?
        WHERE id = ?
        """,
        (quantity, next_count, status, attendee_id),
    )
    conn.commit()
    conn.close()
    return {"id": attendee_id, "quantity": quantity, "checked_in_count": next_count, "status": status}


@app.route("/checkin-action/<int:id>", methods=["POST"])
def checkin_action(id):
    action = (request.form.get("action") or "").strip().lower()
    delta = 1 if action == "checkin" else -1 if action == "undo" else 0
    if delta == 0:
        return {"ok": False, "error": "invalid_action"}, 400
    updated = update_attendee_checkin_count(id, delta)
    if not updated:
        return {"ok": False, "error": "not_found"}, 404
    return {"ok": True, **updated}


def enforce_vip_group_capacity(cursor):
    cursor.execute(
        """
        UPDATE attendees
        SET quantity = 6
        WHERE UPPER(COALESCE(ticket_type, '')) LIKE '%VIP%'
          AND COALESCE(quantity, 1) < 6
        """
    )
    cursor.execute(
        """
        UPDATE attendees
        SET status = CASE
            WHEN COALESCE(checked_in_count, 0) <= 0 THEN 'Not Checked In'
            WHEN COALESCE(checked_in_count, 0) >= COALESCE(quantity, 1) THEN 'Checked In'
            ELSE 'Partially Checked In'
        END
        WHERE UPPER(COALESCE(ticket_type, '')) LIKE '%VIP%'
        """
    )


@app.route("/debug-attendees", methods=["GET"], strict_slashes=False)
@app.route("/debug_attendees", methods=["GET"], strict_slashes=False)
def debug_attendees():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    enforce_vip_group_capacity(cursor)
    conn.commit()
    cursor.execute("SELECT * FROM attendees")
    rows = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return {"status": "ok", "count": len(rows), "rows": rows}


@app.route("/admin/storage-status")
@requires_auth
def admin_storage_status():
    db_abspath = os.path.abspath(DB_PATH)
    exists = os.path.exists(db_abspath)
    writable = os.access(os.path.dirname(db_abspath) or ".", os.W_OK)
    return {
        "db_path": DB_PATH,
        "db_abspath": db_abspath,
        "exists": exists,
        "parent_writable": writable,
        "note": "If db_path points to local ephemeral filesystem on Render, data can reset on deploy."
    }, 200


@app.route("/upload-attendees", methods=["POST"])
def upload_attendees():
    event_name = "Battle of the DJs"
    file = request.files.get("attendee_csv")

    if not file or not file.filename.lower().endswith(".csv"):
        return redirect("/checkin?msg=Please upload a valid CSV file.")

    raw = file.read()
    if not raw:
        return redirect("/checkin?msg=CSV file is empty.")

    text = raw.decode("utf-8-sig", errors="ignore")
    reader = csv.DictReader(io.StringIO(text))

    inserted = 0
    skipped = 0
    skipped_missing_name = 0
    skipped_missing_ticket_type = 0

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    for row in reader:
        if not row:
            skipped += 1
            print("[upload-attendees] skip: empty row")
            continue

        name = ((row.get("Order Name") or row.get("Recipient Name") or "")).strip()
        email = ((row.get("Recipient Email") or "")).strip()
        ticket_type = ((row.get("Item Name") or "")).strip()
        raw_quantity = str(row.get("Item Quantity") or "1").strip()

        if not name:
            skipped += 1
            skipped_missing_name += 1
            print("[upload-attendees] skip: missing name")
            continue

        if not ticket_type:
            skipped += 1
            skipped_missing_ticket_type += 1
            print(f"[upload-attendees] skip: missing ticket_type for name={name} email={email}")
            continue

        try:
            quantity = int(float(raw_quantity))
        except Exception:
            quantity = 1
        if quantity < 1:
            quantity = 1
        if "vip" in ticket_type.lower() and quantity < 6:
            quantity = 6

        cursor.execute(
            """
            SELECT id
            FROM attendees
            WHERE LOWER(COALESCE(event_name, '')) = LOWER(?)
              AND LOWER(COALESCE(customer_name, name, '')) = LOWER(?)
              AND LOWER(COALESCE(ticket_type, '')) = LOWER(?)
              AND COALESCE(quantity, 1) = ?
            LIMIT 1
            """,
            (event_name, name, ticket_type, quantity),
        )
        if cursor.fetchone():
            skipped += 1
            print(f"[upload-attendees] skip: duplicate row for {name} | {ticket_type} | qty={quantity}")
            continue

        cursor.execute(
            """
            INSERT INTO attendees (
                event_name,
                name,
                customer_name,
                ticket_type,
                quantity,
                checked_in_count,
                status
            )
            VALUES (?, ?, ?, ?, ?, 0, 'Not Checked In')
            """,
            (event_name, name, name, ticket_type, quantity),
        )
        inserted += 1
        print(f"[upload-attendees] insert: {name} | {email} | {ticket_type} | qty={quantity}")

    conn.commit()
    conn.close()

    print(f"[upload-attendees] inserted={inserted}")
    print(f"[upload-attendees] skipped={skipped}")
    print(f"[upload-attendees] skipped_missing_name={skipped_missing_name}")
    print(f"[upload-attendees] skipped_missing_ticket_type={skipped_missing_ticket_type}")

    return redirect(f"/checkin?msg=Attendees Imported Successfully ({inserted} added, {skipped} skipped).")


@app.route('/debug-csv', methods=['GET', 'POST'])
def debug_csv():

    if request.method == 'GET':
        return '''
        <form method="POST" enctype="multipart/form-data">
            <input type="file" name="file">
            <button type="submit">Upload CSV for Debug</button>
        </form>
        '''

    import csv
    import sqlite3

    file = request.files['file']
    decoded = file.read().decode('utf-8').splitlines()
    reader = csv.DictReader(decoded)

    first_row = next(reader)

    return {
        "columns": list(first_row.keys()),
        "sample_row": first_row
    }


@app.route("/admin/import-vip", methods=["GET", "POST"])
@requires_auth
def import_vip():
    if request.method == "GET":
        return """
        <html>
          <body style="font-family:Arial;background:#0b0b0b;color:#f2f2f2;padding:24px;">
            <h2 style="color:#D4AF37;">Temporary VIP Import</h2>
            <p>Upload your exported VIP CSV or ZIP file (contains subscribed_email_audience_*.csv).</p>
            <form method="POST" enctype="multipart/form-data">
              <input type="file" name="file" accept=".csv,.zip" required>
              <button type="submit">Import VIP List</button>
            </form>
          </body>
        </html>
        """

    file = request.files.get("file")
    if not file or not file.filename:
        return {"ok": False, "error": "No file uploaded"}, 400

    filename = file.filename.lower()
    raw = file.read()
    if not raw:
        return {"ok": False, "error": "Empty file"}, 400

    csv_text = ""
    if filename.endswith(".zip"):
        import zipfile
        with zipfile.ZipFile(io.BytesIO(raw)) as zf:
            csv_names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
            if not csv_names:
                return {"ok": False, "error": "No CSV found in zip"}, 400
            csv_text = zf.read(csv_names[0]).decode("utf-8-sig", errors="ignore")
    else:
        csv_text = raw.decode("utf-8-sig", errors="ignore")

    rows = list(csv.DictReader(io.StringIO(csv_text)))
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    inserted = 0
    skipped = 0

    for row in rows:
        email = (row.get("Email Address") or "").strip().lower()
        name = (row.get("First Name") or "").strip() or "VIP Member"
        phone = (row.get("Phone Number") or "").strip()
        if not email:
            skipped += 1
            continue

        cursor.execute(
            """
            SELECT 1 FROM leads
            WHERE LOWER(COALESCE(email, '')) = ?
              AND type = 'VIP Signup'
            LIMIT 1
            """,
            (email,),
        )
        if cursor.fetchone():
            skipped += 1
            continue

        details = f"Phone: {phone}" if phone else "Phone: "
        cursor.execute(
            """
            INSERT INTO leads (type, name, email, details, status, archived, archived_at, created_at)
            VALUES ('VIP Signup', ?, ?, ?, 'Active', 0, NULL, CURRENT_TIMESTAMP)
            """,
            (name, email, details),
        )
        inserted += 1

    conn.commit()
    cursor.execute("SELECT COUNT(*) FROM leads WHERE type='VIP Signup' AND status='Active'")
    active_total = cursor.fetchone()[0]
    conn.close()

    return {
        "ok": True,
        "inserted": inserted,
        "skipped": skipped,
        "active_vip_total": active_total,
        "note": "Temporary endpoint: remove /admin/import-vip after successful upload."
    }


# -------------------------
# MEMBERSHIP PAGE
# -------------------------
@app.route("/membership")
def membership():
    return render_template("membership.html")

# -------------------------
# MERCH PAGE
# -------------------------
@app.route("/merch")
def merch():
    merch_items = [
        {
            "id": "nc-born-bull-city-olive-1",
            "name": "NC Born. Bull City Made Tee",
            "color": "Garment-Dyed Olive",
            "image": "images/merch-olive-bull-city-1.png",
            "details": ["Vintage Wash Finish", "Front & Back Prints", "Unisex Fit", "Soft, Pre-Shrunk Cotton"],
            "price": "$35.00",
            "checkout_link": os.getenv("MERCH_LINK_NC_BORN_1", "").strip(),
        },
        {
            "id": "jukebox-charcoal-vinyl",
            "name": "Jukebox Lounge NC Tee",
            "color": "Garment-Dyed Charcoal",
            "image": "images/merch-charcoal-vinyl.png",
            "details": ["Vintage Wash Finish", "Front & Back Prints", "Unisex Fit"],
            "price": "$35.00",
            "checkout_link": os.getenv("MERCH_LINK_CHARCOAL", "").strip(),
        },
        {
            "id": "jukebox-olive-good-vibes",
            "name": "The Jukebox Lounge NC Tee",
            "color": "Garment-Dyed Olive",
            "image": "images/merch-olive-good-vibes.png",
            "details": ["Vintage Wash Finish", "Front & Back Prints", "Unisex Fit", "Soft, Pre-Shrunk Cotton"],
            "price": "$35.00",
            "checkout_link": os.getenv("MERCH_LINK_GOOD_VIBES", "").strip(),
        },
        {
            "id": "nc-born-bull-city-olive-2",
            "name": "NC Born. Bull City Made Tee",
            "color": "Garment-Dyed Olive",
            "image": "images/merch-olive-bull-city-2.png",
            "details": ["Vintage Wash Finish", "Front & Back Prints", "Unisex Fit", "Soft, Pre-Shrunk Cotton"],
            "price": "$35.00",
            "checkout_link": os.getenv("MERCH_LINK_NC_BORN_2", "").strip(),
        },
        {
            "id": "jukebox-black-durham",
            "name": "The Jukebox Lounge NC Tee",
            "color": "Garment-Dyed Black",
            "image": "images/merch-black-durham.png",
            "details": ["Vintage Wash Finish", "Front & Back Prints", "Unisex Fit", "Soft, Pre-Shrunk Cotton"],
            "price": "$35.00",
            "checkout_link": os.getenv("MERCH_LINK_BLACK_DURHAM", "").strip(),
        },
        {
            "id": "jukebox-olive-never-old",
            "name": "Jukebox Lounge NC Tee",
            "color": "Garment-Dyed Olive",
            "image": "images/merch-olive-jukebox-never-old.png",
            "details": ["Vintage Wash Finish", "Front, Back & Sleeve Prints", "Unisex Fit"],
            "price": "$35.00",
            "checkout_link": os.getenv("MERCH_LINK_NEVER_OLD", "").strip(),
        },
    ]
    return render_template("merch.html", merch_items=merch_items)

# -------------------------
# MERCH CHECKOUT
# -------------------------
@app.route("/merch/checkout", methods=["POST"])
def merch_checkout():
    merch_catalog = {
        "nc-born-bull-city-olive-1": {
            "name": "NC Born. Bull City Made Tee",
            "color": "Garment-Dyed Olive",
            "link": os.getenv("MERCH_LINK_NC_BORN_1", "").strip(),
        },
        "jukebox-charcoal-vinyl": {
            "name": "Jukebox Lounge NC Tee",
            "color": "Garment-Dyed Charcoal",
            "link": os.getenv("MERCH_LINK_CHARCOAL", "").strip(),
        },
        "jukebox-olive-good-vibes": {
            "name": "The Jukebox Lounge NC Tee",
            "color": "Garment-Dyed Olive",
            "link": os.getenv("MERCH_LINK_GOOD_VIBES", "").strip(),
        },
        "nc-born-bull-city-olive-2": {
            "name": "NC Born. Bull City Made Tee",
            "color": "Garment-Dyed Olive",
            "link": os.getenv("MERCH_LINK_NC_BORN_2", "").strip(),
        },
        "jukebox-black-durham": {
            "name": "The Jukebox Lounge NC Tee",
            "color": "Garment-Dyed Black",
            "link": os.getenv("MERCH_LINK_BLACK_DURHAM", "").strip(),
        },
        "jukebox-olive-never-old": {
            "name": "Jukebox Lounge NC Tee",
            "color": "Garment-Dyed Olive",
            "link": os.getenv("MERCH_LINK_NEVER_OLD", "").strip(),
        },
    }

    item_id = (request.form.get("item_id") or "").strip()
    size = (request.form.get("size") or "").strip().upper()
    quantity_raw = (request.form.get("quantity") or "1").strip()
    customer_name = (request.form.get("customer_name") or "").strip()
    customer_email = (request.form.get("customer_email") or "").strip().lower()

    item = merch_catalog.get(item_id)
    if not item:
        return redirect("/merch")

    try:
        quantity = int(quantity_raw)
    except Exception:
        quantity = 1
    if quantity < 1:
        quantity = 1
    if quantity > 10:
        quantity = 10

    details = (
        f"Product: {item['name']} ({item['color']})\n"
        f"Size: {size or 'N/A'}\n"
        f"Quantity: {quantity}\n"
        f"Source: Merch Checkout"
    )
    create_lead_record("Merch Order", customer_name or "Customer", customer_email, details, "New")

    # Fallback to a shared merch link if product-specific link is not set yet.
    checkout_link = item.get("link") or os.getenv("MERCH_LINK_DEFAULT", "").strip()
    if checkout_link and checkout_link.startswith("http"):
        return redirect(checkout_link)

    return render_thank_you_safe(
        "ORDER STARTED",
        "Your order details were saved. Add your Square merch link env vars to enable direct checkout.",
    )

# -------------------------
# JOIN MEMBERSHIP
# -------------------------
@app.route("/join-membership", methods=["POST"])
def join_membership():
    try:
        name = request.form.get("name")
        email = request.form.get("email")
        create_lead_record("Membership Signup", name, email, "Waiting for payment", "Active")
        send_membership_welcome_email(name, email)
    except Exception as exc:
        print("[join-membership] submit failed:", exc)
        traceback.print_exc()

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

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    payment, payment_id, amount_cents, note_blob, email, status = parse_square_payment(data)
    is_duplicate_payment = already_logged_payment(cursor, payment_id) if payment_id else False
    square_name = extract_square_name(payment)
    mapped_ticket_type = SQUARE_TO_DB_MAP.get(square_name, square_name)
    debug_quantity = extract_quantity_from_payment(payment)
    debug_ticket_type = parse_ticket_from_note(payment.get("note")) or map_ticket_from_amount(amount_cents)

    print("DEBUG STATUS:", status)
    print("DEBUG PAYMENT ID:", payment_id)
    print("DEBUG AMOUNT:", amount_cents)
    print("DEBUG EMAIL:", email)
    print("DEBUG DUPLICATE PAYMENT:", is_duplicate_payment)
    print("DEBUG SQUARE ITEM NAME:", square_name)
    print("DEBUG MAPPED TICKET TYPE:", mapped_ticket_type)
    print("PAYMENT RECEIVED:", payment_id)
    print("EMAIL:", email)
    print("TICKET TYPE:", debug_ticket_type)
    print("QUANTITY:", debug_quantity)

    cursor.execute(
        """
        INSERT INTO webhook_logs (source, event_id, event_type, note)
        VALUES (?, ?, ?, ?)
        """,
        ("square", event_id, event_type, f"status={status} payment_id={payment_id}"),
    )

    ticket_ids = []
    print("REACHED TICKET LOGIC")
    print("STATUS:", status)
    print("EVENT:", event_type)

    if not payment_id:
        print("Missing payment_id; skipping ticket create.")
    elif status == "COMPLETED" and event_type in ("payment.updated", "payment.created"):
        print("CREATING TICKET")
        try:
            ticket_ids = create_ticket_from_square_payment(cursor, payment, amount_cents, email) or []
        except Exception as exc:
            print("❌ TICKET INSERT FAILED:", exc)
            ticket_ids = []
        print("TICKET IDS:", ticket_ids)
        print("TICKETS CREATED:", len(ticket_ids))
        if ticket_ids:
            print("SENDING EMAIL")
            send_tickets_email_bundle(cursor, payment_id, email, event_name_from_payment(payment))
    else:
        print("Ticket condition not met.")

    ticket_hit = apply_ticket_sale_from_square(cursor, payment)
    membership_hit = apply_membership_from_square(cursor, payment, amount_cents, note_blob, email)

    if payment_id and not is_duplicate_payment and (ticket_hit or membership_hit or ticket_ids):
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
@app.route("/dj-signup", methods=["GET", "POST"])
def dj_signup():

    if request.method == "POST":
        try:
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

            create_lead_record("DJ Application", name, email, details, "New")
        except Exception as exc:
            print("[dj-signup] submit failed:", exc)
            traceback.print_exc()

        return render_thank_you_safe(
            "APPLICATION RECEIVED",
            "Your application has been submitted successfully. Our team will review your sound and reach out if you're a fit for an upcoming Jukebox experience.",
        )

    return render_template("dj_signup.html")


@app.route("/vendor-signup", methods=["GET", "POST"])
def vendor_signup():
    if request.method == "POST":
        try:
            name = request.form.get("name")
            business = request.form.get("business")
            email = request.form.get("email")
            phone = request.form.get("phone")
            vendor_type = request.form.get("vendor_type")
            products = request.form.get("products")
            links = request.form.get("links")
            setup = request.form.get("setup")
            comments = request.form.get("comments")

            details = f"""
            Business: {business}
            Phone: {phone}
            Vendor Type: {vendor_type}
            Products/Services: {products}
            Links: {links}
            Setup Needs: {setup}
            Comments: {comments}
            """
            create_lead_record("Vendor Application", name, email, details, "New")
        except Exception as exc:
            print("[vendor-signup] submit failed:", exc)
            traceback.print_exc()

        return render_thank_you_safe(
            "APPLICATION RECEIVED",
            "Your vendor application has been submitted successfully. Our team will review and reach out with next steps.",
        )

    return render_template("vendor_signup.html")

@app.route("/vip", methods=["POST"])
def vip_signup():
    name = request.form.get("name")
    email = request.form.get("email")
    phone = request.form.get("phone")

    details = f"""
    Phone: {phone}
    """

    try:
        create_lead_record("VIP Signup", name, email, details, "Active")
    except Exception as exc:
        print("[vip-signup] submit failed:", exc)
        traceback.print_exc()

    welcome_plain = (
        f"Hi {name or 'VIP Member'},\n\n"
        "Welcome to The Jukebox Lounge VIP Email List.\n"
        "You now have access to exclusive drops, early announcements, and curated experiences.\n\n"
        "Stay tuned — we have something special lined up.\n\n"
        "See you soon,\n"
        "The Jukebox Lounge NC"
    )

    welcome_html = f"""
    <html>
      <body style="margin:0;padding:0;background:#070707;font-family:Arial,sans-serif;color:#fff;">
        <div style="max-width:620px;margin:24px auto;background:#111;border:1px solid rgba(212,175,55,0.4);border-radius:14px;overflow:hidden;">
          <div style="padding:22px 24px;background:linear-gradient(135deg,#1a1a1a,#0a0a0a);border-bottom:1px solid rgba(212,175,55,0.25);text-align:center;">
            <img src="https://www.jukeboxloungenc.com/static/images/hero.jpg" alt="The Jukebox Lounge NC" style="max-width:100%;height:auto;border-radius:8px;" />
          </div>
          <div style="padding:26px 24px;">
            <h2 style="margin:0 0 10px;color:#D4AF37;letter-spacing:0.6px;">Welcome to the VIP Email List</h2>
            <p style="margin:0 0 12px;color:#f1f1f1;font-size:15px;">Hi {name or 'VIP Member'},</p>
            <p style="margin:0 0 12px;color:#d9d9d9;line-height:1.6;">
              You are officially in. As a VIP member, you’ll get first access to exclusive drops, event updates,
              and elevated experiences from The Jukebox Lounge NC.
            </p>
            <div style="margin:18px 0;padding:14px;border:1px solid rgba(212,175,55,0.3);border-radius:10px;background:#0c0c0c;">
              <p style="margin:0 0 8px;color:#D4AF37;font-weight:bold;">What to expect:</p>
              <p style="margin:0;color:#d9d9d9;line-height:1.6;">• Early access to tickets<br>• VIP-only announcements<br>• Curated nights and premium vibes</p>
            </div>
            <p style="margin:14px 0 0;color:#d9d9d9;line-height:1.6;">
              Stay ready — your next invite is coming soon.
            </p>
            <p style="margin:18px 0 0;color:#d9d9d9;line-height:1.6;">
              See you soon,<br>
              The Jukebox Lounge NC
            </p>
            <p style="margin:18px 0 0;color:#D4AF37;font-weight:bold;">The Jukebox Lounge NC</p>
          </div>
        </div>
      </body>
    </html>
    """

    send_ok = False
    recipient = (email or "").strip()
    if recipient:
        try:
            send_ok = send_html_email(
                "Welcome to The Jukebox Lounge VIP List",
                recipient,
                welcome_plain,
                welcome_html,
            )
        except Exception as exc:
            print("[vip-signup] welcome email failed:", exc)
            traceback.print_exc()
    print(f"[vip-welcome] recipient={recipient or '(missing)'} sent={send_ok}")

    return render_thank_you_safe(
        "WELCOME TO THE VIP EMAIL LIST",
        "You're officially on the VIP Email list. Get ready for exclusive drops, early access, and curated experiences.",
    )

@app.route("/event-interest", methods=["POST"])
def event_interest():
    try:
        raw_name = request.form.get("event_name")
        event_name = clean_event_name(raw_name)

        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("""
INSERT INTO event_requests (event_name, status)
VALUES (?, 'New')
""", (event_name,))
        conn.commit()
        conn.close()
    except Exception as exc:
        print("[event-interest] submit failed:", exc)
        traceback.print_exc()

    return render_thank_you_safe(
        "SUBMITTED",
        "Your input helps shape future Jukebox events.",
    )
@app.route("/vote-event", methods=["POST"])
def vote_event():
    event_name = request.form.get("event_name")

    # 🔥 normalize EVERYTHING
    event_name = event_name.strip().lower()

    conn = sqlite3.connect(DB_PATH)
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
    conn = sqlite3.connect(DB_PATH)
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

    return redirect("/events")


@app.route("/update-event-request/<int:id>", methods=["POST"])
@requires_auth
def update_event_request(id):
    new_status = (request.form.get("status") or "New").strip().title()
    if new_status not in ("New", "Completed"):
        new_status = "New"

    archived = 1 if new_status == "Completed" else 0
    archived_at_expr = "CURRENT_TIMESTAMP" if archived == 1 else "NULL"

    conn = sqlite3.connect(DB_PATH)
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
    return redirect("/events")

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
    if event_name == "grown & sexy" or event_name == "grown" or event_name.startswith("grown "):
        event_name = "juneteenth celebration"

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

    if event["name"] == "Juneteenth Celebration":
        return render_template("vip_early_access_event.html", event=event)

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    canonical = [
        ("Early Bird", "early"),
        ("General Admission", "ga"),
        ("VIP Section", "vip"),
        ("DJ VIP Section", "booth"),
    ]
    ticket_data = []
    for ticket_name, key in canonical:
        cfg = (event.get("tickets", {}) or {}).get(key, {}) or {}
        price = float(cfg.get("price", 0) or 0)
        quantity = int(cfg.get("size", 0) or 0)
        sold = 0

        # Prefer canonical inventory counters from ticket_types.
        cursor.execute(
            """
            SELECT COALESCE(sold, 0), COALESCE(max_quantity, 0)
            FROM ticket_types
            WHERE event_name = ? AND ticket_name = ?
            LIMIT 1
            """,
            (event["name"], ticket_name),
        )
        inventory_row = cursor.fetchone()
        if inventory_row:
            sold = int(inventory_row[0] or 0)
            quantity = int(inventory_row[1] or quantity or 0)
        else:
            # Fallback for legacy rows if inventory table is missing a match.
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
            sold = int(cursor.fetchone()[0] or 0)

        remaining = max(0, quantity - sold)
        if event["name"] == "Battle of the DJs" and ticket_name == "Early Bird":
            remaining = 0
        if event["name"] == "Battle of the DJs" and ticket_name == "VIP Section":
            remaining = 0
        ticket_data.append({
            "name": ticket_name,
            "price": round(price, 2),
            "sold": sold,
            "quantity": quantity,
            "remaining": remaining,
            "sold_out": remaining <= 0 and quantity > 0,
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


@app.route("/vip-early-access")
def vip_early_access_event():
    # Intentionally unlinked private page for VIP early-access sharing.
    event = {
        "name": "Grown & Sexy",
        "description": "The Jukebox Lounge NC presents Grown & Sexy: Melanin — the official finale of our 3-Part Grand Opening Series and Juneteenth Weekend Celebration. Join us June 20th at West End Social for a classy 30+ experience celebrating culture, confidence, music, and all shades of beautiful. Dressy casual to upscale attire encouraged. No athletic wear or ball caps.",
        "event_datetime": "June 20, 2026",
        "doors": "8:00 PM",
        "location": "West End Social",
        "ticket_label": "Juneteenth Weekend Celebration",
        "dress_code": "Dressy casual to upscale attire encouraged. No athletic wear. No ball caps.",
        "age_requirement": "30+ Event",
        "featured_drink": "Keeva's Juke Joint Old-Fashioned: old-fashioned muddled oranges, splash of ginger ale.",
        "map_link": "https://www.google.com/maps/search/?api=1&query=West+End+Social",
        "ticket_link": "https://square.link/u/51KF7WKE",
    }
    return render_template("vip_early_access_event.html", event=event)

@app.route("/check-tickets")
@requires_auth
def check_tickets():
    return "SOURCE OF TRUTH VIOLATION: USE event_tickets ONLY", 500

@app.route("/test-sell")
@requires_auth
def test_sell():
    return "SOURCE OF TRUTH VIOLATION: USE event_tickets ONLY", 500

@app.route("/check-leads")
@requires_auth
def check_leads():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM leads")
    data = cursor.fetchall()

    conn.close()

    return str(data)

@app.route("/test-lead")
@requires_auth
def test_lead():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO leads (type, name, email, details, status)
        VALUES (?, ?, ?, ?, ?)
    """, ("Membership Signup", "Test User", "test@email.com", "Wants to join", "new"))

    conn.commit()
    conn.close()

    return "Test lead added!"

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

    conn = sqlite3.connect(DB_PATH)
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
    new_status = (request.form.get("status") or "").strip()

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT type FROM leads WHERE id = ?", (id,))
    row = cursor.fetchone()
    lead_type = row[0] if row else "Contact Message"
    status_value = normalize_lead_status(lead_type, new_status)
    archived = 1 if lead_is_archived_status(status_value) else 0
    archived_at_expr = "CURRENT_TIMESTAMP" if archived == 1 else "NULL"

    cursor.execute(f"""
        UPDATE leads
        SET status = ?,
            archived = ?,
            archived_at = {archived_at_expr}
        WHERE id = ?
    """, (status_value, archived, id))

    conn.commit()
    conn.close()
    return redirect(request.referrer or "/admin/leads")


@app.route("/update-lead-note/<int:id>", methods=["POST"])
@requires_auth
def update_lead_note(id):
    note = (request.form.get("note") or "").strip()
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(
        """
        UPDATE leads
        SET notes = ?
        WHERE id = ?
        """,
        (note, id),
    )
    conn.commit()
    conn.close()
    return redirect(request.referrer or "/admin/leads")


@app.route("/delete-lead/<int:id>", methods=["POST"])
@requires_auth
def delete_lead(id):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM leads WHERE id = ?", (id,))
    conn.commit()
    conn.close()
    return redirect(request.referrer or "/admin/leads")


@app.route("/admin/leads/mass-email", methods=["GET", "POST"])
@requires_auth
def mass_email_leads():
    if request.method == "GET":
        return redirect("/admin/leads?msg=Use+Open+in+Gmail+inside+VIP+or+Membership+logs")
    # Disable backend send path for now to avoid production 500s; use Gmail compose workflow.
    return redirect("/admin/leads?msg=Mass+email+is+set+to+Open+in+Gmail+workflow")
    try:
        category = (request.form.get("category") or "").strip()
        subject = (request.form.get("subject") or "").strip()
        body = (request.form.get("body") or "").strip()
        cta_text = (request.form.get("cta_text") or "").strip()
        cta_url = (request.form.get("cta_url") or "").strip()

        if category not in ("VIP Signup", "Membership Signup"):
            return redirect("/admin/leads?msg=Invalid+email+category")
        if not subject or not body:
            t = urllib.parse.quote_plus(category.lower())
            return redirect(f"/admin/leads?type={t}&msg=Subject+and+message+are+required")

        attachments = []
        flyer_inline = None
        files = request.files.getlist("attachments")
        if not files and request.files.get("attachments"):
            files = [request.files.get("attachments")]
        for file in files:
            if not file or not file.filename:
                continue
            filename = os.path.basename(file.filename.strip())
            content = file.read()
            if not content:
                continue
            attachments.append({"filename": filename, "content": content})
        print(f"[mass-email] attachments uploaded={len(attachments)}")

        flyer_file = request.files.get("flyer_image")
        if flyer_file and flyer_file.filename:
            flyer_filename = os.path.basename(flyer_file.filename.strip())
            flyer_content = flyer_file.read()
            flyer_mimetype = (flyer_file.mimetype or "").strip().lower()
            if flyer_content:
                flyer_inline = {
                    "filename": flyer_filename,
                    "content": flyer_content,
                    "mimetype": flyer_mimetype or "image/jpeg",
                }
        print(f"[mass-email] flyer inline uploaded={bool(flyer_inline)}")

        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT DISTINCT LOWER(TRIM(email))
            FROM leads
            WHERE email IS NOT NULL
              AND TRIM(email) <> ''
              AND type = ?
              AND status = 'Active'
            """,
            (category,),
        )
        rows = cursor.fetchall()
        conn.close()

        recipients = [r[0] for r in rows if r and r[0]]
        if not recipients:
            t = urllib.parse.quote_plus(category.lower())
            return redirect(f"/admin/leads?type={t}&msg=No+active+recipients+found")

        sent = 0
        failed = 0
        for recipient in recipients:
            ok = send_email_with_attachments(
                subject,
                body,
                recipient,
                attachments=attachments,
                flyer_inline=flyer_inline,
                cta_text=cta_text,
                cta_url=cta_url,
            )
            if ok:
                sent += 1
            else:
                failed += 1

        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS mass_email_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                category TEXT,
                subject TEXT,
                recipients_count INTEGER DEFAULT 0,
                attachments_count INTEGER DEFAULT 0,
                sent_count INTEGER DEFAULT 0,
                failed_count INTEGER DEFAULT 0,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        cursor.execute(
            """
            INSERT INTO mass_email_log (category, subject, recipients_count, attachments_count, sent_count, failed_count)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (category, subject, len(recipients), len(attachments), sent, failed),
        )
        conn.commit()
        conn.close()

        t = urllib.parse.quote_plus(category.lower())
        msg = urllib.parse.quote_plus(f"Mass email complete: sent {sent}, failed {failed}.")
        return redirect(f"/admin/leads?type={t}&msg={msg}")
    except Exception as exc:
        print("[mass-email] route failed:", exc)
        traceback.print_exc()
        fallback_category = (request.form.get("category") or "VIP Signup").strip().lower()
        t = urllib.parse.quote_plus(fallback_category)
        err = urllib.parse.quote_plus(f"Mass email failed: {str(exc)[:120]}")
        return redirect(f"/admin/leads?type={t}&msg={err}")


@app.route("/admin/leads")
@requires_auth
def admin_leads():
    type_filter = (request.args.get("type") or "").strip().lower()
    status_filter = (request.args.get("status") or "").strip()
    q = (request.args.get("q") or "").strip().lower()
    show_archived = (request.args.get("show_archived") or "0").strip() in ("1", "true", "yes")
    msg = (request.args.get("msg") or "").strip()

    # Normalize common filter aliases so VIP/Membership logs always resolve.
    if "vip" in type_filter:
        type_filter = "vip signup"
    elif "membership" in type_filter:
        type_filter = "membership signup"
    elif "vendor" in type_filter:
        type_filter = "vendor application"
    elif "dj" in type_filter or "band" in type_filter:
        type_filter = "dj application"
    elif "inquir" in type_filter or "contact" in type_filter:
        type_filter = "contact message"

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    # Be resilient to older live schemas that may not yet have new columns.
    cursor.execute("PRAGMA table_info(leads)")
    lead_columns = {r[1] for r in cursor.fetchall()}
    has_archived = "archived" in lead_columns
    has_created_at = "created_at" in lead_columns
    has_notes = "notes" in lead_columns

    archived_expr = "COALESCE(archived, 0)" if has_archived else "0"
    created_expr = "COALESCE(created_at, '')" if has_created_at else "''"
    notes_expr = "COALESCE(notes, '')" if has_notes else "''"

    cursor.execute(
        f"""
        SELECT id, type, name, email, details, status, {archived_expr}, {created_expr}, {notes_expr}
        FROM leads
        ORDER BY id DESC
        """
    )
    rows = cursor.fetchall()
    mass_email_logs = []
    try:
        if type_filter in ("vip signup", "membership signup"):
            target_category = "VIP Signup" if type_filter == "vip signup" else "Membership Signup"
            cursor.execute(
                """
                SELECT category, subject, recipients_count, attachments_count, sent_count, failed_count, created_at
                FROM mass_email_log
                WHERE category = ?
                ORDER BY id DESC
                LIMIT 10
                """,
                (target_category,),
            )
        else:
            cursor.execute(
                """
                SELECT category, subject, recipients_count, attachments_count, sent_count, failed_count, created_at
                FROM mass_email_log
                ORDER BY id DESC
                LIMIT 10
                """
            )
        mass_email_logs = cursor.fetchall()
    except sqlite3.OperationalError as exc:
        print("[admin-leads] mass_email_log read skipped:", exc)
        mass_email_logs = []
    conn.close()

    filtered = []
    for r in rows:
        category = lead_category(r[1])
        archived = int(r[6] or 0)
        if not show_archived and archived == 1:
            continue
        if type_filter and type_filter != category.lower():
            continue
        if status_filter and r[5] != status_filter:
            continue
        search_blob = f"{r[2]} {r[3]} {r[4]}".lower()
        if q and q not in search_blob:
            continue
        filtered.append(
            {
                "id": r[0],
                "category": category,
                "type": r[1],
                "name": r[2],
                "email": r[3],
                "details": r[4],
                "status": r[5],
                "archived": archived,
                "created_at": r[7],
                "notes": r[8],
                "allowed_statuses": LEAD_STATUS_MAP.get(category, ("New",)),
            }
        )

    application_mode = type_filter in ("dj application", "vendor application")
    mass_email_mode = type_filter in ("vip signup", "membership signup")
    mass_email_recipients = []
    if mass_email_mode:
        target_category = "VIP Signup" if type_filter == "vip signup" else "Membership Signup"

        # Primary: active recipients only (case-insensitive)
        for lead in filtered:
            if lead["category"] != target_category:
                continue
            if (lead["status"] or "").strip().lower() != "active":
                continue
            email = (lead["email"] or "").strip().lower()
            if email and email not in mass_email_recipients:
                mass_email_recipients.append(email)

        # Fallback: if none marked Active, use non-archived rows for that same log.
        if not mass_email_recipients:
            for lead in filtered:
                if lead["category"] != target_category:
                    continue
                if int(lead.get("archived", 0)) == 1:
                    continue
                email = (lead["email"] or "").strip().lower()
                if email and email not in mass_email_recipients:
                    mass_email_recipients.append(email)
    vip_active_count = 0
    membership_active_count = 0
    for lead in filtered:
        if lead["category"] == "VIP Signup" and lead["status"] == "Active":
            vip_active_count += 1
        if lead["category"] == "Membership Signup" and lead["status"] == "Active":
            membership_active_count += 1

    return render_template(
        "admin_leads.html",
        leads=filtered,
        type_filter=type_filter,
        status_filter=status_filter,
        q=q,
        show_archived=show_archived,
        application_mode=application_mode,
        mass_email_mode=mass_email_mode,
        msg=msg,
        mass_email_logs=mass_email_logs,
        vip_active_count=vip_active_count,
        membership_active_count=membership_active_count,
        mass_email_recipients=mass_email_recipients,
    )


@app.route("/admin/event/<path:event_name>/customers")
@requires_auth
def admin_event_customers(event_name):
    decoded = urllib.parse.unquote(event_name).strip()
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT id, COALESCE(name, ''), COALESCE(email, ''), COALESCE(ticket_id, ''),
               COALESCE(ticket_type, ''), COALESCE(payment_id, ''), COALESCE(status, 'not_checked_in'),
               COALESCE(checked_in, 0)
        FROM event_tickets
        WHERE COALESCE(event_name, 'Battle of the DJs') = ?
        ORDER BY id DESC
        """,
        (decoded,),
    )
    rows = cursor.fetchall()
    conn.close()
    customers = [
        {
            "id": r[0],
            "customer": r[1] or r[2],
            "email": r[2],
            "ticket_id": r[3],
            "ticket_type": r[4],
            "payment_id": r[5],
            "checked_in": int(r[7] or 0) == 1 or (r[6] == "checked_in"),
        }
        for r in rows
    ]
    return render_template("admin_event_customers.html", event_name=decoded, customers=customers)


@app.route("/admin/ticket/<int:ticket_row_id>/checkin", methods=["POST"])
@requires_auth
def admin_toggle_checkin(ticket_row_id):
    checked = (request.form.get("checked") or "0").strip() in ("1", "true", "yes")
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    if checked:
        cursor.execute(
            """
            UPDATE event_tickets
            SET checked_in = 1, status = 'checked_in', checked_in_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (ticket_row_id,),
        )
    else:
        cursor.execute(
            """
            UPDATE event_tickets
            SET checked_in = 0, status = 'not_checked_in', checked_in_at = NULL
            WHERE id = ?
            """,
            (ticket_row_id,),
        )
    conn.commit()
    conn.close()
    return redirect(request.referrer or "/events")


@app.route("/admin/system-health")
@requires_auth
def admin_system_health():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Discover potential legacy ticket tables/sources
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    all_tables = [r[0] for r in cursor.fetchall()]
    ticket_like_tables = [t for t in all_tables if "ticket" in t.lower()]
    legacy_ticket_tables = [t for t in ticket_like_tables if t != "event_tickets"]

    # Unified source metrics (event_tickets only)
    cursor.execute("SELECT COUNT(*) FROM event_tickets")
    tickets_total = int(cursor.fetchone()[0] or 0)

    cursor.execute(
        """
        SELECT COALESCE(event_name, ''), COUNT(*)
        FROM event_tickets
        GROUP BY COALESCE(event_name, '')
        ORDER BY COALESCE(event_name, '')
        """
    )
    tickets_by_event = [
        {"event_name": (r[0] or ""), "count": int(r[1] or 0)}
        for r in cursor.fetchall()
    ]

    cursor.execute("SELECT COUNT(*) FROM event_tickets WHERE COALESCE(TRIM(email), '') = ''")
    tickets_missing_email = int(cursor.fetchone()[0] or 0)

    cursor.execute("SELECT COUNT(*) FROM event_tickets WHERE COALESCE(TRIM(event_name), '') = ''")
    tickets_missing_event = int(cursor.fetchone()[0] or 0)

    cursor.execute("SELECT COUNT(*) FROM event_tickets WHERE COALESCE(checked_in, 0) = 1 OR status = 'checked_in'")
    checkin_true_count = int(cursor.fetchone()[0] or 0)

    cursor.execute(
        """
        SELECT COUNT(*)
        FROM event_tickets
        WHERE COALESCE(checked_in, 0) = 0 AND COALESCE(status, 'not_checked_in') != 'checked_in'
        """
    )
    checkin_false_count = int(cursor.fetchone()[0] or 0)

    cursor.execute(
        """
        SELECT COUNT(*)
        FROM event_tickets
        WHERE COALESCE(TRIM(ticket_id), '') = ''
           OR COALESCE(TRIM(payment_id), '') = ''
           OR COALESCE(TRIM(ticket_type), '') = ''
        """
    )
    orphan_records_count = int(cursor.fetchone()[0] or 0)

    conn.close()

    # Lock is about live query behavior, not table existence.
    legacy_live_query_routes = []  # keep empty once all live paths are event_tickets-only
    source_of_truth_locked = len(legacy_live_query_routes) == 0
    inconsistency_detected = (
        (not source_of_truth_locked)
        or tickets_missing_email > 0
        or tickets_missing_event > 0
        or orphan_records_count > 0
    )
    if inconsistency_detected:
        print("SYSTEM DATA INCONSISTENCY DETECTED")

    return {
        "tickets_total": tickets_total,
        "tickets_by_event": tickets_by_event,
        "tickets_missing_email": tickets_missing_email,
        "tickets_missing_event": tickets_missing_event,
        "checkin_true_count": checkin_true_count,
        "checkin_false_count": checkin_false_count,
        "orphan_records_count": orphan_records_count,
        "source_of_truth_table": "event_tickets",
        "source_of_truth_locked": source_of_truth_locked,
        "ticket_like_tables_found": ticket_like_tables,
        "legacy_ticket_tables_found": legacy_ticket_tables,
        "legacy_live_query_routes": legacy_live_query_routes,
        "warning": "SYSTEM DATA INCONSISTENCY DETECTED" if inconsistency_detected else "",
    }


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
    from urllib.parse import unquote
    normalized_ticket_id = (ticket_id or "").strip()
    normalized_ticket_id = unquote(normalized_ticket_id)
    if "/checkin/" in normalized_ticket_id:
        normalized_ticket_id = normalized_ticket_id.split("/checkin/")[-1].strip()
    normalized_ticket_id = normalized_ticket_id.split("?")[0].strip().strip("/")
    print("SCANNING:", normalized_ticket_id)

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute(
        "SELECT id, ticket_id, status, checked_in, payment_id, created_at FROM event_tickets WHERE ticket_id = ?",
        (normalized_ticket_id,),
    )
    row = cursor.fetchone()
    print("FOUND IN DB:", row)

    if not row:
        conn.close()
        return render_template("checkin_result.html", status="invalid")

    current_status = (row[2] or "").lower()
    checked_in_flag = int(row[3] or 0)

    if current_status == "checked_in" or checked_in_flag == 1:
        conn.close()
        return render_template("checkin_result.html", status="already_checked_in")

    cursor.execute(
        """
        UPDATE event_tickets
        SET status = 'checked_in',
            checked_in = 1,
            checked_in_at = CURRENT_TIMESTAMP
        WHERE ticket_id = ?
        """,
        (normalized_ticket_id,),
    )
    conn.commit()
    conn.close()

    return render_template("checkin_result.html", status="success")


@app.route("/tickets/admin")
@requires_auth
def tickets_admin():
    print(f"[tickets_admin] using database: {DB_PATH}")
    conn = sqlite3.connect(DB_PATH)
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

    conn = sqlite3.connect(DB_PATH)
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

        ticket_ids = create_ticket_from_square_payment(cursor, payment, amount_cents, email) or []
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

        if ticket_ids:
            send_tickets_email_bundle(cursor, payment_id, email, event_name_from_payment(payment))

        if ticket_hit or membership_hit or ticket_ids:
            log_square_payment(
                cursor,
                payment_id,
                "ticket" if (ticket_hit or ticket_ids) else "membership",
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

            backfill_conn = sqlite3.connect(DB_PATH)
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

    conn = sqlite3.connect(DB_PATH)
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
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("SELECT ticket_id FROM event_tickets LIMIT 10")
    rows = cursor.fetchall()

    conn.close()
    return {"tickets": rows}


@app.route("/debug/square")
def debug_square():
    import sqlite3
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM square_payment_log LIMIT 10")
    rows = cursor.fetchall()

    conn.close()

    return {"square_payments": rows}


@app.route("/debug-ticket/<ticket_id>")
def debug_ticket(ticket_id):
    import sqlite3
    from urllib.parse import unquote
    raw_ticket_id = ticket_id
    normalized_ticket_id = unquote((ticket_id or "").strip())
    if "/checkin/" in normalized_ticket_id:
        normalized_ticket_id = normalized_ticket_id.split("/checkin/")[-1].strip()
    normalized_ticket_id = normalized_ticket_id.split("?")[0].strip().strip("/")

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT id, name, email, ticket_type, ticket_id, status, payment_id, checkin_url, qr_url, created_at
        FROM event_tickets
        WHERE ticket_id = ?
        LIMIT 1
        """,
        (normalized_ticket_id,),
    )
    row = cursor.fetchone()
    conn.close()

    return {
        "ticket_id": raw_ticket_id,
        "normalized_ticket_id": normalized_ticket_id,
        "exists": bool(row),
        "raw_db_record": row,
    }


@app.route("/tickets/<email>")
def tickets_by_email(email):
    normalized_email = (email or "").strip().lower()
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT ticket_id, ticket_type, qr_url
        FROM event_tickets
        WHERE LOWER(email) = ?
        ORDER BY id DESC
        """,
        (normalized_email,),
    )
    rows = cursor.fetchall()
    conn.close()
    return {
        "email": normalized_email,
        "tickets": [
            {
                "ticket_id": row[0],
                "ticket_type": row[1],
                "qr_url": row[2],
            }
            for row in rows
        ],
    }


@app.route("/admin/resend-all-tickets")
@requires_auth
def resend_all_tickets():
    force = (request.args.get("force") or "0").strip() in ("1", "true", "yes")
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    recovered = recover_missing_tickets(cursor)
    conn.commit()

    grouped = load_customer_tickets(cursor, include_checked_in=force)
    emails_sent = 0
    tickets_processed = 0
    failures = []

    for email, tickets in grouped.items():
        ok = send_tickets_email_for_customer(email, tickets, "Your Jukebox Lounge QR Tickets")
        if ok:
            emails_sent += 1
            tickets_processed += len(tickets)
        else:
            failures.append(email)

    conn.close()
    return {
        "emails_sent": emails_sent,
        "tickets_processed": tickets_processed,
        "failures": failures,
        "recovered_tickets": recovered,
        "forced": force,
    }


@app.route("/admin/resend/<path:email>")
@requires_auth
def resend_customer_tickets(email):
    force = (request.args.get("force") or "0").strip() in ("1", "true", "yes")
    target_email = (email or "").strip().lower()
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    grouped = load_customer_tickets(cursor, include_checked_in=force, target_email=target_email)
    tickets = grouped.get(target_email, [])
    if not tickets:
        conn.close()
        return {
            "email": target_email,
            "emails_sent": 0,
            "tickets_processed": 0,
            "failures": [],
            "message": "No tickets found for customer",
            "forced": force,
        }

    ok = send_tickets_email_for_customer(target_email, tickets, "Your Jukebox Lounge QR Tickets")
    conn.close()
    return {
        "email": target_email,
        "emails_sent": 1 if ok else 0,
        "tickets_processed": len(tickets) if ok else 0,
        "failures": [] if ok else [target_email],
        "forced": force,
    }


@app.route('/scan')
def scan():
    return render_template('scan.html')


@app.route('/admin/generate-qr/<ticket_id>')
def generate_qr(ticket_id):
    import qrcode
    from io import BytesIO
    from flask import send_file
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(
        "SELECT ticket_id FROM event_tickets WHERE ticket_id = ?",
        ((ticket_id or "").strip(),),
    )
    row = cursor.fetchone()
    conn.close()
    if not row:
        return "Ticket not found", 404

    stored_ticket_id = row[0]
    url = f"https://www.jukeboxloungenc.com/checkin/{stored_ticket_id}"

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
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(
        "SELECT ticket_id FROM event_tickets WHERE ticket_id = ?",
        ((ticket_id or "").strip(),),
    )
    row = cursor.fetchone()
    conn.close()
    if not row:
        return "Ticket not found", 404

    stored_ticket_id = row[0]
    url = f"https://www.jukeboxloungenc.com/checkin/{stored_ticket_id}"
    img = qrcode.make(url)

    buf = BytesIO()
    img.save(buf, format="PNG")
    buf.seek(0)

    return send_file(buf, mimetype='image/png')


@app.route("/admin/dashboard-redesign")
def admin_dashboard_redesign():
    return render_template("dashboard_redesign.html")

# -------------------------
# RUN
# -------------------------
if __name__ == "__main__":
    app.run(debug=True, port=5050, use_reloader=False)


