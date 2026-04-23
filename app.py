from flask import Flask, render_template, request, redirect
import sqlite3
import smtplib
from email.mime.text import MIMEText

from functools import wraps
from flask import request, Response

def check_auth(username, password):
    return username == "admin" and password == "jukebox123"

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

app = Flask(__name__)

# -------------------------
# DATABASE SETUP
# -------------------------
conn = sqlite3.connect("database.db")
cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS leads (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    type TEXT,
    name TEXT,
    email TEXT,
    details TEXT,
    status TEXT
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS memberships (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    email TEXT,
    amount REAL,
    status TEXT
)
""")

conn.commit()
conn.close()

# -------------------------
# EMAIL CONFIG
# -------------------------
EMAIL_ADDRESS = "thejukeboxloungenc@gmail.com"
EMAIL_PASSWORD = "ilpnirohqeekiblb"

def send_email(subject, body, to_email):
    try:
        msg = MIMEText(body)
        msg["Subject"] = subject
        msg["From"] = EMAIL_ADDRESS
        msg["To"] = to_email

        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
            smtp.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
            smtp.send_message(msg)
    except Exception as e:
        print("Email failed:", e)

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
    }
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
    return render_template("events.html", events=events_data)

@app.route("/events/<int:event_id>")
def event_details(event_id):
    event = next((e for e in events_data if e["id"] == event_id), None)

    if not event:
        return "Event not found"

    return render_template("event_details.html", event=event)
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

        return render_template("thank_you.html")

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
    """, ("Membership", name, email, "Waiting for payment", "Pending"))

    conn.commit()
    conn.close()

    return redirect("https://square.link/u/fgiSNspy")

# -------------------------
# WEBHOOK
# -------------------------
@app.route("/square-webhook", methods=["POST"])
def square_webhook():
    data = request.json
    print("WEBHOOK HIT:", data)

    try:
        if data.get("type") == "payment.updated":
            payment = data["data"]["object"]["payment"]

            amount = payment["amount_money"]["amount"] / 100
            email = payment.get("buyer_email_address", "unknown")

            # 🎟 MAP AMOUNT → EVENT + TICKET TYPE
            if amount == 13:
                ticket_type = "Early Bird"
                event_name = "Battle of the DJs"

            elif amount == 18:
                ticket_type = "General Admission"
                event_name = "Battle of the DJs"

            elif amount == 175:
                ticket_type = "VIP"
                event_name = "Battle of the DJs"

            elif amount == 200:
                ticket_type = "Booth"
                event_name = "Battle of the DJs"

            else:
                ticket_type = "Other"
                event_name = "Unknown Event"

            conn = sqlite3.connect("database.db")
            cursor = conn.cursor()

            cursor.execute("""
                INSERT INTO tickets (email, amount, type, event_name, status)
                VALUES (?, ?, ?, ?, ?)
            """, (email, amount, ticket_type, event_name, "paid"))

            conn.commit()
            conn.close()

            print("✅ Ticket saved")

    except Exception as e:
        print("Webhook error:", e)

    return "ok", 200
# -------------------------
# DASHBOARD
# -------------------------
@app.route("/dashboard")
@requires_auth
def dashboard():
    conn = sqlite3.connect("database.db")
    cursor = conn.cursor()

    # LEADS
    cursor.execute("SELECT * FROM leads ORDER BY id DESC")
    leads = cursor.fetchall()

    # METRICS
    cursor.execute("SELECT COUNT(*) FROM leads")
    total_leads = cursor.fetchone()[0]

    # MEMBERSHIPS
    cursor.execute("SELECT COUNT(*) FROM memberships")
    membership_count = cursor.fetchone()[0] or 0
    membership_revenue = membership_count * 10

    # TICKETS
    cursor.execute("SELECT COUNT(*) FROM tickets")
    ticket_count = cursor.fetchone()[0] or 0

    cursor.execute("SELECT SUM(amount) FROM tickets")
    ticket_revenue = cursor.fetchone()[0] or 0

    conn.close()

    total_revenue = ticket_revenue + membership_revenue

    return render_template(
        "dashboard.html",
        leads=leads,
        total_leads=total_leads,
        membership_count=membership_count,
        membership_revenue=membership_revenue,
        ticket_count=ticket_count,
        ticket_revenue=ticket_revenue,
        total_revenue=total_revenue
    )

conn = sqlite3.connect("database.db")
cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS tickets (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    email TEXT,
    amount REAL,
    type TEXT,
    status TEXT
)
""")

conn.commit()
conn.close()

def requires_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        auth = request.authorization
        if not auth or not check_auth(auth.username, auth.password):
            return authenticate()
        return f(*args, **kwargs)
    return decorated
# -------------------------
# RUN
# -------------------------
if __name__ == "__main__":
    print("🚀 STARTING APP...")
    app.run(debug=True, port=5050)