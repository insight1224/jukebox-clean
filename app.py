from flask import Flask, render_template, request, redirect
import sqlite3
import smtplib
from email.mime.text import MIMEText

from functools import wraps
from flask import request, Response

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
# ✅ DATABASE SETUP (RUN ON APP START)

def init_db():
    conn = sqlite3.connect("database.db")
    cursor = conn.cursor()

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

    # EVENT REQUESTS
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS event_requests (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        event_name TEXT,
        status TEXT DEFAULT 'New'
    )
    """)

    # EVENT VOTES
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS event_votes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        event_name TEXT UNIQUE,
        votes INTEGER DEFAULT 0
    )
    """)

    # ✅ TICKET TABLE (FIXED COLUMN NAME)
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

    # ✅ SEED DATA
    tickets = [
        ("Battle of the DJs", "Early Bird", 13, 30),
        ("Battle of the DJs", "General Admission", 18, 366),
        ("Battle of the DJs", "VIP Section", 175, 6),
        ("Battle of the DJs", "DJ VIP Section", 200, 3),
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
    """, ("Membership", name, email, "Waiting for payment", "Pending"))

    conn.commit()
    conn.close()

    return redirect("https://square.link/u/fgiSNspy")

# -------------------------
# WEBHOOK
# -------------------------
@app.route("/square-webhook", methods=["POST"])
def square_webhook():
    print("🔥 WEBHOOK HIT")

    data = request.json
    print(data)

    conn = sqlite3.connect("database.db")
    cursor = conn.cursor()

    payment = data.get("data", {}).get("object", {}).get("payment", {})
    amount = payment.get("amount_money", {}).get("amount")

    print("AMOUNT:", amount)

    if amount == 13:
        ticket_name = "Early Bird"
    elif amount == 18:
        ticket_name = "General Admission"
    elif amount == 175:
        ticket_name = "VIP Section"
    elif amount == 200:
        ticket_name = "DJ VIP Section"
    else:
        conn.close()
        return "ignored", 200

    cursor.execute("""
        UPDATE ticket_types
        SET sold = sold + 1
        WHERE event_name = ? AND ticket_name = ?
    """, ("Battle of the DJs", ticket_name))

    conn.commit()
    conn.close()

    return "ok", 200
# -------------------------
# DASHBOARD
# -------------------------
@app.route("/dashboard")
@requires_auth
def dashboard():
    conn = sqlite3.connect("database.db")
    cursor = conn.cursor()

    # ✅ GET VOTES (VINYL PERFORMANCE)
    cursor.execute("""
        SELECT event_name, votes
        FROM event_votes
        ORDER BY votes DESC
    """)
    event_votes = cursor.fetchall()

    # ✅ GET REQUESTS (IDEAS PIPELINE)
    cursor.execute("""
        SELECT id, event_name, status
        FROM event_requests
        ORDER BY id DESC
    """)
    event_requests = cursor.fetchall()

    conn.close()

    return render_template(
        "dashboard.html",
        event_votes=event_votes,
        event_requests=event_requests
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
def complete_request(id):
    conn = sqlite3.connect("database.db")
    cursor = conn.cursor()

    # 🔥 DELETE the request instead of updating it
    cursor.execute("DELETE FROM event_requests WHERE id = ?", (id,))

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

    except Exception as e:
        conn.close()
        return f"TICKET DB ERROR: {e}", 500

    ticket_data = []

    for name, price, quantity, sold in tickets:
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

    return render_template(
        "event_detail.html",
        event=event,
        ticket_data=ticket_data
    )
@app.route("/rebuild-db")
def rebuild_db():
    conn = sqlite3.connect("database.db")
    cursor = conn.cursor()

    # EVENTS
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT
    )
    """)

    # TICKETS
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

    # VOTES
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS event_votes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        event_name TEXT UNIQUE,
        votes INTEGER DEFAULT 0
    )
    """)

    # LEADS
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

    conn.commit()
    conn.close()

    return "DB rebuilt successfully"
# -------------------------
# RUN
# -------------------------
if __name__ == "__main__":
    init_db()   # ✅ ADD THIS
    app.run(debug=True, use_reloader=False)