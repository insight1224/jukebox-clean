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

cursor.execute("""
CREATE TABLE IF NOT EXISTS event_requests (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_name TEXT
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS event_votes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_name TEXT,
    votes INTEGER DEFAULT 1
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
    conn = sqlite3.connect("database.db")
    cursor = conn.cursor()

    global events_data

    # ✅ CURATED VINYL OPTIONS
    vinyl_options = [
        "Grown and Sexy Ball",
        "Line Dancing",
        "Afrobeats",
        "Live Bands",
        "Open Mic"
    ]

    # ✅ ENSURE THEY EXIST IN DB
    for event in vinyl_options:
        cursor.execute("SELECT * FROM event_votes WHERE event_name = ?", (event,))
        exists = cursor.fetchone()

        if not exists:
            cursor.execute(
                "INSERT INTO event_votes (event_name, votes) VALUES (?, 0)",
                (event,)
            )

    # ✅ GET VOTES
    cursor.execute("SELECT event_name, votes FROM event_votes ORDER BY votes DESC")
    event_votes = cursor.fetchall()

    conn.commit()
    conn.close()

    return render_template(
        "events.html",
        events=events_data,
        event_votes=event_votes,
        vinyl_options=vinyl_options
    )
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
# -------------------------
# RUN
# -------------------------
if __name__ == "__main__":
    print("🚀 STARTING APP...")
    app.run(debug=True, port=5050)