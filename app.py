from flask import Flask, render_template, request, redirect
import smtplib
import sqlite3
from email.mime.text import MIMEText

# -------------------------
# APP INIT (MUST BE FIRST)
# -------------------------
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

conn.commit()
conn.close()

# -------------------------
# EMAIL CONFIG
# -------------------------
EMAIL_ADDRESS = "thejukeboxloungenc@gmail.com"
EMAIL_PASSWORD = "ilpnirohqeekiblb"

# -------------------------
# EMAIL FUNCTION
# -------------------------
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
        "description": """Showcase your sound and compete for the crown. 
This is an indoor and outdoor event; in the event of inclement weather, tickets will be transferable to a future date or eligible for a refund. 
This is a 30+ event — valid IDs will be checked at the door. Guests who do not meet the age requirement will be denied entry with no refund.""",
        "flyer": "/static/images/flyer-part1.jpg",

        "tickets": {
            "early": {"price": 13, "sold": 30, "size": 1},
            "ga": {"price": 18, "sold": 90, "size": 1},
            "vip": {"price": 175, "sold": 6, "size": 1},
            "booth": {"price": 200, "sold": 2, "size": 6}
        },

        "early_link": "https://square.link/u/EyY0RvTh?src=sheet",
        "ga_link": "https://square.link/u/Y9p9XqJo?src=sheet",
        "vip_link": "https://square.link/u/ikIAImYb?src=sheet",
        "booth_link": "https://square.link/u/QfLXGM6i?src=sheet",
    }
]

# -------------------------
# HOME
# -------------------------
@app.route("/")
def home():
    return render_template("index.html")

# -------------------------
# EVENTS PAGE
# -------------------------
@app.route("/events")
def events():
    return render_template("events.html", events=events_data)

# -------------------------
# EVENT DETAIL
# -------------------------
@app.route("/event/<int:event_id>")
def event_detail(event_id):
    event = next((e for e in events_data if e["id"] == event_id), None)

    if not event:
        return "Event not found", 404

    return render_template("event_detail.html", event=event)

# -------------------------
# VIP FORM
# -------------------------
@app.route("/vip", methods=["POST"])
def vip():
    name = request.form.get("name")
    email = request.form.get("email")
    phone = request.form.get("phone")

    conn = sqlite3.connect("database.db")
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO leads (type, name, email, details, status)
        VALUES (?, ?, ?, ?, ?)
    """, ("VIP Signup", name, email, phone, "Active"))

    conn.commit()
    conn.close()

    send_email(
        "Welcome to VIP",
        f"Hi {name},\n\nWelcome to The Jukebox Lounge VIP List.",
        email
    )

    send_email(
        "New VIP Signup",
        f"Name: {name}\nEmail: {email}\nPhone: {phone}",
        EMAIL_ADDRESS
    )

    return render_template(
        "thank_you.html",
        title="You're On The List",
        message="Check your email for confirmation."
    )
# -------------------------
# CONTACT FORM
# -------------------------
# -------------------------
# CONTACT FORM
# -------------------------
@app.route("/contact", methods=["GET", "POST"])
def contact():
    if request.method == "POST":
        name = request.form.get("name")
        email = request.form.get("email")
        message = request.form.get("message")

        # SAVE TO DATABASE
        conn = sqlite3.connect("database.db")
        cursor = conn.cursor()

        cursor.execute("""
            INSERT INTO leads (type, name, email, details, status)
            VALUES (?, ?, ?, ?, ?)
        """, ("Contact Message", name, email, message, "New"))

        conn.commit()
        conn.close()

        # EMAILS
        send_email(
            "New Inquiry",
            f"Name: {name}\nEmail: {email}\nMessage:\n{message}",
            EMAIL_ADDRESS
        )

        send_email(
            "We Received Your Inquiry",
            f"Hi {name},\n\nWe will contact you within 24 hours.",
            email
        )

        return render_template(
            "thank_you.html",
            title="Message Sent",
            message="We’ll be in touch within 24 hours."
        )

    return render_template("contact.html")
# -------------------------
# DJ SIGNUP
# -------------------------
@app.route("/dj-signup", methods=["GET", "POST"])
def dj_signup():
    if request.method == "POST":
        data = request.form

        name = data.get("name")
        email = data.get("email")
        genre = data.get("genre")
        links = data.get("links")

        # SAVE TO DATABASE
        conn = sqlite3.connect("database.db")
        cursor = conn.cursor()

        details = f"Genre: {genre} | Links: {links}"

        cursor.execute("""
            INSERT INTO leads (type, name, email, details, status)
            VALUES (?, ?, ?, ?, ?)
        """, ("DJ Application", name, email, details, "New"))

        conn.commit()
        conn.close()

        # EMAILS
        send_email(
            "New DJ Application",
            f"Name: {name}\nEmail: {email}\nGenre: {genre}\nLinks: {links}",
            EMAIL_ADDRESS
        )

        send_email(
            "Application Received",
            f"Hi {name},\n\nWe’ve received your application and will review it shortly.",
            email
        )

        return render_template(
            "thank_you.html",
            title="Application Received",
            message="We will be in touch soon."
        )

    return render_template("dj_signup.html")
# -------------------------
# DASHBOARD
# -------------------------
@app.route("/dashboard")
def dashboard():
    search = request.args.get("search", "")
    filter_type = request.args.get("type", "")

    conn = sqlite3.connect("database.db")
    cursor = conn.cursor()

    query = "SELECT * FROM leads WHERE 1=1"
    params = []

    if filter_type:
        query += " AND type = ?"
        params.append(filter_type)

    if search:
        query += " AND (name LIKE ? OR email LIKE ? OR details LIKE ?)"
        params.extend([f"%{search}%", f"%{search}%", f"%{search}%"])

    query += " ORDER BY id DESC"

    cursor.execute(query, params)
    leads = cursor.fetchall()

    # METRICS
    cursor.execute("SELECT COUNT(*) FROM leads")
    total_leads = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM leads WHERE type='VIP Signup'")
    vip_count = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM leads WHERE type='Contact Message'")
    requests_count = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM leads WHERE type='DJ Application'")
    dj_count = cursor.fetchone()[0]

    conn.close()

    # REVENUE + ATTENDANCE
    total_revenue = 0
    total_attendance = 0

    for event in events_data:
        for ticket in event["tickets"].values():
            total_revenue += ticket["price"] * ticket["sold"]
            total_attendance += ticket["sold"] * ticket["size"]

    max_capacity = 450
    remaining = max_capacity - total_attendance

event_stats = []

for event in events_data:
    event_revenue = 0
    event_attendance = 0

    for ticket in event["tickets"].values():
        event_revenue += ticket["price"] * ticket["sold"]
        event_attendance += ticket["sold"] * ticket["size"]

    event_stats.append({
        "name": event["name"],
        "revenue": event_revenue,
        "attendance": event_attendance,
        "tickets_sold": sum(t["sold"] for t in event["tickets"].values())
    })

max_capacity = event.get("capacity", 150)  # fallback if not set
remaining = max_capacity - event_attendance

"remaining": remaining

    return render_template(
    "dashboard.html",
    leads=leads,
    total_leads=total_leads,
    vip_count=vip_count,
    requests_count=requests_count,
    dj_count=dj_count,
    total_revenue=total_revenue,
    total_attendance=total_attendance,
    remaining=remaining,
    event_stats=event_stats   # 👈 ADD THIS
)

# -------------------------
# UPDATE STATUS
# -------------------------
@app.route("/update-status/<int:lead_id>/<status>")
def update_status(lead_id, status):
    conn = sqlite3.connect("database.db")
    cursor = conn.cursor()

    cursor.execute(
        "UPDATE leads SET status=? WHERE id=?",
        (status, lead_id)
    )

    conn.commit()
    conn.close()

    return redirect("/dashboard")

@app.route("/membership")
def membership():
    return render_template("membership.html")

# -------------------------
# RUN APP
# -------------------------
if __name__ == "__main__":
    app.run()