const crypto = require("crypto");
const express = require("express");
const QRCode = require("qrcode");
const nodemailer = require("nodemailer");
const db = require("../db");

const router = express.Router();

const TICKET_TYPES = {
  early_bird: { label: "Early Bird", amountCents: 1300 },
  general_admission: { label: "General Admission", amountCents: 1800 },
  vip_section: { label: "VIP Section", amountCents: 17500 },
  dj_vip_section: { label: "DJ VIP Section", amountCents: 20000 }
};

function squareBaseUrl() {
  return process.env.SQUARE_ENV === "production"
    ? "https://connect.squareup.com"
    : "https://connect.squareupsandbox.com";
}

function createTicketId() {
  return `TICKET_${crypto.randomBytes(8).toString("hex").toUpperCase()}`;
}

function buildMailer() {
  const { SMTP_HOST, SMTP_PORT, SMTP_SECURE, SMTP_USER, SMTP_PASS } = process.env;
  if (!SMTP_HOST || !SMTP_USER || !SMTP_PASS) return null;

  return nodemailer.createTransport({
    host: SMTP_HOST,
    port: Number(SMTP_PORT || 465),
    secure: String(SMTP_SECURE || "true").toLowerCase() === "true",
    auth: { user: SMTP_USER, pass: SMTP_PASS }
  });
}

async function sendTicketEmail(ticket) {
  const transporter = buildMailer();
  if (!transporter) return;

  const from = process.env.EMAIL_FROM || process.env.SMTP_USER;
  const subject = "Your Jukebox Event Ticket";
  const html = `
    <h2>Ticket Confirmed</h2>
    <p><strong>Name:</strong> ${ticket.name}</p>
    <p><strong>Ticket ID:</strong> ${ticket.ticket_id}</p>
    <p><strong>Type:</strong> ${ticket.ticket_type}</p>
    <p><strong>Check-in URL:</strong> <a href="${ticket.checkin_url}">${ticket.checkin_url}</a></p>
    <p>Present this QR code at check-in:</p>
    <img src="${ticket.qr_code_data_url}" alt="Ticket QR Code" />
  `;

  await transporter.sendMail({
    from,
    to: ticket.email,
    subject,
    html
  });
}

async function processSquarePayment({ sourceId, amountCents, idempotencyKey, buyerEmailAddress }) {
  const token = process.env.SQUARE_ACCESS_TOKEN;
  if (!token) {
    throw new Error("SQUARE_ACCESS_TOKEN is not configured.");
  }

  const payload = {
    source_id: sourceId,
    idempotency_key: idempotencyKey,
    amount_money: {
      amount: amountCents,
      currency: "USD"
    },
    autocomplete: true,
    buyer_email_address: buyerEmailAddress
  };

  const response = await fetch(`${squareBaseUrl()}/v2/payments`, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
      "Square-Version": "2024-11-20"
    },
    body: JSON.stringify(payload)
  });

  const data = await response.json();
  if (!response.ok || !data.payment) {
    const reason = data?.errors?.map((err) => err.detail).join("; ") || "Square payment failed";
    throw new Error(reason);
  }

  return data.payment;
}

router.get("/public-config", (req, res) => {
  res.json({
    applicationId: process.env.SQUARE_APPLICATION_ID || "",
    locationId: process.env.SQUARE_LOCATION_ID || "",
    ticketTypes: TICKET_TYPES
  });
});

router.post("/purchase", async (req, res) => {
  try {
    const { sourceId, name, email, ticketType } = req.body;
    const selected = TICKET_TYPES[ticketType];
    if (!sourceId || !name || !email || !selected) {
      return res.status(400).json({ error: "Missing or invalid purchase fields." });
    }

    const idempotencyKey = crypto.randomUUID();
    const payment = await processSquarePayment({
      sourceId,
      amountCents: selected.amountCents,
      idempotencyKey,
      buyerEmailAddress: email
    });

    const ticketId = createTicketId();
    const baseUrl = process.env.BASE_URL || `${req.protocol}://${req.get("host")}`;
    const checkinUrl = `${baseUrl}/checkin/${ticketId}`;
    const qrCodeDataUrl = await QRCode.toDataURL(checkinUrl);

    const insert = db.prepare(`
      INSERT INTO tickets (
        name, email, ticket_type, amount_cents, ticket_id,
        status, payment_id, checkin_url, qr_code_data_url
      ) VALUES (?, ?, ?, ?, ?, 'not_checked_in', ?, ?, ?)
    `);

    insert.run(
      name.trim(),
      email.trim().toLowerCase(),
      selected.label,
      selected.amountCents,
      ticketId,
      payment.id,
      checkinUrl,
      qrCodeDataUrl
    );

    const ticket = db
      .prepare("SELECT * FROM tickets WHERE ticket_id = ?")
      .get(ticketId);

    sendTicketEmail(ticket).catch((err) => {
      console.error("Email send failed:", err.message);
    });

    return res.json({
      success: true,
      ticket: {
        name: ticket.name,
        email: ticket.email,
        ticketId: ticket.ticket_id,
        ticketType: ticket.ticket_type,
        status: ticket.status,
        checkinUrl: ticket.checkin_url,
        qrCodeDataUrl: ticket.qr_code_data_url,
        createdAt: ticket.created_at
      }
    });
  } catch (error) {
    console.error("Purchase error:", error.message);
    return res.status(500).json({ error: error.message || "Purchase failed" });
  }
});

router.get("/admin/tickets", (req, res) => {
  const q = (req.query.q || "").trim().toLowerCase();
  let tickets;
  if (q) {
    tickets = db
      .prepare(
        `
          SELECT *
          FROM tickets
          WHERE lower(name) LIKE ?
             OR lower(email) LIKE ?
             OR lower(ticket_id) LIKE ?
          ORDER BY id DESC
        `
      )
      .all(`%${q}%`, `%${q}%`, `%${q}%`);
  } else {
    tickets = db.prepare("SELECT * FROM tickets ORDER BY id DESC").all();
  }
  res.json({ tickets });
});

module.exports = router;
