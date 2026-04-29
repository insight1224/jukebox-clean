require("dotenv").config();
const path = require("path");
const express = require("express");
const db = require("./db");
const ticketRoutes = require("./routes/tickets");

const app = express();
const PORT = Number(process.env.PORT || 5050);

app.use(express.json());
app.use(express.urlencoded({ extended: true }));

app.use(express.static(path.join(__dirname, "..", "frontend")));

app.use("/api", ticketRoutes);

app.get("/health", (_req, res) => {
  res.json({ ok: true });
});

app.get("/checkin/:ticketId", (req, res) => {
  const ticketId = (req.params.ticketId || "").trim();
  if (!ticketId) {
    return res.status(400).send(renderCheckinMessage("Invalid ticket", "missing"));
  }

  const row = db.prepare("SELECT * FROM tickets WHERE ticket_id = ?").get(ticketId);
  if (!row) {
    return res.status(404).send(renderCheckinMessage("Invalid ticket", "invalid"));
  }

  if (row.status === "checked_in") {
    return res.status(409).send(
      renderCheckinMessage(`Ticket already used (${row.ticket_id})`, "used", row)
    );
  }

  db.prepare(
    "UPDATE tickets SET status = 'checked_in', checked_in_at = CURRENT_TIMESTAMP WHERE ticket_id = ?"
  ).run(ticketId);
  const updated = db.prepare("SELECT * FROM tickets WHERE ticket_id = ?").get(ticketId);
  return res.send(renderCheckinMessage(`Checked in successfully (${ticketId})`, "ok", updated));
});

app.get("/admin", (req, res) => {
  res.sendFile(path.join(__dirname, "..", "frontend", "admin.html"));
});

app.get("*", (_req, res) => {
  res.sendFile(path.join(__dirname, "..", "frontend", "index.html"));
});

app.listen(PORT, () => {
  console.log(`Ticketing MVP running on http://localhost:${PORT}`);
});

function renderCheckinMessage(message, state, ticket = null) {
  const palette =
    state === "ok"
      ? { bg: "#0f2a1b", border: "#2ccf75", text: "#d3ffe6" }
      : state === "used"
        ? { bg: "#2a1a10", border: "#f29f52", text: "#ffe8d1" }
        : { bg: "#2a1111", border: "#f05a5a", text: "#ffdede" };

  const detail = ticket
    ? `<p><strong>Name:</strong> ${ticket.name}</p><p><strong>Status:</strong> ${ticket.status}</p>`
    : "";

  return `
    <!doctype html>
    <html lang="en">
      <head>
        <meta charset="utf-8" />
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <title>Check-in</title>
        <style>
          body { margin:0; font-family: Arial,sans-serif; background:#090909; color:#f6f6f6; display:grid; place-items:center; min-height:100vh; }
          .card { width:min(540px,calc(100vw - 2rem)); background:${palette.bg}; border:1px solid ${palette.border}; border-radius:12px; padding:1.2rem; }
          h1 { margin:0 0 .6rem; font-size:1.4rem; color:${palette.text}; }
          p { margin:.35rem 0; }
        </style>
      </head>
      <body>
        <div class="card">
          <h1>${state === "ok" ? "✅" : state === "used" ? "❌" : "⚠️"} ${message}</h1>
          ${detail}
        </div>
      </body>
    </html>
  `;
}
