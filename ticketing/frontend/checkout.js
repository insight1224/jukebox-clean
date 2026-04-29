let card;
let payments;
let ticketTypes = {};

const els = {
  name: document.getElementById("name"),
  email: document.getElementById("email"),
  ticketType: document.getElementById("ticketType"),
  priceText: document.getElementById("priceText"),
  buyBtn: document.getElementById("buyBtn"),
  errorText: document.getElementById("errorText"),
  checkoutCard: document.getElementById("checkoutCard"),
  successCard: document.getElementById("successCard"),
  ticketIdText: document.getElementById("ticketIdText"),
  ticketNameText: document.getElementById("ticketNameText"),
  ticketEmailText: document.getElementById("ticketEmailText"),
  ticketTypeText: document.getElementById("ticketTypeText"),
  checkinLink: document.getElementById("checkinLink"),
  qrImage: document.getElementById("qrImage")
};

function formatCents(cents) {
  return `$${(Number(cents || 0) / 100).toFixed(2)}`;
}

function selectedTicket() {
  return ticketTypes[els.ticketType.value];
}

function updatePrice() {
  const selected = selectedTicket();
  els.priceText.textContent = selected ? formatCents(selected.amountCents) : "$0.00";
}

async function loadConfig() {
  const res = await fetch("/api/public-config");
  const data = await res.json();
  ticketTypes = data.ticketTypes || {};

  els.ticketType.innerHTML = "";
  Object.entries(ticketTypes).forEach(([key, value]) => {
    const option = document.createElement("option");
    option.value = key;
    option.textContent = `${value.label} (${formatCents(value.amountCents)})`;
    els.ticketType.appendChild(option);
  });
  updatePrice();

  if (!data.applicationId || !data.locationId) {
    throw new Error("Square public config missing (application/location ID).");
  }

  if (!window.Square) {
    throw new Error("Square Web Payments SDK failed to load.");
  }

  payments = window.Square.payments(data.applicationId, data.locationId);
  card = await payments.card();
  await card.attach("#card-container");
}

async function tokenizeCard() {
  const tokenResult = await card.tokenize();
  if (tokenResult.status !== "OK") {
    throw new Error(tokenResult.errors?.[0]?.message || "Card tokenization failed");
  }
  return tokenResult.token;
}

async function purchase() {
  els.errorText.textContent = "";

  const name = (els.name.value || "").trim();
  const email = (els.email.value || "").trim();
  const ticketType = els.ticketType.value;

  if (!name || !email || !ticketType) {
    els.errorText.textContent = "Please complete all fields.";
    return;
  }

  els.buyBtn.disabled = true;
  els.buyBtn.textContent = "Processing...";

  try {
    const sourceId = await tokenizeCard();
    const res = await fetch("/api/purchase", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ sourceId, name, email, ticketType })
    });

    const data = await res.json();
    if (!res.ok || !data.success) {
      throw new Error(data.error || "Payment failed");
    }

    const ticket = data.ticket;
    els.ticketIdText.textContent = ticket.ticketId;
    els.ticketNameText.textContent = ticket.name;
    els.ticketEmailText.textContent = ticket.email;
    els.ticketTypeText.textContent = ticket.ticketType;
    els.checkinLink.href = ticket.checkinUrl;
    els.checkinLink.textContent = ticket.checkinUrl;
    els.qrImage.src = ticket.qrCodeDataUrl;

    els.checkoutCard.classList.add("hidden");
    els.successCard.classList.remove("hidden");
  } catch (err) {
    els.errorText.textContent = err.message || "Purchase failed.";
  } finally {
    els.buyBtn.disabled = false;
    els.buyBtn.textContent = "Buy Ticket";
  }
}

els.ticketType.addEventListener("change", updatePrice);
els.buyBtn.addEventListener("click", purchase);

loadConfig().catch((err) => {
  els.errorText.textContent = err.message || "Failed to initialize checkout.";
  els.buyBtn.disabled = true;
});
