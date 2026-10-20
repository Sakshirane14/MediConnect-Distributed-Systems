const WS_BASE = "ws://" + (window.location.hostname || "localhost") + ":8000";

class StockWatcher {
  constructor(medicineId, onUpdate) {
    this.medicineId = medicineId;
    this.onUpdate = onUpdate;
    this.ws = null;
    this.pingInterval = null;
  }

  connect() {
    this.ws = new WebSocket(WS_BASE + "/ws/stock/" + this.medicineId);
    this.ws.onopen = () => {
      console.log("[WS] Connected to stock feed: " + this.medicineId);
      this.pingInterval = setInterval(() => {
        if (this.ws && this.ws.readyState === WebSocket.OPEN) this.ws.send("ping");
      }, 30000);
    };
    this.ws.onmessage = (evt) => {
      try {
        const data = JSON.parse(evt.data);
        if (data.medicine_id && data.new_quantity !== undefined) {
          this.onUpdate(data);
        }
      } catch (e) {}
    };
    this.ws.onclose = () => {
      clearInterval(this.pingInterval);
    };
    this.ws.onerror = (e) => console.error("[WS] Stock error:", e);
  }

  disconnect() {
    clearInterval(this.pingInterval);
    if (this.ws) this.ws.close();
  }
}

/**
 * Subscribe to live replenishment-order status updates for a scope
 * (a pharmacy_id, or the literal 'DISTRIBUTOR' for the network-wide feed).
 * Calls onUpdate(payload) whenever an order is created/approved/rejected/
 * dispatched/received — no page refresh needed (FA-1 Stream-Oriented
 * Communication demo).
 */
function connectOrdersSocket(scope, onUpdate) {
  const ws = new WebSocket(WS_BASE + "/ws/orders/" + scope);
  let pingInterval;
  ws.onopen = () => {
    console.log("[WS] Connected to orders feed: " + scope);
    pingInterval = setInterval(() => {
      if (ws.readyState === WebSocket.OPEN) ws.send("ping");
    }, 30000);
  };
  ws.onmessage = (evt) => {
    if (evt.data === "pong") return;
    try {
      const data = JSON.parse(evt.data);
      onUpdate(data);
    } catch (e) {}
  };
  ws.onclose = () => clearInterval(pingInterval);
  ws.onerror = (e) => console.error("[WS] Orders feed error:", e);
  return ws;
}

/**
 * Subscribe to brand-new Medicine Requirement Lists addressed to this
 * distributor (called on the Distributor Dashboard).
 */
function connectRequirementsFeedSocket(distributorId, onUpdate) {
  const ws = new WebSocket(WS_BASE + "/ws/requirements/" + distributorId);
  let pingInterval;
  ws.onopen = () => {
    pingInterval = setInterval(() => { if (ws.readyState === WebSocket.OPEN) ws.send("ping"); }, 30000);
  };
  ws.onmessage = (evt) => {
    if (evt.data === "pong") return;
    try { onUpdate(JSON.parse(evt.data)); } catch (e) {}
  };
  ws.onclose = () => clearInterval(pingInterval);
  return ws;
}

/**
 * Subscribe to live distributor-response status for ONE requirement
 * (called on the Pharmacy Dashboard's requirement detail/comparison view)
 * — this is the "Distributor A -> Responded" live update, FA-1
 * Stream-Oriented Communication.
 */
function connectRequirementStatusSocket(requirementId, onUpdate) {
  const ws = new WebSocket(WS_BASE + "/ws/requirement/" + requirementId);
  let pingInterval;
  ws.onopen = () => {
    pingInterval = setInterval(() => { if (ws.readyState === WebSocket.OPEN) ws.send("ping"); }, 30000);
  };
  ws.onmessage = (evt) => {
    if (evt.data === "pong") return;
    try { onUpdate(JSON.parse(evt.data)); } catch (e) {}
  };
  ws.onclose = () => clearInterval(pingInterval);
  return ws;
}


/** Subscribe to new-notification signals for the logged-in business identity. */
function connectNotificationsSocket(recipientId, onUpdate) {
  const ws = new WebSocket(WS_BASE + "/ws/notifications/" + recipientId);
  let pingInterval;
  ws.onopen = () => {
    console.log("[WS] Connected to notifications feed: " + recipientId);
    pingInterval = setInterval(() => { if (ws.readyState === WebSocket.OPEN) ws.send("ping"); }, 30000);
  };
  ws.onmessage = (evt) => {
    if (evt.data === "pong") return;
    try { onUpdate(JSON.parse(evt.data)); } catch (e) {}
  };
  ws.onclose = () => clearInterval(pingInterval);
  ws.onerror = (e) => console.error("[WS] Notifications feed error:", e);
  return ws;
}

/**
 * Subscribe to the single global Image Requirement broadcast channel.
 * Fires on every upload AND every claim, for every connected dashboard —
 * this is what makes "first to claim wins" visible live (a distributor
 * who loses the race sees the item disappear from their own list within
 * the same second, not on next poll).
 */
function connectImageRequirementsSocket(onUpdate) {
  const ws = new WebSocket(WS_BASE + "/ws/image-requirements");
  let pingInterval;
  ws.onopen = () => {
    pingInterval = setInterval(() => { if (ws.readyState === WebSocket.OPEN) ws.send("ping"); }, 30000);
  };
  ws.onmessage = (evt) => {
    if (evt.data === "pong") return;
    try { onUpdate(JSON.parse(evt.data)); } catch (e) {}
  };
  ws.onclose = () => clearInterval(pingInterval);
  ws.onerror = (e) => console.error("[WS] Image requirements feed error:", e);
  return ws;
}
