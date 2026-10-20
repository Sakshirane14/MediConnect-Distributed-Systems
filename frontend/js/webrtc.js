/**
 * MediConnect Direct Chat — WebRTC RTCDataChannel P2P text messaging
 * between Pharmacy Staff <-> Distributor Staff.
 *
 * Webcam/microphone/video have been intentionally removed (this app does
 * not need a video call for its core use case) — but WebRTC itself is
 * retained, per FA-1 syllabus, as a genuine peer-to-peer transport: once
 * the RTCPeerConnection is established, chat messages travel directly
 * between the two browsers over an RTCDataChannel, not through the server.
 *
 * The signaling relay (main_api/app/ws/webrtc_signaling.py) is reused
 * completely unchanged — it only relays opaque JSON signaling messages
 * (offer/answer/ICE candidates) and never inspected media, so nothing
 * there needed to change to drop video.
 *
 * Same-laptop identity: peer/session identity is a random per-tab id
 * (crypto.randomUUID + sessionStorage), separate from the business login
 * identity, so two tabs on one laptop are always distinct peers.
 */

const WS_BASE_CALL = "ws://" + (window.location.hostname || "localhost") + ":8000";

const ICE_SERVERS = [
  { urls: "stun:stun.l.google.com:19302" },
  { urls: "stun:stun1.l.google.com:19302" },
];

function getPeerSessionId() {
  let id = sessionStorage.getItem("mediconnect_peer_session_id");
  if (!id) {
    id = (crypto.randomUUID ? crypto.randomUUID() : "peer-" + Math.random().toString(36).slice(2));
    sessionStorage.setItem("mediconnect_peer_session_id", id);
  }
  return id;
}

class MediChatSession {
  constructor(roomId, onStatusChange, onMessage, role) {
    this.roomId = roomId;
    this.onStatusChange = onStatusChange;
    this.onMessage = onMessage;
    this.role = role || "Peer";
    this.peerSessionId = getPeerSessionId();

    this.ws = null;
    this.pc = null;
    this.dataChannel = null;
    this.isInitiator = false;

    this._pendingCandidates = [];
    this._remoteDescSet = false;
  }

  start() {
    console.log(`[WebRTC] Starting chat session. role=${this.role} peer=${this.peerSessionId} room=${this.roomId}`);
    this._connectSignaling();
  }

  _connectSignaling() {
    const url = WS_BASE_CALL + "/ws/call/" + this.roomId + "?peer=" + encodeURIComponent(this.peerSessionId);
    console.log("[WebRTC] Connecting signaling:", url);
    this.ws = new WebSocket(url);

    this.ws.onopen = () => {
      console.log("[WebRTC] Signaling connected. room=" + this.roomId + " peer=" + this.peerSessionId);
      this.onStatusChange("connecting", "Connected to room. Waiting for the other side...");
    };
    this.ws.onmessage = async (evt) => {
      const msg = JSON.parse(evt.data);
      console.log("[WebRTC] <-", msg.type, msg);
      await this._handleSignal(msg);
    };
    this.ws.onerror = (e) => {
      console.error("[WebRTC] Signaling error:", e);
      this.onStatusChange("error", "Signaling connection error.");
    };
    this.ws.onclose = () => {
      console.log("[WebRTC] Signaling closed.");
      this.onStatusChange("ended", "Chat session ended.");
    };
  }

  async _handleSignal(msg) {
    switch (msg.type) {
      case "waiting":
        this.onStatusChange("waiting", "Waiting for the other participant to join...");
        break;

      case "peer_joined":
        console.log("[WebRTC] Peer joined -> I am the initiator, creating data channel + offer");
        this.isInitiator = true;
        this._createPeerConnection();
        this.dataChannel = this.pc.createDataChannel("mediconnect-chat");
        this._wireDataChannel();
        try {
          const offer = await this.pc.createOffer();
          await this.pc.setLocalDescription(offer);
          this._send({ type: "offer", sdp: offer.sdp });
          console.log("[WebRTC] Offer sent");
          this.onStatusChange("connecting", "Participant joined! Establishing connection...");
        } catch (e) {
          console.error("[WebRTC] Failed to create/send offer:", e);
          this.onStatusChange("error", "Failed to start chat: " + e.message);
        }
        break;

      case "offer":
        console.log("[WebRTC] Offer received -> creating answer");
        this._createPeerConnection();
        this.pc.ondatachannel = (evt) => {
          console.log("[WebRTC] Data channel received from initiator");
          this.dataChannel = evt.channel;
          this._wireDataChannel();
        };
        try {
          await this.pc.setRemoteDescription({ type: "offer", sdp: msg.sdp });
          this._remoteDescSet = true;
          await this._flushPendingCandidates();
          const answer = await this.pc.createAnswer();
          await this.pc.setLocalDescription(answer);
          this._send({ type: "answer", sdp: answer.sdp });
          console.log("[WebRTC] Answer sent");
          this.onStatusChange("connecting", "Answering...");
        } catch (e) {
          console.error("[WebRTC] Failed to handle offer:", e);
          this.onStatusChange("error", "Failed to answer: " + e.message);
        }
        break;

      case "answer":
        console.log("[WebRTC] Answer received");
        try {
          await this.pc.setRemoteDescription({ type: "answer", sdp: msg.sdp });
          this._remoteDescSet = true;
          await this._flushPendingCandidates();
        } catch (e) {
          console.error("[WebRTC] Failed to set remote answer:", e);
        }
        break;

      case "ice-candidate":
        if (msg.candidate) {
          if (this.pc && this._remoteDescSet) {
            try { await this.pc.addIceCandidate(msg.candidate); }
            catch (e) { console.warn("[WebRTC] Failed to add ICE candidate:", e); }
          } else {
            this._pendingCandidates.push(msg.candidate);
          }
        }
        break;

      case "peer_left":
        this.onStatusChange("ended", "The other participant left the chat.");
        break;

      case "error":
        console.error("[WebRTC] Server error:", msg.message);
        this.onStatusChange("error", msg.message || "Room error.");
        break;
    }
  }

  async _flushPendingCandidates() {
    while (this._pendingCandidates.length) {
      const c = this._pendingCandidates.shift();
      try { await this.pc.addIceCandidate(c); } catch (e) { console.warn("[WebRTC] Queued ICE candidate failed:", e); }
    }
  }

  _createPeerConnection() {
    if (this.pc) return;
    this.pc = new RTCPeerConnection({ iceServers: ICE_SERVERS });

    this.pc.onicecandidate = (evt) => {
      if (evt.candidate) {
        console.log("[WebRTC] ICE candidate generated");
        this._send({ type: "ice-candidate", candidate: evt.candidate });
      }
    };

    this.pc.onconnectionstatechange = () => {
      console.log("[WebRTC] connectionState:", this.pc.connectionState);
      if (this.pc.connectionState === "failed") this.onStatusChange("error", "Connection failed.");
      if (this.pc.connectionState === "disconnected") this.onStatusChange("error", "Connection lost.");
    };
    this.pc.oniceconnectionstatechange = () => {
      console.log("[WebRTC] iceConnectionState:", this.pc.iceConnectionState);
    };
    this.pc.onsignalingstatechange = () => {
      console.log("[WebRTC] signalingState:", this.pc.signalingState);
    };
  }

  _wireDataChannel() {
    this.dataChannel.onopen = () => {
      console.log("[WebRTC] DataChannel OPEN");
      this.onStatusChange("connected", "Connected — you can chat now.");
    };
    this.dataChannel.onclose = () => {
      console.log("[WebRTC] DataChannel closed");
      this.onStatusChange("ended", "Chat session ended.");
    };
    this.dataChannel.onerror = (e) => {
      console.error("[WebRTC] DataChannel error:", e);
    };
    this.dataChannel.onmessage = (evt) => {
      let payload;
      try { payload = JSON.parse(evt.data); } catch (e) { payload = { text: evt.data, sender: "Peer" }; }
      console.log("[WebRTC] Message received:", payload);
      this.onMessage(payload);
    };
  }

  sendMessage(text, senderLabel) {
    if (!this.dataChannel || this.dataChannel.readyState !== "open") {
      console.warn("[WebRTC] Cannot send - data channel not open");
      return false;
    }
    const payload = { text, sender: senderLabel, timestamp: new Date().toISOString() };
    this.dataChannel.send(JSON.stringify(payload));
    return true;
  }

  _send(msg) {
    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify(msg));
    } else {
      console.warn("[WebRTC] Tried to send while socket not open:", msg.type);
    }
  }

  hangUp() {
    if (this.dataChannel) { this.dataChannel.close(); this.dataChannel = null; }
    if (this.pc) { this.pc.close(); this.pc = null; }
    if (this.ws) { this.ws.close(); this.ws = null; }
    this.onStatusChange("ended", "Chat session ended.");
  }
}
