# MediConnect — Distributed Pharmacy Procurement & Multi-Distributor Medicine Supply Network

**FA-1 Distributed Systems Mini Project.** A pharmacy uses one MediConnect
interface to publish medicine requirements, search and communicate with
multiple independent distributor nodes, compare their responses, place
procurement orders, and keep operating even when individual distributor
nodes fail.

---

## 1. Architecture

```
                      PHARMACY (client)
                            |
                            |  HTTP
                            v
                     MediConnect Main API
                        (Middleware)
                            |
              Concurrent gRPC Fan-Out (parallel, not sequential)
                 /          |          \
                /           |           \
               v            v            v
       Distributor A  Distributor B  Distributor C   (+ Distributor D, live)
          DIS001          DIS002         DIS003
         (own DB)        (own DB)       (own DB)
               \            |           /
                \           |          /
                    RabbitMQ (7 routing keys)
                            |
                  Notification Service (consumer)

          WebSockets: live order status, requirement responses, network health
          WebRTC (RTCDataChannel): Pharmacy <-> Distributor direct text chat
```

Each **Distributor** is an independent node: its own gRPC service, its own
SQLite database, configured purely by environment variables
(`DISTRIBUTOR_ID`, `DISTRIBUTOR_NAME`, `GRPC_PORT`, `DB_PATH`). The **same
Docker image** runs as Distributor A, B, C, or D — adding a new one is a
config change, not a code change. Main API is middleware: it discovers
distributors via a lightweight self-registration mechanism and never
touches a distributor's database directly.

Pharmacies are **clients** of this network (their own local inventory lives
centrally in Main API's DB), not independent distributed nodes themselves
— the distributed-systems demonstration is the distributor network.

---

## 2. Two ways to search/procure

**Workflow A — Urgent single-medicine search ("Find Suppliers").**
Pharmacy searches one medicine; Main API fans the query out via gRPC to
every registered distributor concurrently and shows price/stock/response
time from each. Demonstrates RPC directly and immediately.

**Workflow B — Medicine Requirement List.**
Pharmacy bundles several needed medicines (with quantities and priority)
into one requirement and broadcasts it to the whole distributor network.
Each distributor independently evaluates the list against its **own** live
inventory (via gRPC) and submits a response. The pharmacy sees live
"Distributor A → Responded / Waiting..." status (WebSockets) and a
side-by-side comparison, then can place an order or start a direct chat
with any distributor.

---

## 3. WebRTC — text chat only, no webcam

Video/audio calling was intentionally removed — this app's core use case
doesn't need it, and debugging cross-tab camera contention wasn't worth
further time. **WebRTC itself is retained** (it's explicitly FA-1 syllabus)
as a genuine **RTCDataChannel peer-to-peer text chat** between Pharmacy
Staff and Distributor Staff. Once the two peers connect, messages travel
directly browser-to-browser, not through the server.

The signaling relay (`main_api/app/ws/webrtc_signaling.py`) needed **zero
changes** to support this — it only ever relayed opaque JSON
offer/answer/ICE messages and never inspected media, so dropping video was
purely a frontend change (`frontend/js/webrtc.js`, `frontend/chat.html`).

Same-laptop identity: each browser tab gets its own random
`crypto.randomUUID()` peer id in `sessionStorage`, separate from the
logged-in business identity, so two tabs (or a normal + incognito window)
on one laptop are always treated as distinct peers.

---

## 4. Running it

### Option A — Docker Compose (recommended)
```bash
docker compose up --build
```
Brings up: `rabbitmq`, `distributor-a`, `distributor-b`, `distributor-c`,
`main-api`, `notification-service`, `frontend`. Distributor D is **not**
started by default (see Scalability demo below).

- Frontend: http://localhost:8080
- Main API + Swagger: http://localhost:8000/docs
- RabbitMQ management UI: http://localhost:15672 (guest/guest)

Then seed demo accounts:
```bash
python seed_demo_data.py
```

### Option B — Local dev (no Docker, no RabbitMQ)
```bash
python3 -m venv venv
./venv/bin/pip install -r main_api/requirements.txt -r distributor_service/requirements.txt
./venv/bin/python run_local.py
```
In a second terminal:
```bash
./venv/bin/python seed_demo_data.py
```
This starts Main API + 3 distributor nodes + a static frontend server,
without RabbitMQ. Everything works except the async notification pipeline
(publishing simply logs a non-fatal warning and is skipped — demonstrates
the fault-tolerant, non-blocking design). Use Docker Compose for the full
RabbitMQ demo.

**Note on versions:** `grpcio`, `grpcio-tools`, and `protobuf` are pinned
to exact versions in every `requirements.txt` (not `>=`) because the
`.proto`-generated code embeds a minimum-required protobuf runtime version
— an unpinned `pip install` can resolve an older protobuf than the
generated code needs, causing a `VersionError` at import time. If you ever
regenerate the `.proto` files yourself, regenerate with the exact pinned
`grpcio-tools` version so this stays consistent.

---

## 5. Logging in

- **Pharmacy A/B/C Staff:** `pharmacy.a@mediconnect.demo` / `pharmacy.b@...` / `pharmacy.c@...`
- **Distributor A/B/C Staff:** `distributor.a@mediconnect.demo` / `distributor.b@...` / `distributor.c@...`
- Password for all: `Demo@1234`

(Or register your own — Pharmacy Staff picks a pharmacy at sign-up,
Distributor Staff picks a distributor node.)

---

## 6. Demonstrating each mechanism

### gRPC (RPC)
Pharmacy Dashboard → "Find Suppliers" → search "Dolo" — fans out to every
registered distributor in parallel, shows price/stock/response time.

### RabbitMQ (Message-Oriented)
```bash
docker compose logs -f notification-service
```
Create a requirement, submit a distributor response, or move an order
through its lifecycle — watch each of the 7 event types get consumed
asynchronously (`requirement_created`, `distributor_response_submitted`,
`order_created`, `order_approved`, `order_rejected`, `order_dispatched`,
`order_received`).

### WebSockets (Stream-Oriented)
Open a Pharmacy Dashboard's requirement comparison view and a Distributor
Dashboard side by side. Submit a response on the distributor side — the
pharmacy's "Distributor X → Responded" status updates live with no
refresh. Same for order status and the Network Status page.

### WebRTC (P2P / Distributed Multimedia)
From a Pharmacy or Distributor dashboard, click "Direct Chat" — opens
`chat.html?room=...`. Open the same room as the other role (Pharmacy in
one tab, that Distributor in another) to connect and exchange text
messages over an RTCDataChannel.

### Fault Tolerance
```bash
docker compose stop distributor-b
```
Network Status page shows DIS002 → OFFLINE within seconds. Search again —
Distributor A and C still respond; B shows as unavailable; the request
doesn't fail.
```bash
docker compose start distributor-b
```

### Scalability — add Distributor D live
**Docker:**
```bash
docker compose --profile scale up -d distributor-d
```
**Local dev (new terminal, while `run_local.py` is running):**
```bash
python add_distributor_d.py
```
Either way, Distributor D self-registers with Main API automatically.
Refresh the Network Status page — `registered_nodes: 3 → 4`. Search any
medicine again — Distributor D participates with zero Main API code
changes (see `distributor_client.py` / `registry.py` — there is no
per-distributor branching logic anywhere).

### Concurrency
Two pharmacies approving orders against the same distributor's limited
stock at the same instant: exactly one succeeds, the other gets a `409
Insufficient stock`, and the distributor's stock never goes negative. See
`TESTING.md` §5 for a scripted reproduction.

---

## 7. FA-1 Syllabus Mapping

### Unit 1 — Introduction to Distributed Systems

| Topic | Implementation |
|---|---|
| Definition | Multiple independent Distributor services (own gRPC endpoint, own DB) cooperate, presented to the pharmacy as one MediConnect interface. |
| Goals | Resource sharing (distributor inventories reachable through one platform), access/location transparency (pharmacy never sees `distributor-a:50051` directly), scalability (Distributor D joins live), availability (surviving nodes keep serving on failure). |
| Types | Distributed Information System (inventory data across nodes) + Distributed Application/Service-Based System. |
| Architecture | Client-server (browser ↔ Main API) + service-oriented (each distributor is its own service) + event-driven (RabbitMQ). |
| Design Issues | Concurrency (atomic `PlaceOrder` at the owning node), partial failure (per-node try/except + timeout), communication delay (`SIMULATE_DELAY_MS`), consistency (`UPDATE...WHERE quantity>=?`), scalability (dynamic registry), timeout handling (2s gRPC deadline). |
| Middleware | Main API: discovers distributors, invokes gRPC, aggregates responses, manages RabbitMQ/WebSocket, hides all of this from the frontend. |
| Overview | Independent networked services cooperating over defined protocols — documented above and directly observable via `docker compose ps`. |
| Distributed Multimedia | WebRTC RTCDataChannel chat + optional Purchase Order/Invoice/quotation document attachments. |
| Model of Distributed Computations | Independent processes: Main API, Distributor A/B/C(/D), RabbitMQ consumer (notification-service), WebSocket/WebRTC signaling. |
| Virtualization | Docker Compose — every service in its own container; Distributor A/B/C/D share one image, differentiated only by env vars. |

### Unit 2 — Communication

| Topic | Implementation |
|---|---|
| Fundamentals | Browser → Main API (HTTP/WS) → independent Distributor services (gRPC). |
| RPC | gRPC `SearchMedicine`, `GetMedicineDetails`, `PlaceOrder`, `UpdateStock`, `HealthCheck` — `proto/pharmacy.proto`. |
| Message-Oriented | RabbitMQ direct exchange `mediconnect.events`, 7 routing keys, consumed by `notification_service`. |
| Stream-Oriented | WebSockets: `/ws/orders/{scope}`, `/ws/requirement/{id}`, `/ws/requirements/{distributor_id}` — live status with no polling. |
| P2P Messaging | WebRTC RTCDataChannel direct text chat, Pharmacy ↔ Distributor. |
| WebRTC | `frontend/js/webrtc.js` + `chat.html`, reusing the unchanged signaling relay. |
| Names/Identifiers/Addresses | Name = "HealthMed Distributors" (human label); Identifier = "DIS001" (stable key); Address = "distributor-a:50051" (gRPC endpoint) — see `registry.py`. |
| Fault Tolerance | Per-node gRPC timeout + try/except in `distributor_client.py`; one node's failure never fails the aggregate response. |

---

## 8. Known limitations (by design, for FA-1 scope)

- Distributor stock is decremented at **Approve** time via gRPC `PlaceOrder`
  (atomic at the owning node) — this is the checkpoint that satisfies the
  concurrency requirement. Dispatch/Receive are status-only afterward.
- The distributor registry is in-memory (re-populated by self-registration
  heartbeats every 20s) — a Main API restart briefly shows 0 nodes until
  distributors re-announce; this is intentional simplicity for FA-1 scope,
  not a persistent service-discovery database.
- No payment gateway, GPS tracking, AI recommendations, blockchain,
  Kubernetes, or enterprise auth — intentionally out of scope.
- Business documents (quotations, purchase orders) are uploaded via a
  simple shared endpoint rather than a full DMS.
- `run_local.py` does not start RabbitMQ or the notification service; use
  Docker Compose for the full async-messaging demo.

---

## 9. 5–10 Minute Demo Sequence

1. **(0:00)** `docker compose up --build` → `docker compose ps` shows independent containers. *(Virtualization, Architecture)*
2. **(1:00)** Log in as Pharmacy A → Dashboard: local inventory shows Dolo 650 LOW STOCK.
3. **(2:00)** Click "Find Suppliers" → search "Dolo" → 3 distinct gRPC responses with different price/stock/response-time. *(RPC, Resource Sharing, Transparency)*
4. **(3:00)** Select several low-stock medicines → "Create Requirement List" → submit. *(Requirement flow)*
5. **(3:30)** `docker compose logs -f notification-service` — show `requirement_created` event. *(Message-Oriented Communication)*
6. **(4:30)** Log in as Distributor A (separate tab) → see the new requirement live (WebSocket push) → "Evaluate & Submit Response". *(Stream-Oriented, independent computation)*
7. **(5:00)** Back on Pharmacy A's comparison view → "Distributor A → Responded" appears with no refresh; repeat with Distributor B for a partial-fulfilment contrast. *(Stream-Oriented Communication)*
8. **(6:00)** Place an order with the chosen distributor → approve → dispatch → receive; pharmacy's local stock visibly increases. *(Concurrency-safe order lifecycle)*
9. **(7:00)** Open Direct Chat between Pharmacy A and that Distributor in two tabs — exchange a couple of RTCDataChannel messages, no webcam. *(P2P / WebRTC)*
10. **(8:00)** `docker compose stop distributor-b` → Network Status page → DIS002 OFFLINE → search again → A and C still respond. *(Fault Tolerance)*
11. **(9:00)** `docker compose --profile scale up -d distributor-d` → Network Status → 3 → 4 nodes → search again → D participates automatically. *(Scalability)*
12. **(10:00)** Close with the syllabus mapping table above.

---

## 10. Questions & Simple Answers

**Q: Why is Main API "middleware" and not just a proxy?**
A: It doesn't just forward requests — it discovers which distributors currently exist, fans a single logical query out to several of them in parallel, aggregates/normalizes their independent responses, applies a timeout so one slow node can't block the rest, and republishes results over WebSockets/RabbitMQ. Hiding all of that from the browser is exactly the middleware role.

**Q: How do you actually prove Distributor A/B/C aren't just rows in one table?**
A: Each runs as its own OS process/container with its own gRPC port and its own SQLite file. Killing one container's process only affects requests to that one node — provable live with `docker compose stop distributor-b`.

**Q: How does adding Distributor D not require new code?**
A: `registry.py` holds a plain dict populated by whichever distributors have self-registered; `distributor_client.py`'s search function loops over `get_registry()` generically. There is no `if distributor_id == "DIS004"` anywhere.

**Q: Where exactly does concurrency get enforced?**
A: Inside `distributor_service/app/inventory.py`'s `place_order()` — a single SQL `UPDATE medicines SET quantity = quantity - ? WHERE medicine_id = ? AND quantity >= ?`. SQLite executes that atomically, so two simultaneous callers can never both succeed past the available quantity.

**Q: Why RTCDataChannel and not just a WebSocket for chat?**
A: To keep a genuine, syllabus-required WebRTC peer-to-peer demonstration — once connected, messages travel directly between the two browsers rather than through the server, which a plain WebSocket chat would not demonstrate.

**Q: What happens if RabbitMQ is down?**
A: `publish_event()` catches the failure and logs a warning; the gRPC search and REST order/requirement flows are completely unaffected — messaging is deliberately non-blocking and best-effort.
