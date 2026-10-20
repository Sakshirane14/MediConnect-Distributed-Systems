# Unit 3 & Unit 4 — Syllabus Coverage

This documents exactly what was added for Unit 3 (Synchronization) and
Unit 4 (Emerging Distributed Paradigms), what's genuinely implemented vs.
what's a deliberate simplification, and — for Unit 4 — why most of it is
mapped to existing architecture rather than built as new code.

---

## UNIT 3 — Synchronization (implemented)

| Syllabus Topic | Where | What it actually does |
|---|---|---|
| **Logical Clocks (Lamport's Algorithm)** | `distributor_service/app/clock.py`, `main_api/app/sync/lamport.py` | Every distributor node AND Main API each keep their own independent monotonic counter. Every gRPC call (`HealthCheck`, `PlaceOrder`) piggybacks the sender's clock; the receiver applies the real rule: `local = max(local, received) + 1`. No shared clock exists anywhere — that's the whole point. |
| **Vector Clocks** | `main_api/app/sync/lamport.py` (`_vector_clock`) | Main API tracks one component per known node (`{"MAIN": n, "DIS001": n, ...}`), updated from each node's reported Lamport value. **Honest simplification**: nodes never talk to each other, only to Main API, so this is a vector *as observed by the hub*, not a textbook peer-to-peer vector clock. Documented as such in the code. |
| **Clock Synchronization** | `proto/pharmacy.proto` (`server_unix_time`), `main_api/app/sync/lamport.py` (`record_clock_offset`) | **Cristian's Algorithm**, implemented for real: each node stamps its wall-clock time when replying to `HealthCheck`; Main API uses the measured round-trip time to estimate that node's clock offset. Shown live on the Network Status page. |
| **Election Algorithms (Bully Algorithm)** | `main_api/app/sync/election.py` | The real ranking rule (highest online ID wins) and the real trigger condition (coordinator unreachable → re-elect) are implemented and tested — killing the current coordinator's process auto-elects the next-highest node within one status check. **Honest simplification**: the ELECTION/COORDINATOR messages are logged as a trace rather than literally sent node-to-node over new gRPC calls, since nodes have no channel to each other in this architecture. A "Trigger Election" button runs it on demand too. |
| **Mutual Exclusion Algorithms** | `main_api/app/routers/orders.py` (approve), `main_api/app/routers/image_requirements.py` (claim) | This reuses your **already-working, already-tested** atomic SQL guards (`UPDATE ... WHERE quantity >= ?` and `UPDATE ... WHERE status='open'`) — no new locking logic was added, because what you had already *was* a correct mutual-exclusion mechanism. What's new is explicit `mutex_request` / `mutex_grant` / `mutex_deny` logging around both of them, visible in the new Distributed Event Log, so the request → critical-section → grant-or-deny sequence is something you can actually point to and narrate in a viva. |
| **Global State (Chandy-Lamport-inspired)** | `main_api/app/sync/snapshot.py` | "Take Snapshot" captures Main API's own state plus every online node's full inventory within one async call, tagged with the Lamport clock at capture. **Honest simplification**: the real Chandy-Lamport algorithm also captures in-flight *messages* on channels between processes (relevant for systems with long-lived streaming channels); this system's gRPC calls are all single request/response, so there's nothing analogous to drain. The genuinely hard part — capturing independent processes' state at approximately the same instant and tagging it with logical, not wall-clock, time — is real. |
| Beacon protocol | *(not implemented)* | A wireless/sensor-network clock-sync mechanism — not a meaningful fit for an HTTP/gRPC web app. Skipped rather than faked. |
| Self-study (Lodha-Kshemkalyani fair mutex, Knapp's deadlock classification) | *(not implemented)* | Syllabus marks these as self-study/theory, not implementation. |

### Where to see it in the running app
Open **Network Status** (`network-status.html`) — every Unit 3 topic above has a live panel there:
Coordinator/Election, Clocks table (Lamport + Cristian's offset), Distributed Event Log, Global Snapshot.

---

## UNIT 4 — Emerging Distributed Paradigms (Distributed File System implemented; rest mapped)

Unit 4 is mostly comparative/case-study material. Rather than bolt on
unconvincing toy versions of Kubernetes or a blockchain, here's the
honest mapping of what this project already does to each concept — this
is what you'd say in a viva if asked "where's Unit 4":

| Topic | Mapping |
|---|---|
| **Distributed Object-Based Systems** | Already fully demonstrated by the gRPC layer itself — each distributor node exposes remote objects/methods (`SearchMedicine`, `PlaceOrder`, ...) invoked transparently by Main API, which is exactly this paradigm. |
| **Distributed Web-Based Systems** | The entire app: REST/HTTP + WebSockets between browser and Main API. |
| **Distributed File System** | **Implemented (simplified).** `storage_service/` is a tiny storage node; three run as independent containers/folders named **Pune, Mumbai, Delhi** (like HDFS DataNodes). Main API (`main_api/app/dfs/storage.py`) plays the NameNode: every uploaded requirement photo is copied to all regions at once, and the `image_replicas` table records which region holds which file. Reading a photo tries Pune first, then Mumbai, then Delhi — stop a region and photos still open. Seen on the Network Status page ("Distributed File Storage — Regions") and as region chips under each photo on both dashboards. **Honest limits:** photos are small so there is no chunking, "regions" are labels on one laptop, and a region that was down during an upload is not auto-repaired later. |
| **Serverless Architectures** | `notification_service` is the closest real analogue: it's stateless and purely event-triggered (reacts to a RabbitMQ message, does its job, exits its handler) — the same execution model as a FaaS function, just not deployed on an actual serverless platform. |
| **Case studies (Megaport, Cloudflare, AWS, Hadoop, Kubernetes)** | Comparative theory, not implementable in a student mini-project — e.g. "our distributor registry ≈ Kubernetes service discovery," "our RabbitMQ + notification_service ≈ AWS SNS/SQS + Lambda," "per-node independent DB ≈ HDFS-style sharding." Use these as talking points, not features. |
| Self-study (Blockchain/DLT, distributed DB tradeoffs) | Theory only, per syllabus. |

**Why only the DFS is new code:** object-based RPC, web-based architecture and event-driven "serverless-style" processing were already built for Units 1–2, and the DFS was the one Unit 4 idea that could be shown for real with a small, honest implementation. Blockchain and the big case studies stay theory — faking them would be less defensible in a viva than explaining why.
