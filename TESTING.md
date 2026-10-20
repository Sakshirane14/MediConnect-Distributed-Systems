# MediConnect — FA-1 Verification & Testing Guide

Exact commands to test each Distributed Systems concept.

## 0. Seed demo accounts (once, after the stack is up)
```bash
python seed_demo_data.py
```
Creates pharmacy.a/b/c@mediconnect.demo and distributor.a/b/c@mediconnect.demo — password `Demo@1234` for all.

## 1. Verify containers
```bash
docker compose ps
```
Expected: rabbitmq, distributor-a, distributor-b, distributor-c, main-api, notification-service, frontend — all `Up`.

## 2. Distributed gRPC search
```bash
TOKEN=$(curl -s -X POST http://localhost:8000/api/auth/login -H "Content-Type: application/json" \
  -d '{"email":"pharmacy.a@mediconnect.demo","password":"Demo@1234"}' | python3 -c "import sys,json;print(json.load(sys.stdin)['access_token'])")

curl -s "http://localhost:8000/api/medicines/search?query=Dolo" -H "Authorization: Bearer $TOKEN"
```
Expect 3 results (DIS001/002/003), each with its own price, quantity, and response_ms.

## 3. Medicine Requirement List — independent distributor responses
```bash
REQ=$(curl -s -X POST http://localhost:8000/api/requirements -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d '{"items":[{"medicine_id":"MED001","medicine_name":"Dolo 650","requested_quantity":100,"priority":"High"}]}')
echo $REQ
REQ_ID=$(echo $REQ | python3 -c "import sys,json;print(json.load(sys.stdin)['requirement_id'])")

D1_TOKEN=$(curl -s -X POST http://localhost:8000/api/auth/login -H "Content-Type: application/json" \
  -d '{"email":"distributor.a@mediconnect.demo","password":"Demo@1234"}' | python3 -c "import sys,json;print(json.load(sys.stdin)['access_token'])")
D2_TOKEN=$(curl -s -X POST http://localhost:8000/api/auth/login -H "Content-Type: application/json" \
  -d '{"email":"distributor.b@mediconnect.demo","password":"Demo@1234"}' | python3 -c "import sys,json;print(json.load(sys.stdin)['access_token'])")

curl -s -X POST http://localhost:8000/api/requirements/$REQ_ID/respond -H "Authorization: Bearer $D1_TOKEN"
curl -s -X POST http://localhost:8000/api/requirements/$REQ_ID/respond -H "Authorization: Bearer $D2_TOKEN"

curl -s http://localhost:8000/api/requirements/$REQ_ID -H "Authorization: Bearer $TOKEN" | python3 -m json.tool
```
Distributor A (500 units of Dolo) should show `can_fulfil_all: true`; Distributor B (80 units) should show `can_fulfil_all: false` — genuinely computed from each one's own live inventory.

## 4. Full order lifecycle
```bash
ORDER=$(curl -s -X POST http://localhost:8000/api/orders -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d '{"distributor_id":"DIS001","medicine_id":"MED001","medicine_name":"Dolo 650","quantity":50,"unit_price":26}')
ORDER_ID=$(echo $ORDER | python3 -c "import sys,json;print(json.load(sys.stdin)['order_id'])")

curl -s -X POST http://localhost:8000/api/orders/$ORDER_ID/approve  -H "Authorization: Bearer $D1_TOKEN"
curl -s -X POST http://localhost:8000/api/orders/$ORDER_ID/dispatch -H "Authorization: Bearer $D1_TOKEN"
curl -s -X POST http://localhost:8000/api/orders/$ORDER_ID/receive  -H "Authorization: Bearer $TOKEN"

curl -s http://localhost:8000/api/pharmacy/PHA001/inventory -H "Authorization: Bearer $TOKEN"
```
Pharmacy A's local Dolo 650 stock should have increased by 50.

## 5. Concurrency safety (oversell protection)
Distributor A has 500 units of Dolo. Fire two approvals from two different
pharmacies' orders that together exceed availability (e.g. 300 + 300) —
exactly one succeeds, the other returns `409 Insufficient stock`, and
`distributor_a.db`'s quantity never goes negative. (This is exactly what
`add_distributor_d.py`'s sibling test script in the project's dev history
verified — see README §6 "Concurrency".)

## 6. Fault tolerance
```bash
docker stop mediconnect-distributor-b
curl -s "http://localhost:8000/api/network/status" -H "Authorization: Bearer $TOKEN"
curl -s "http://localhost:8000/api/medicines/search?query=Dolo" -H "Authorization: Bearer $TOKEN"
docker start mediconnect-distributor-b
```
Distributor B shows `offline`; search still returns A and C; the request never fails outright.

## 7. Scalability — add Distributor D live
```bash
docker compose --profile scale up -d distributor-d
curl -s "http://localhost:8000/api/network/status" -H "Authorization: Bearer $TOKEN"
curl -s "http://localhost:8000/api/medicines/search?query=Dolo" -H "Authorization: Bearer $TOKEN"
```
`registered_nodes` goes from 3 to 4; the search response now includes DIS004 with zero Main API code changes.

## 8. WebSockets (live updates)
Open a Pharmacy Dashboard's requirement comparison modal and a Distributor
Dashboard side by side in the browser. Submit a distributor response —
the pharmacy's view updates within a second, no refresh.

## 9. WebRTC RTCDataChannel chat (same laptop)
1. Tab 1: log in as Pharmacy A → open `chat.html?room=CHAT-PHA001-DIS001&with=DIS001`.
2. Tab 2: log in as Distributor A → open `chat.html?room=CHAT-PHA001-DIS001&with=PHA001` (same room id).
3. Watch the browser console for `[WebRTC]` logs: signaling connect → `peer_joined` → offer/answer → ICE candidates → `DataChannel OPEN`.
4. Type a message in either tab — it should appear in the other tab instantly, with no server round-trip after the connection is established.
