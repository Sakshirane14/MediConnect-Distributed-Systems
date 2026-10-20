"""
Seeds demo accounts for the FA-1 presentation. Run this AFTER the stack is
up (docker compose, or run_local.py) so /api/auth/register is reachable.

Usage:
    python seed_demo_data.py                # targets http://localhost:8000
    python seed_demo_data.py http://host:8000
"""
import sys
import json
import urllib.request

API_BASE = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:8000"
PASSWORD = "Demo@1234"

ACCOUNTS = [
    {"name": "Rahul Verma",  "email": "pharmacy.a@mediconnect.demo",    "password": PASSWORD, "role": "pharmacy_staff",    "pharmacy_id": "PHA001"},
    {"name": "Sneha Patil",  "email": "pharmacy.b@mediconnect.demo",    "password": PASSWORD, "role": "pharmacy_staff",    "pharmacy_id": "PHB002"},
    {"name": "Amit Joshi",   "email": "pharmacy.c@mediconnect.demo",    "password": PASSWORD, "role": "pharmacy_staff",    "pharmacy_id": "PHC003"},
    {"name": "Priya Sharma", "email": "distributor.a@mediconnect.demo", "password": PASSWORD, "role": "distributor_staff", "distributor_id": "DIS001"},
    {"name": "Karan Mehta",  "email": "distributor.b@mediconnect.demo", "password": PASSWORD, "role": "distributor_staff", "distributor_id": "DIS002"},
    {"name": "Divya Nair",   "email": "distributor.c@mediconnect.demo", "password": PASSWORD, "role": "distributor_staff", "distributor_id": "DIS003"},
]

def register(account):
    data = json.dumps(account).encode()
    req = urllib.request.Request(
        API_BASE + "/api/auth/register", data=data,
        headers={"Content-Type": "application/json"}, method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            body = json.loads(resp.read())
            print(f"  Created: {account['email']} ({account['role']}, "
                  f"pharmacy={account.get('pharmacy_id','-')}, distributor={account.get('distributor_id','-')})")
            return body
    except urllib.error.HTTPError as e:
        detail = json.loads(e.read()).get("detail", "")
        if "already registered" in str(detail):
            print(f"  Already exists: {account['email']}")
        else:
            print(f"  FAILED: {account['email']} -> {detail}")

if __name__ == "__main__":
    print(f"Seeding demo accounts against {API_BASE} ...")
    for acc in ACCOUNTS:
        register(acc)
    print("\nDone. Login with password: " + PASSWORD)
