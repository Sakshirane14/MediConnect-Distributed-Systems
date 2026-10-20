from __future__ import annotations
import os
import aiosqlite
import logging

logger = logging.getLogger(__name__)

# Reusable distributor node — configured entirely via environment variables.
# This is the FA-1 Scalability mechanism: adding "Distributor D" means
# running this exact same image with different env vars, no code changes.
DISTRIBUTOR_ID   = os.environ.get("DISTRIBUTOR_ID", "DIS001")
DISTRIBUTOR_NAME = os.environ.get("DISTRIBUTOR_NAME", "Distributor")
DB_PATH = os.environ.get(
    "DB_PATH",
    os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), f"data/distributor_{DISTRIBUTOR_ID.lower()}.db"),
)

# Seed inventory per distributor — deliberately different prices/stock per
# node so the "compare distributors" search result is meaningful (FA-1 §2).
SEED_DATA = {
    "DIS001": [  # HealthMed Distributors
        ("MED001", "Dolo 650", "Paracetamol 650mg tablet for fever and pain relief", 26.00, 500, 20),
        ("MED002", "Crocin Advance", "Paracetamol 500mg fast-acting tablet", 18.00, 300, 15),
        ("MED003", "Azithromycin 500mg", "Antibiotic tablet - prescription required", 95.00, 120, 10),
        ("MED004", "Pantoprazole 40mg", "Proton pump inhibitor for acidity", 32.00, 0, 15),
        ("MED005", "Cetirizine 10mg", "Antihistamine for allergy relief", 14.00, 400, 20),
    ],
    "DIS002": [  # CareSupply Healthcare
        ("MED001", "Dolo 650", "Paracetamol 650mg tablet for fever and pain relief", 24.00, 80, 20),
        ("MED002", "Crocin Advance", "Paracetamol 500mg fast-acting tablet", 17.00, 250, 15),
        ("MED006", "Ibuprofen 400mg", "NSAID for pain, fever, and inflammation", 40.00, 200, 15),
        ("MED007", "Metformin 500mg", "Oral diabetes medication", 28.00, 350, 20),
        ("MED008", "Atorvastatin 10mg", "Cholesterol-lowering statin", 65.00, 90, 15),
    ],
    "DIS003": [  # MediWholesale
        ("MED001", "Dolo 650", "Paracetamol 650mg tablet for fever and pain relief", 25.00, 150, 20),
        ("MED003", "Azithromycin 500mg", "Antibiotic tablet - prescription required", 88.00, 60, 10),
        ("MED009", "Omeprazole 20mg", "PPI for GERD and ulcers", 22.00, 300, 15),
        ("MED010", "Amoxicillin 500mg", "Broad-spectrum antibiotic", 48.00, 220, 15),
        ("MED011", "Vitamin D3 1000IU", "Vitamin D supplement", 16.00, 500, 25),
    ],
    "DIS004": [  # Distributor D — for the scalability demo (add at runtime)
        ("MED001", "Dolo 650", "Paracetamol 650mg tablet for fever and pain relief", 23.50, 1000, 30),
        ("MED002", "Crocin Advance", "Paracetamol 500mg fast-acting tablet", 16.50, 500, 20),
        ("MED012", "Montelukast 10mg", "Leukotriene antagonist for asthma/allergy", 90.00, 150, 15),
    ],
}

# Orders this distributor has accepted (its own local audit trail — mirrors
# the order lifecycle whose cross-node coordination lives in Main API).
ORDERS_TABLE_SQL = """
    CREATE TABLE IF NOT EXISTS accepted_orders (
        order_id    TEXT PRIMARY KEY,
        medicine_id TEXT NOT NULL,
        quantity    INTEGER NOT NULL,
        pharmacy_id TEXT NOT NULL,
        accepted_at TEXT DEFAULT (datetime('now'))
    )
"""


async def init_db() -> None:
    os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("PRAGMA journal_mode=WAL")
        await db.execute("PRAGMA foreign_keys=ON")

        await db.execute("""
            CREATE TABLE IF NOT EXISTS medicines (
                medicine_id         TEXT PRIMARY KEY,
                name                TEXT NOT NULL,
                description         TEXT DEFAULT '',
                price               REAL NOT NULL,
                quantity            INTEGER NOT NULL DEFAULT 0,
                low_stock_threshold INTEGER NOT NULL DEFAULT 5,
                created_at          TEXT DEFAULT (datetime('now'))
            )
        """)

        await db.execute(ORDERS_TABLE_SQL)

        cursor = await db.execute("SELECT COUNT(*) FROM medicines")
        row = await cursor.fetchone()
        if row[0] == 0:
            seed = SEED_DATA.get(DISTRIBUTOR_ID, [])
            for med_id, name, desc, price, qty, threshold in seed:
                await db.execute(
                    "INSERT OR IGNORE INTO medicines VALUES (?,?,?,?,?,?,datetime('now'))",
                    (med_id, name, desc, price, qty, threshold),
                )
            logger.info(f"Seeded {len(seed)} medicines for {DISTRIBUTOR_ID}")

        await db.commit()
    logger.info(f"{DISTRIBUTOR_ID}: DB initialised at {DB_PATH}")


def get_db_path() -> str:
    return DB_PATH
