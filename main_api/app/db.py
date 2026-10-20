from __future__ import annotations
import os
import logging
import aiosqlite

logger = logging.getLogger(__name__)
DB_PATH = os.environ.get("MAIN_DB_PATH", os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "data/main_api.db"))

# Since pharmacies are now CLIENTS of the distributed distributor network
# (not independent gRPC nodes themselves — FA-1 §45), each pharmacy's local
# inventory is a simple central table in Main API's own DB. The
# DISTRIBUTORS remain the real distributed nodes with independent DBs.
PHARMACY_SEED = {
    "PHA001": [
        ("MED001", "Dolo 650", 4, 5),
        ("MED002", "Crocin Advance", 20, 5),
        ("MED006", "Ibuprofen 400mg", 0, 3),
    ],
    "PHB002": [
        ("MED001", "Dolo 650", 30, 5),
        ("MED003", "Azithromycin 500mg", 2, 3),
        ("MED009", "Omeprazole 20mg", 0, 5),
    ],
    "PHC003": [
        ("MED001", "Dolo 650", 15, 5),
        ("MED005", "Cetirizine 10mg", 1, 5),
        ("MED011", "Vitamin D3 1000IU", 25, 10),
    ],
}


async def init_db() -> None:
    os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("PRAGMA journal_mode=WAL")
        await db.execute("PRAGMA foreign_keys=ON")

        # role: 'pharmacy_staff' | 'distributor_staff'
        await db.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id       TEXT PRIMARY KEY,
                name          TEXT NOT NULL,
                email         TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                role          TEXT NOT NULL DEFAULT 'pharmacy_staff',
                pharmacy_id    TEXT,
                distributor_id TEXT,
                created_at    TEXT DEFAULT (datetime('now'))
            )
        """)

        # Pharmacy's own local inventory — central (pharmacies are clients,
        # not distributed nodes, in this version of the architecture).
        await db.execute("""
            CREATE TABLE IF NOT EXISTS pharmacy_inventory (
                pharmacy_id         TEXT NOT NULL,
                medicine_id         TEXT NOT NULL,
                medicine_name       TEXT NOT NULL,
                quantity            INTEGER NOT NULL DEFAULT 0,
                low_stock_threshold INTEGER NOT NULL DEFAULT 5,
                PRIMARY KEY (pharmacy_id, medicine_id)
            )
        """)

        # Procurement order lifecycle: requested -> approved/rejected -> dispatched -> received
        # Cross-node coordination record: which pharmacy ordered from which
        # distributor. The atomic stock deduction itself happens AT the
        # distributor node (via gRPC PlaceOrder), not here.
        await db.execute("""
            CREATE TABLE IF NOT EXISTS procurement_orders (
                order_id        TEXT PRIMARY KEY,
                pharmacy_id     TEXT NOT NULL,
                distributor_id  TEXT NOT NULL,
                medicine_id     TEXT NOT NULL,
                medicine_name   TEXT NOT NULL,
                quantity        INTEGER NOT NULL DEFAULT 1,
                unit_price      REAL NOT NULL DEFAULT 0,
                status          TEXT NOT NULL DEFAULT 'requested',
                requested_by    TEXT,
                request_timestamp    TEXT DEFAULT (datetime('now')),
                approved_timestamp   TEXT,
                dispatched_timestamp TEXT,
                received_timestamp   TEXT
            )
        """)

        await db.execute("""
            CREATE TABLE IF NOT EXISTS notifications (
                notification_id TEXT PRIMARY KEY,
                recipient_type  TEXT NOT NULL,
                recipient_id    TEXT NOT NULL,
                message         TEXT NOT NULL,
                read_flag       INTEGER NOT NULL DEFAULT 0,
                created_at      TEXT DEFAULT (datetime('now'))
            )
        """)

        # Purchase Order / Invoice / Delivery Note attachments (FA-1 §18
        # Distributed Multimedia).
        await db.execute("""
            CREATE TABLE IF NOT EXISTS business_documents (
                document_id   TEXT PRIMARY KEY,
                order_id      TEXT,
                doc_type      TEXT NOT NULL DEFAULT 'purchase_order',
                uploaded_by   TEXT NOT NULL,
                filename      TEXT NOT NULL,
                size_kb       REAL NOT NULL,
                created_at    TEXT DEFAULT (datetime('now'))
            )
        """)

        # Medicine Requirement List: a pharmacy bundles several needed
        # medicines into one requirement and broadcasts it to every
        # registered distributor node, which each independently evaluate
        # and respond using their OWN inventory (FA-1 §2 new requirements).
        await db.execute("""
            CREATE TABLE IF NOT EXISTS requirements (
                requirement_id TEXT PRIMARY KEY,
                pharmacy_id    TEXT NOT NULL,
                status         TEXT NOT NULL DEFAULT 'open',
                attachment_document_id TEXT,
                requested_by   TEXT,
                fulfilled_by_distributor_id TEXT,
                created_at     TEXT DEFAULT (datetime('now'))
            )
        """)
        # Migration guard: older DB files created before this column existed.
        try:
            await db.execute("ALTER TABLE requirements ADD COLUMN fulfilled_by_distributor_id TEXT")
        except Exception:
            pass  # column already exists

        await db.execute("""
            CREATE TABLE IF NOT EXISTS requirement_items (
                requirement_id TEXT NOT NULL,
                medicine_id    TEXT NOT NULL,
                medicine_name  TEXT NOT NULL,
                requested_quantity INTEGER NOT NULL,
                priority       TEXT NOT NULL DEFAULT 'Normal',
                PRIMARY KEY (requirement_id, medicine_id)
            )
        """)

        # One response per distributor per requirement — each independently
        # computed from that distributor's own live inventory (via gRPC).
        await db.execute("""
            CREATE TABLE IF NOT EXISTS distributor_responses (
                requirement_id   TEXT NOT NULL,
                distributor_id   TEXT NOT NULL,
                can_fulfil_all   INTEGER NOT NULL DEFAULT 0,
                total_price      REAL NOT NULL DEFAULT 0,
                quotation_document_id TEXT,
                submitted_at     TEXT DEFAULT (datetime('now')),
                PRIMARY KEY (requirement_id, distributor_id)
            )
        """)

        await db.execute("""
            CREATE TABLE IF NOT EXISTS distributor_response_items (
                requirement_id     TEXT NOT NULL,
                distributor_id     TEXT NOT NULL,
                medicine_id        TEXT NOT NULL,
                available_quantity INTEGER NOT NULL DEFAULT 0,
                price              REAL NOT NULL DEFAULT 0,
                can_fulfil         INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (requirement_id, distributor_id, medicine_id)
            )
        """)

        # Image Requirement — a pharmacist uploads a PHOTO of a needs list
        # (handwritten note, printed list, whatever) instead of typing
        # structured items. FA-1 §8 Distributed Multimedia Systems: an
        # image is captured, stored, and transferred across the network to
        # every distributor node's dashboard. It's also a genuine
        # distributed-systems RACE: every currently-registered distributor
        # sees the same open image requirement at the same time, and only
        # the FIRST one to successfully claim it gets the order — enforced
        # with the same atomic-UPDATE pattern used for stock reservation
        # (see claim_image_requirement below), not a lock the app holds in
        # memory.
        await db.execute("""
            CREATE TABLE IF NOT EXISTS image_requirements (
                image_requirement_id      TEXT PRIMARY KEY,
                pharmacy_id                TEXT NOT NULL,
                requested_by                TEXT,
                image_filename              TEXT NOT NULL,
                note                        TEXT DEFAULT '',
                status                      TEXT NOT NULL DEFAULT 'open',
                claimed_by_distributor_id  TEXT,
                order_id                    TEXT,
                claimed_at                  TEXT,
                created_at                  TEXT DEFAULT (datetime('now'))
            )
        """)

        # FA-1 Unit 4 — Distributed File System. One row per (file, region)
        # copy: the "NameNode" bookkeeping that says which storage regions
        # hold which uploaded photo.
        await db.execute("""
            CREATE TABLE IF NOT EXISTS image_replicas (
                filename   TEXT NOT NULL,
                region     TEXT NOT NULL,
                stored_at  TEXT DEFAULT (datetime('now')),
                PRIMARY KEY (filename, region)
            )
        """)

        for pharmacy_id, items in PHARMACY_SEED.items():
            for med_id, name, qty, threshold in items:
                await db.execute(
                    "INSERT OR IGNORE INTO pharmacy_inventory VALUES (?,?,?,?,?)",
                    (pharmacy_id, med_id, name, qty, threshold),
                )

        await db.commit()
    logger.info("Main API DB initialised")


# ── Users / Auth ───────────────────────────────────────────────────────────
async def create_user(user_id: str, name: str, email: str,
                      password_hash: str, role: str = "pharmacy_staff",
                      pharmacy_id: str | None = None, distributor_id: str | None = None) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        try:
            await db.execute(
                "INSERT INTO users VALUES (?,?,?,?,?,?,?,datetime('now'))",
                (user_id, name, email, password_hash, role, pharmacy_id, distributor_id),
            )
            await db.commit()
            return True
        except Exception:
            return False

async def get_user_by_email(email: str) -> dict | None:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("SELECT * FROM users WHERE email = ?", (email,))
        row = await cursor.fetchone()
        return dict(row) if row else None

async def get_user_by_id(user_id: str) -> dict | None:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
        row = await cursor.fetchone()
        return dict(row) if row else None


# ── Pharmacy local inventory ────────────────────────────────────────────
async def get_pharmacy_inventory(pharmacy_id: str) -> list[dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT * FROM pharmacy_inventory WHERE pharmacy_id = ? ORDER BY medicine_name", (pharmacy_id,)
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]

async def get_pharmacy_stock_item(pharmacy_id: str, medicine_id: str) -> dict | None:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT * FROM pharmacy_inventory WHERE pharmacy_id = ? AND medicine_id = ?",
            (pharmacy_id, medicine_id),
        )
        row = await cursor.fetchone()
        return dict(row) if row else None

async def upsert_pharmacy_stock(pharmacy_id: str, medicine_id: str, medicine_name: str,
                                quantity_delta: int, low_stock_threshold: int = 5) -> int:
    """Add (or subtract, with a negative delta) quantity_delta to a pharmacy's
    local stock of a medicine, creating the row if it doesn't exist yet.
    Returns the resulting quantity."""
    async with aiosqlite.connect(DB_PATH) as db:
        existing = await (await db.execute(
            "SELECT quantity FROM pharmacy_inventory WHERE pharmacy_id = ? AND medicine_id = ?",
            (pharmacy_id, medicine_id),
        )).fetchone()
        if existing is None:
            new_qty = max(0, quantity_delta)
            await db.execute(
                "INSERT INTO pharmacy_inventory VALUES (?,?,?,?,?)",
                (pharmacy_id, medicine_id, medicine_name, new_qty, low_stock_threshold),
            )
        else:
            new_qty = max(0, existing[0] + quantity_delta)
            await db.execute(
                "UPDATE pharmacy_inventory SET quantity = ? WHERE pharmacy_id = ? AND medicine_id = ?",
                (new_qty, pharmacy_id, medicine_id),
            )
        await db.commit()
        return new_qty

async def set_pharmacy_stock(pharmacy_id: str, medicine_id: str, new_quantity: int) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "UPDATE pharmacy_inventory SET quantity = ? WHERE pharmacy_id = ? AND medicine_id = ?",
            (new_quantity, pharmacy_id, medicine_id),
        )
        await db.commit()
        return cursor.rowcount > 0


# ── Procurement Orders (pharmacy -> distributor) ────────────────────────
async def create_order(order_id: str, pharmacy_id: str, distributor_id: str, medicine_id: str,
                       medicine_name: str, quantity: int, unit_price: float, requested_by: str) -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT OR IGNORE INTO procurement_orders "
            "(order_id, pharmacy_id, distributor_id, medicine_id, medicine_name, quantity, unit_price, status, requested_by, request_timestamp) "
            "VALUES (?,?,?,?,?,?,?,'requested',?,datetime('now'))",
            (order_id, pharmacy_id, distributor_id, medicine_id, medicine_name, quantity, unit_price, requested_by),
        )
        await db.commit()

async def get_order_by_id(order_id: str) -> dict | None:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("SELECT * FROM procurement_orders WHERE order_id = ?", (order_id,))
        row = await cursor.fetchone()
        return dict(row) if row else None

async def get_orders_for_pharmacy(pharmacy_id: str) -> list[dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT * FROM procurement_orders WHERE pharmacy_id = ? ORDER BY request_timestamp DESC", (pharmacy_id,)
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]

async def get_orders_for_distributor(distributor_id: str, status: str | None = None) -> list[dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        if status:
            cursor = await db.execute(
                "SELECT * FROM procurement_orders WHERE distributor_id = ? AND status = ? ORDER BY request_timestamp DESC",
                (distributor_id, status),
            )
        else:
            cursor = await db.execute(
                "SELECT * FROM procurement_orders WHERE distributor_id = ? ORDER BY request_timestamp DESC",
                (distributor_id,),
            )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]

async def get_all_orders(status: str | None = None) -> list[dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        if status:
            cursor = await db.execute(
                "SELECT * FROM procurement_orders WHERE status = ? ORDER BY request_timestamp DESC", (status,)
            )
        else:
            cursor = await db.execute("SELECT * FROM procurement_orders ORDER BY request_timestamp DESC")
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]

async def update_order_status(order_id: str, status: str, timestamp_column: str | None = None) -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        if timestamp_column:
            await db.execute(
                f"UPDATE procurement_orders SET status = ?, {timestamp_column} = datetime('now') WHERE order_id = ?",
                (status, order_id),
            )
        else:
            await db.execute("UPDATE procurement_orders SET status = ? WHERE order_id = ?", (status, order_id))
        await db.commit()

async def count_orders_dispatched_today() -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT COUNT(*) FROM procurement_orders "
            "WHERE status IN ('dispatched','received') AND date(dispatched_timestamp) = date('now')"
        )
        row = await cursor.fetchone()
        return row[0] if row else 0


# ── Notifications ────────────────────────────────────────────────────────
async def save_notification(notification_id: str, recipient_type: str,
                            recipient_id: str, message: str) -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT OR IGNORE INTO notifications VALUES (?,?,?,?,0,datetime('now'))",
            (notification_id, recipient_type, recipient_id, message),
        )
        await db.commit()

async def get_notifications(recipient_id: str) -> list[dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT * FROM notifications WHERE recipient_id = ? ORDER BY created_at DESC LIMIT 50",
            (recipient_id,),
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]

async def mark_all_notifications_read(recipient_id: str) -> int:
    """Mark all unread notifications for one recipient as read."""
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "UPDATE notifications SET read_flag = 1 WHERE recipient_id = ? AND read_flag = 0",
            (recipient_id,),
        )
        await db.commit()
        return cursor.rowcount if cursor.rowcount is not None else 0


# ── Business documents ───────────────────────────────────────────────────
async def save_business_document(document_id: str, order_id: str | None, doc_type: str,
                                 uploaded_by: str, filename: str, size_kb: float) -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT OR IGNORE INTO business_documents VALUES (?,?,?,?,?,?,datetime('now'))",
            (document_id, order_id, doc_type, uploaded_by, filename, size_kb),
        )
        await db.commit()

async def get_all_business_documents() -> list[dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("SELECT * FROM business_documents ORDER BY created_at DESC LIMIT 100")
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]


# ── Medicine Requirement List ────────────────────────────────────────────
async def create_requirement(requirement_id: str, pharmacy_id: str, requested_by: str,
                             items: list[dict], attachment_document_id: str | None = None) -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT OR IGNORE INTO requirements "
            "(requirement_id, pharmacy_id, status, attachment_document_id, requested_by, fulfilled_by_distributor_id, created_at) "
            "VALUES (?,?,?,?,?,NULL,datetime('now'))",
            (requirement_id, pharmacy_id, "open", attachment_document_id, requested_by),
        )
        for item in items:
            await db.execute(
                "INSERT OR IGNORE INTO requirement_items VALUES (?,?,?,?,?)",
                (requirement_id, item["medicine_id"], item["medicine_name"],
                 item["requested_quantity"], item.get("priority", "Normal")),
            )
        await db.commit()

async def get_requirement(requirement_id: str) -> dict | None:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("SELECT * FROM requirements WHERE requirement_id = ?", (requirement_id,))
        row = await cursor.fetchone()
        return dict(row) if row else None

async def get_requirement_items(requirement_id: str) -> list[dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("SELECT * FROM requirement_items WHERE requirement_id = ?", (requirement_id,))
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]

async def get_requirements_for_pharmacy(pharmacy_id: str) -> list[dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT * FROM requirements WHERE pharmacy_id = ? ORDER BY created_at DESC", (pharmacy_id,)
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]

async def get_all_open_requirements() -> list[dict]:
    """Every registered distributor node is informed of every OPEN
    requirement (FA-1 §requirement flow) — visibility isn't filtered per
    distributor; independence comes from each one computing its OWN
    response from its OWN inventory. Filtered to status='open' so a
    requirement stops appearing in every distributor's queue the moment
    the pharmacy accepts one of the responses (see accept_requirement in
    routers/requirements.py) — otherwise it would sit there forever even
    after being fulfilled."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("SELECT * FROM requirements WHERE status = 'open' ORDER BY created_at DESC")
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]

async def update_requirement_status(requirement_id: str, status: str, fulfilled_by_distributor_id: str | None = None) -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE requirements SET status = ?, fulfilled_by_distributor_id = ? WHERE requirement_id = ?",
            (status, fulfilled_by_distributor_id, requirement_id),
        )
        await db.commit()

async def get_response_items(requirement_id: str, distributor_id: str) -> list[dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT * FROM distributor_response_items WHERE requirement_id = ? AND distributor_id = ?",
            (requirement_id, distributor_id),
        )
        return [dict(r) for r in await cursor.fetchall()]

async def save_distributor_response(requirement_id: str, distributor_id: str, can_fulfil_all: bool,
                                    total_price: float, items: list[dict],
                                    quotation_document_id: str | None = None) -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT OR REPLACE INTO distributor_responses VALUES (?,?,?,?,?,datetime('now'))",
            (requirement_id, distributor_id, int(can_fulfil_all), total_price, quotation_document_id),
        )
        await db.execute(
            "DELETE FROM distributor_response_items WHERE requirement_id = ? AND distributor_id = ?",
            (requirement_id, distributor_id),
        )
        for item in items:
            await db.execute(
                "INSERT INTO distributor_response_items VALUES (?,?,?,?,?,?)",
                (requirement_id, distributor_id, item["medicine_id"],
                 item["available_quantity"], item["price"], int(item["can_fulfil"])),
            )
        await db.commit()

async def get_responses_for_requirement(requirement_id: str) -> list[dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT * FROM distributor_responses WHERE requirement_id = ? ORDER BY submitted_at ASC", (requirement_id,)
        )
        responses = [dict(r) for r in await cursor.fetchall()]
        for resp in responses:
            item_cursor = await db.execute(
                "SELECT * FROM distributor_response_items WHERE requirement_id = ? AND distributor_id = ?",
                (requirement_id, resp["distributor_id"]),
            )
            resp["items"] = [dict(r) for r in await item_cursor.fetchall()]
        return responses

async def has_distributor_responded(requirement_id: str, distributor_id: str) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT 1 FROM distributor_responses WHERE requirement_id = ? AND distributor_id = ?",
            (requirement_id, distributor_id),
        )
        return (await cursor.fetchone()) is not None


# ── Image Requirement (multimedia, first-to-claim-wins) ─────────────────
async def create_image_requirement(image_requirement_id: str, pharmacy_id: str, requested_by: str,
                                    image_filename: str, note: str = "") -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT OR IGNORE INTO image_requirements "
            "(image_requirement_id, pharmacy_id, requested_by, image_filename, note, status, created_at) "
            "VALUES (?,?,?,?,?,'open',datetime('now'))",
            (image_requirement_id, pharmacy_id, requested_by, image_filename, note),
        )
        await db.commit()

async def get_image_requirement(image_requirement_id: str) -> dict | None:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT * FROM image_requirements WHERE image_requirement_id = ?", (image_requirement_id,)
        )
        row = await cursor.fetchone()
        return dict(row) if row else None

async def get_image_requirements_for_pharmacy(pharmacy_id: str) -> list[dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT * FROM image_requirements WHERE pharmacy_id = ? ORDER BY created_at DESC", (pharmacy_id,)
        )
        return [dict(r) for r in await cursor.fetchall()]

async def get_image_requirements_for_distributor(distributor_id: str) -> list[dict]:
    """Every OPEN image requirement (broadcast to every distributor,
    regardless of who it's from) PLUS anything this specific distributor
    has already claimed (so it doesn't vanish from their own view once
    they've won it)."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT * FROM image_requirements WHERE status = 'open' OR claimed_by_distributor_id = ? "
            "ORDER BY created_at DESC",
            (distributor_id,),
        )
        return [dict(r) for r in await cursor.fetchall()]

async def claim_image_requirement(image_requirement_id: str, distributor_id: str, order_id: str) -> bool:
    """THE race-condition-safe step. A single atomic UPDATE guarded by
    `WHERE status = 'open'` — SQLite serializes writers, so if two
    distributors call this at almost the same instant, exactly one
    UPDATE's rowcount comes back 1 and the other comes back 0. There is no
    separate 'check then set' — checking and setting happen in the same
    statement, which is what actually prevents the race (a check-then-set
    done as two separate statements would NOT be safe)."""
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "UPDATE image_requirements "
            "SET status = 'claimed', claimed_by_distributor_id = ?, order_id = ?, claimed_at = datetime('now') "
            "WHERE image_requirement_id = ? AND status = 'open'",
            (distributor_id, order_id, image_requirement_id),
        )
        await db.commit()
        return cursor.rowcount > 0


# ── Distributed File System: which regions hold which file ─────────────
async def record_replicas(filename: str, regions: list[str]) -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        for region in regions:
            await db.execute(
                "INSERT OR IGNORE INTO image_replicas (filename, region) VALUES (?,?)",
                (filename, region),
            )
        await db.commit()

async def get_replica_regions(filename: str) -> list[str]:
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT region FROM image_replicas WHERE filename = ? ORDER BY stored_at, region", (filename,)
        )
        return [r[0] for r in await cursor.fetchall()]

async def get_all_replicas() -> dict[str, list[str]]:
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT filename, region FROM image_replicas ORDER BY stored_at, region")
        out: dict[str, list[str]] = {}
        for filename, region in await cursor.fetchall():
            out.setdefault(filename, []).append(region)
        return out
