from __future__ import annotations
import os
import asyncio
import logging
import aiosqlite
from .db import get_db_path, DISTRIBUTOR_ID

logger = logging.getLogger(__name__)

# FA-1 §16 Communication Delay demo: set SIMULATE_DELAY_MS on a distributor
# container to make it respond slowly (or set to a huge value + a short
# Main API gRPC timeout to demonstrate a TIMEOUT). Zero/unset = no delay.
SIMULATE_DELAY_MS = int(os.environ.get("SIMULATE_DELAY_MS", "0"))


def _status(qty: int, threshold: int) -> str:
    if qty <= 0:
        return "out_of_stock"
    if qty <= threshold:
        return "low_stock"
    return "available"


async def _maybe_delay() -> None:
    if SIMULATE_DELAY_MS > 0:
        logger.info(f"{DISTRIBUTOR_ID}: simulating {SIMULATE_DELAY_MS}ms network delay")
        await asyncio.sleep(SIMULATE_DELAY_MS / 1000)


async def search_medicines(query: str) -> list[dict]:
    await _maybe_delay()
    pattern = f"%{query}%"
    results = []
    async with aiosqlite.connect(get_db_path()) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT * FROM medicines WHERE name LIKE ? OR description LIKE ?",
            (pattern, pattern),
        )
        rows = await cursor.fetchall()
        for row in rows:
            results.append({
                "distributor_id": DISTRIBUTOR_ID,
                "medicine_id":    row["medicine_id"],
                "name":           row["name"],
                "available":      row["quantity"] > 0,
                "quantity":       row["quantity"],
                "price":          row["price"],
                "status":         _status(row["quantity"], row["low_stock_threshold"]),
            })
    return results


async def get_medicine_details(medicine_id: str) -> dict | None:
    await _maybe_delay()
    async with aiosqlite.connect(get_db_path()) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT * FROM medicines WHERE medicine_id = ?", (medicine_id,)
        )
        row = await cursor.fetchone()
        if not row:
            return None
        return {
            "medicine_id":         row["medicine_id"],
            "name":                row["name"],
            "description":         row["description"],
            "price":               row["price"],
            "quantity":            row["quantity"],
            "available":           row["quantity"] > 0,
            "distributor_id":      DISTRIBUTOR_ID,
            "low_stock_threshold": row["low_stock_threshold"],
        }


async def place_order(order_id: str, medicine_id: str, quantity: int, pharmacy_id: str) -> dict:
    """
    Atomically accept a pharmacy's order against THIS distributor's own
    stock. This is the FA-1 concurrency-safety checkpoint (§17): the single
    `UPDATE ... WHERE quantity >= ?` is atomic within SQLite, so two
    pharmacies ordering from the same distributor at the same moment can
    never together take more stock than physically exists here.

    Idempotent per order_id — a retried call after a flaky response does
    not double-deduct stock.
    """
    async with aiosqlite.connect(get_db_path()) as db:
        await db.execute("PRAGMA journal_mode=WAL")

        existing = await (await db.execute(
            "SELECT quantity FROM medicines WHERE medicine_id = ?", (medicine_id,)
        )).fetchone()

        already = await (await db.execute(
            "SELECT 1 FROM accepted_orders WHERE order_id = ?", (order_id,)
        )).fetchone()
        if already:
            current_qty = existing[0] if existing else 0
            logger.info(f"{DISTRIBUTOR_ID}: order {order_id} already accepted - skipping duplicate")
            return {"success": True, "new_quantity": current_qty, "message": "Order already accepted."}

        cursor = await db.execute(
            "UPDATE medicines SET quantity = quantity - ? WHERE medicine_id = ? AND quantity >= ?",
            (quantity, medicine_id, quantity),
        )
        if cursor.rowcount == 0:
            await db.rollback()
            available = existing[0] if existing else 0
            logger.warning(f"{DISTRIBUTOR_ID}: place_order failed - {medicine_id} insufficient stock (have {available}, need {quantity})")
            return {"success": False, "new_quantity": available, "message": f"Insufficient stock: requested {quantity}, available {available}."}

        await db.execute(
            "INSERT INTO accepted_orders VALUES (?,?,?,?,datetime('now'))",
            (order_id, medicine_id, quantity, pharmacy_id),
        )
        row = await (await db.execute(
            "SELECT quantity FROM medicines WHERE medicine_id = ?", (medicine_id,)
        )).fetchone()
        new_qty = row[0]
        await db.commit()

    logger.info(f"{DISTRIBUTOR_ID}: accepted order {order_id} for {pharmacy_id} - {quantity}x {medicine_id} -> remaining={new_qty}")
    return {"success": True, "new_quantity": new_qty, "message": "Order accepted and stock reserved."}


async def update_stock(medicine_id: str, new_quantity: int, staff_id: str) -> dict:
    async with aiosqlite.connect(get_db_path()) as db:
        cursor = await db.execute(
            "UPDATE medicines SET quantity = ? WHERE medicine_id = ?",
            (new_quantity, medicine_id),
        )
        if cursor.rowcount == 0:
            await db.rollback()
            return {"success": False, "message": f"Medicine {medicine_id} not found."}
        await db.commit()
    logger.info(f"{DISTRIBUTOR_ID}: stock updated {medicine_id} -> qty={new_quantity} by {staff_id}")
    return {"success": True, "message": "Stock updated."}


async def list_all_medicines() -> list[dict]:
    results = []
    async with aiosqlite.connect(get_db_path()) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("SELECT * FROM medicines ORDER BY name")
        rows = await cursor.fetchall()
        for row in rows:
            results.append({
                "medicine_id":         row["medicine_id"],
                "name":                row["name"],
                "description":         row["description"],
                "price":               row["price"],
                "quantity":            row["quantity"],
                "available":           row["quantity"] > 0,
                "status":              _status(row["quantity"], row["low_stock_threshold"]),
                "low_stock_threshold": row["low_stock_threshold"],
            })
    return results
