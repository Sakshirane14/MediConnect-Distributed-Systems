from __future__ import annotations
import asyncio
import logging
import os
import sys
import time
import grpc

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "generated"))
from .generated import pharmacy_pb2, pharmacy_pb2_grpc

from .db import init_db, DISTRIBUTOR_ID, DISTRIBUTOR_NAME
from . import inventory
from .clock import LamportClock

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

GRPC_PORT = int(os.environ.get("GRPC_PORT", "50051"))
GRPC_ADDRESS = os.environ.get("GRPC_ADDRESS", f"localhost:{GRPC_PORT}")  # how OTHER services reach me
MAIN_API_URL = os.environ.get("MAIN_API_URL", "http://main-api:8000")

# FA-1 Unit 3 — this node's own Lamport logical clock (see clock.py).
lamport_clock = LamportClock()


class DistributorServiceServicer(pharmacy_pb2_grpc.DistributorServiceServicer):
    async def SearchMedicine(self, request, context):
        lamport_clock.tick()  # local event — Unit 3 Logical Clocks
        logger.info(f"{DISTRIBUTOR_ID}: SearchMedicine query='{request.query}' (clock={lamport_clock.value})")
        results = await inventory.search_medicines(request.query)
        grpc_results = [
            pharmacy_pb2.MedicineResult(
                distributor_id=r["distributor_id"],
                medicine_id=r["medicine_id"],
                name=r["name"],
                available=r["available"],
                quantity=r["quantity"],
                price=r["price"],
                status=r["status"],
            )
            for r in results
        ]
        return pharmacy_pb2.SearchMedicineResponse(results=grpc_results)

    async def GetMedicineDetails(self, request, context):
        lamport_clock.tick()
        logger.info(f"{DISTRIBUTOR_ID}: GetMedicineDetails id='{request.medicine_id}'")
        details = await inventory.get_medicine_details(request.medicine_id)
        if not details:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(f"Medicine {request.medicine_id} not found in {DISTRIBUTOR_ID}")
            return pharmacy_pb2.MedicineDetails()
        return pharmacy_pb2.MedicineDetails(
            medicine_id=details["medicine_id"],
            name=details["name"],
            description=details["description"],
            price=details["price"],
            quantity=details["quantity"],
            available=details["available"],
            distributor_id=details["distributor_id"],
            low_stock_threshold=details["low_stock_threshold"],
        )

    async def PlaceOrder(self, request, context):
        # FA-1 Unit 3: apply Lamport's receive rule using Main API's
        # piggybacked clock value, THEN enter the mutual-exclusion-guarded
        # critical section (inventory.place_order's atomic UPDATE). The
        # clock value returned below is this event's exact position in the
        # causal order — i.e. "stock was decremented at logical time T",
        # which is the whole point of attaching a clock to it rather than
        # just a wall-clock timestamp that could disagree across machines.
        my_clock = lamport_clock.update(request.sender_lamport_clock)
        logger.info(
            f"{DISTRIBUTOR_ID}: PlaceOrder med={request.medicine_id} order={request.order_id} "
            f"qty={request.quantity} pharmacy={request.pharmacy_id} (clock={my_clock}, "
            f"ENTERING critical section)"
        )
        result = await inventory.place_order(
            request.order_id, request.medicine_id, request.quantity, request.pharmacy_id
        )
        logger.info(f"{DISTRIBUTOR_ID}: PlaceOrder EXITED critical section - success={result['success']}")
        return pharmacy_pb2.PlaceOrderResponse(
            success=result["success"],
            new_quantity=result["new_quantity"],
            message=result["message"],
            lamport_clock=my_clock,
        )

    async def UpdateStock(self, request, context):
        lamport_clock.tick()
        logger.info(
            f"{DISTRIBUTOR_ID}: UpdateStock med={request.medicine_id} qty={request.new_quantity} staff={request.staff_id}"
        )
        result = await inventory.update_stock(
            request.medicine_id, request.new_quantity, request.staff_id
        )
        return pharmacy_pb2.UpdateStockResponse(
            success=result["success"],
            message=result["message"],
        )

    async def HealthCheck(self, request, context):
        # FA-1 Unit 3: Logical Clocks (receive rule) + Clock Synchronization
        # (Cristian's Algorithm). server_unix_time is this node's physical
        # wall-clock reading at the instant of reply; Main API pairs it with
        # the measured round-trip time to estimate this node's clock offset
        # WITHOUT the two machines needing any shared time source.
        my_clock = lamport_clock.update(request.sender_lamport_clock)
        return pharmacy_pb2.HealthCheckResponse(
            status="healthy",
            distributor_id=DISTRIBUTOR_ID,
            distributor_name=DISTRIBUTOR_NAME,
            lamport_clock=my_clock,
            server_unix_time=time.time(),
        )


async def register_with_main_api() -> None:
    """
    FA-1 §9 Service Registration / Discovery — lightweight, college-scale.
    On startup (and periodically, in case Main API restarted after me),
    this distributor announces itself to Main API's in-memory registry.
    Main API's search/health logic iterates over whatever is currently
    registered; it has zero distributor-specific code. This is what makes
    "add Distributor D at runtime" work without touching Main API.
    """
    import httpx
    payload = {
        "distributor_id": DISTRIBUTOR_ID,
        "name": DISTRIBUTOR_NAME,
        "grpc_address": GRPC_ADDRESS,
    }
    while True:
        try:
            async with httpx.AsyncClient(timeout=3.0) as client:
                resp = await client.post(f"{MAIN_API_URL}/internal/distributors/register", json=payload)
                if resp.status_code == 200:
                    logger.info(f"{DISTRIBUTOR_ID}: registered with Main API at {MAIN_API_URL}")
                else:
                    logger.warning(f"{DISTRIBUTOR_ID}: registration returned {resp.status_code}")
        except Exception as e:
            logger.warning(f"{DISTRIBUTOR_ID}: could not reach Main API to register yet ({e}) - retrying in 5s")
        await asyncio.sleep(20)  # keep re-announcing so a Main API restart re-discovers me


async def serve():
    await init_db()
    server = grpc.aio.server()
    pharmacy_pb2_grpc.add_DistributorServiceServicer_to_server(
        DistributorServiceServicer(), server
    )
    listen_addr = f"0.0.0.0:{GRPC_PORT}"
    server.add_insecure_port(listen_addr)
    logger.info(f"{DISTRIBUTOR_ID} ({DISTRIBUTOR_NAME}): gRPC server listening on {listen_addr}, advertised as {GRPC_ADDRESS}")
    await server.start()

    asyncio.create_task(register_with_main_api())

    await server.wait_for_termination()


if __name__ == "__main__":
    asyncio.run(serve())
