from __future__ import annotations
import logging
from fastapi import APIRouter, Query, HTTPException
from ..grpc_clients.distributor_client import (
    search_all_distributors,
    get_medicine_from_distributor,
    health_check_distributor,
)
from ..grpc_clients.registry import get_registry

router = APIRouter(prefix="/api", tags=["medicines"])
logger = logging.getLogger(__name__)

@router.get("/medicines/search")
async def search_medicines(query: str = Query(..., min_length=1)):
    """Find Suppliers: fan out to every currently-registered distributor
    node concurrently and aggregate the results (FA-1 §14)."""
    logger.info(f"Search request: query='{query}'")
    results = await search_all_distributors(query)
    return {"query": query, "results": results, "total": len(results)}

@router.get("/medicines/{medicine_id}")
async def get_medicine_details(medicine_id: str, distributor_id: str = Query(...)):
    details = await get_medicine_from_distributor(distributor_id, medicine_id)
    if not details:
        raise HTTPException(status_code=404, detail="Medicine not found.")
    return details

@router.get("/distributors/health")
async def distributors_health():
    results = {}
    for distributor_id in get_registry():
        results[distributor_id] = await health_check_distributor(distributor_id)
    return results
