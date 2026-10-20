from __future__ import annotations
"""FA-1 Unit 4 — Distributed File System status: which storage regions
exist, which are online, and how many files each one holds."""
from fastapi import APIRouter, Depends
from ..auth.auth import get_current_user
from ..dfs import storage as dfs
from ..db import get_all_replicas

router = APIRouter(prefix="/api/storage", tags=["storage"])


@router.get("/status")
async def storage_status(current_user: dict = Depends(get_current_user)):
    regions = await dfs.storage_health()
    replicas = await get_all_replicas()
    online = sum(1 for r in regions if r["status"] == "online")
    return {
        "regions": regions,
        "online": online,
        "total_regions": len(regions),
        "files_tracked": len(replicas),
        # one row per file: which regions hold a copy
        "files": [{"filename": fn, "regions": regs} for fn, regs in replicas.items()],
    }
