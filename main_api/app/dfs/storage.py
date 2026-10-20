from __future__ import annotations
"""
FA-1 Unit 4 — Distributed File System (the "NameNode" side).

Uploaded requirement photos are no longer kept on Main API's own disk.
Instead each photo is COPIED to every configured storage region (each an
independent storage_service process with its own folder), and Main API
records WHICH regions hold WHICH file in the image_replicas table. When
someone opens a photo, Main API reads it from the first region that is
alive — so losing a region does not lose the photo (replication =
fault tolerance for data, the same idea as HDFS).

Honest simplifications: files are small photos so there is no chunking;
"regions" are just labels because everything runs on one laptop; there is
no automatic repair of a region that missed a write while it was down.

Regions come from the STORAGE_NODES env var:
    "Pune=http://storage-pune:9000,Mumbai=http://storage-mumbai:9000,..."
Adding a region = adding one entry there + one container. No code change.
"""
import asyncio
import logging
import os
import time
import httpx
from ..sync import lamport

logger = logging.getLogger(__name__)

_DEFAULT_LOCAL = "Pune=http://localhost:9101,Mumbai=http://localhost:9102,Delhi=http://localhost:9103"
WRITE_TIMEOUT = 4.0
READ_TIMEOUT = 3.0
HEALTH_TIMEOUT = 1.5

# Rate-limit "failover" event-log entries (a thumbnail reloads often; we
# don't want the Distributed Event Log flooded while a region is down).
_last_failover_log: dict[tuple[str, str], float] = {}


def get_nodes() -> list[tuple[str, str]]:
    raw = os.environ.get("STORAGE_NODES", _DEFAULT_LOCAL)
    nodes = []
    for part in raw.split(","):
        part = part.strip()
        if "=" in part:
            region, url = part.split("=", 1)
            nodes.append((region.strip(), url.strip().rstrip("/")))
    return nodes


async def replicate_file(filename: str, content: bytes) -> list[str]:
    """Write the file to every region at the same time. Returns the list
    of regions that actually stored it (a dead region just doesn't appear)."""
    nodes = get_nodes()

    async def _put(region: str, base: str) -> str | None:
        try:
            async with httpx.AsyncClient(timeout=WRITE_TIMEOUT) as client:
                r = await client.put(f"{base}/files/{filename}", content=content)
                return region if r.status_code == 200 else None
        except Exception as exc:
            logger.warning(f"DFS: write to region {region} failed - {exc}")
            return None

    results = await asyncio.gather(*[_put(region, base) for region, base in nodes])
    stored = [r for r in results if r]
    lamport.record_event(
        "dfs_replicate", filename,
        f"File {filename} replicated to {len(stored)}/{len(nodes)} regions: {', '.join(stored) or 'NONE'}",
    )
    return stored


async def fetch_file(filename: str, regions: list[str]) -> tuple[bytes, str] | None:
    """Try each region that SHOULD have the file, in order, until one
    answers. Returns (bytes, region_it_came_from) or None if every
    replica is unreachable."""
    base_by_region = dict(get_nodes())
    # Try regions in their configured order (Pune first, then Mumbai, ...)
    # — like "nearest region first" — so stopping the first region visibly
    # forces a failover to the next one instead of depending on DB row order.
    order = [r for r, _ in get_nodes() if r in regions] + [r for r in regions if r not in base_by_region]
    failed: list[str] = []
    for region in order:
        base = base_by_region.get(region)
        if not base:
            continue
        try:
            async with httpx.AsyncClient(timeout=READ_TIMEOUT) as client:
                r = await client.get(f"{base}/files/{filename}")
            if r.status_code == 200:
                if failed:
                    key = (filename, failed[0])
                    if time.time() - _last_failover_log.get(key, 0) > 60:
                        _last_failover_log[key] = time.time()
                        lamport.record_event(
                            "dfs_failover", filename,
                            f"Read of {filename}: {', '.join(failed)} unreachable -> served from {region} instead",
                        )
                return r.content, region
            failed.append(region)
        except Exception as exc:
            logger.warning(f"DFS: read from region {region} failed - {exc}")
            failed.append(region)
    return None


async def storage_health() -> list[dict]:
    async def _check(region: str, base: str) -> dict:
        try:
            async with httpx.AsyncClient(timeout=HEALTH_TIMEOUT) as client:
                r = await client.get(f"{base}/health")
            data = r.json()
            return {"region": region, "address": base, "status": "online", "file_count": data.get("file_count", 0)}
        except Exception:
            return {"region": region, "address": base, "status": "offline", "file_count": None}

    return list(await asyncio.gather(*[_check(region, base) for region, base in get_nodes()]))
