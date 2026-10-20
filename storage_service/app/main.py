from __future__ import annotations
"""
FA-1 Unit 4 — Distributed File System (storage node).

One instance of this tiny service = ONE storage "region" (e.g. Pune,
Mumbai, Delhi). Each region is its own process/container with its OWN
folder on disk — exactly like a DataNode in HDFS. It knows nothing about
the other regions; it only stores, returns, and lists files. All the
"which regions hold which file" bookkeeping lives in Main API (the
NameNode role). Configured only by environment variables, so adding a 4th
region is a config change, not a code change:

  REGION_NAME   human label shown in the UI (default "Region")
  STORAGE_DIR   folder this region keeps its files in
"""
import os
from pathlib import Path
from fastapi import FastAPI, Request, HTTPException, Response

REGION = os.environ.get("REGION_NAME", "Region")
STORAGE_DIR = Path(os.environ.get("STORAGE_DIR", "./storage"))
STORAGE_DIR.mkdir(parents=True, exist_ok=True)

app = FastAPI(title=f"MediConnect Storage Node — {REGION}")


def _safe_path(filename: str) -> Path:
    # Block path tricks like ../../etc/passwd — only a plain file name is allowed.
    if Path(filename).name != filename or filename.startswith("."):
        raise HTTPException(status_code=400, detail="Invalid file name.")
    return STORAGE_DIR / filename


@app.get("/health")
async def health():
    count = sum(1 for p in STORAGE_DIR.iterdir() if p.is_file())
    return {"status": "healthy", "region": REGION, "file_count": count}


@app.put("/files/{filename}")
async def put_file(filename: str, request: Request):
    data = await request.body()
    if not data:
        raise HTTPException(status_code=400, detail="Empty body.")
    _safe_path(filename).write_bytes(data)
    return {"stored": True, "region": REGION, "filename": filename, "bytes": len(data)}


@app.get("/files/{filename}")
async def get_file(filename: str):
    path = _safe_path(filename)
    if not path.exists():
        raise HTTPException(status_code=404, detail="Not on this region.")
    return Response(content=path.read_bytes(), media_type="application/octet-stream")


@app.get("/files")
async def list_files():
    return {"region": REGION, "files": sorted(p.name for p in STORAGE_DIR.iterdir() if p.is_file())}
