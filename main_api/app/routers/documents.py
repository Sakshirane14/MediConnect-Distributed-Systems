from __future__ import annotations
"""
Business document upload — FA-1 §8 Distributed Multimedia Systems.
Replaces the old B2C "prescription upload" with B2B documents attached to a
replenishment order: Purchase Order, Invoice, or Delivery Note.
"""
import os
import uuid
import logging
from pathlib import Path
from fastapi import APIRouter, UploadFile, File, Form, HTTPException, Depends
from ..auth.auth import get_current_user
from ..db import save_business_document

router = APIRouter(prefix="/api", tags=["documents"])
logger = logging.getLogger(__name__)

UPLOAD_DIR = Path(os.environ.get("UPLOAD_DIR", os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "uploads")))
MAX_SIZE = 5 * 1024 * 1024
ALLOWED_TYPES = {"image/jpeg", "image/png", "image/webp", "application/pdf"}
ALLOWED_DOC_TYPES = {"purchase_order", "invoice", "delivery_note"}

@router.post("/documents")
async def upload_document(
    file: UploadFile = File(...),
    doc_type: str = Form("purchase_order"),
    order_id: str = Form(None),
    current_user: dict = Depends(get_current_user),
):
    if doc_type not in ALLOWED_DOC_TYPES:
        raise HTTPException(status_code=400, detail=f"doc_type must be one of {sorted(ALLOWED_DOC_TYPES)}.")

    if file.content_type not in ALLOWED_TYPES:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid file type '{file.content_type}'. Allowed: JPEG, PNG, WebP, PDF.",
        )

    content = await file.read()
    if len(content) > MAX_SIZE:
        raise HTTPException(
            status_code=413,
            detail=f"File too large ({len(content) // 1024}KB). Maximum allowed: 5MB.",
        )

    ext = Path(file.filename or "document.pdf").suffix or ".pdf"
    unique_name = f"DOC{uuid.uuid4().hex[:8]}{ext}"
    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    dest = UPLOAD_DIR / unique_name
    dest.write_bytes(content)

    size_kb = round(len(content) / 1024, 1)
    document_id = f"DOCID{uuid.uuid4().hex[:8]}"
    await save_business_document(document_id, order_id, doc_type, current_user["user_id"], unique_name, size_kb)

    logger.info(f"Business document uploaded: {unique_name} ({doc_type}) by {current_user['user_id']}")
    return {
        "success":     True,
        "document_id": document_id,
        "filename":    unique_name,
        "doc_type":    doc_type,
        "size_kb":     size_kb,
        "message":     "Document uploaded successfully.",
    }
