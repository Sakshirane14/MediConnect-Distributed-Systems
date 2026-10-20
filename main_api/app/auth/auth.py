from __future__ import annotations
import os
import uuid
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from jose import JWTError, jwt
from passlib.context import CryptContext
from pydantic import BaseModel
from ..db import create_user, get_user_by_email, get_user_by_id

logger = logging.getLogger(__name__)

SECRET_KEY = os.environ.get("JWT_SECRET", "mediconnect-dev-secret-change-in-prod")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60 * 24

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
bearer_scheme = HTTPBearer(auto_error=False)

class RegisterRequest(BaseModel):
    name: str
    email: str
    password: str
    role: str = "pharmacy_staff"          # 'pharmacy_staff' | 'distributor_staff'
    pharmacy_id: Optional[str] = None     # required when role == 'pharmacy_staff'
    distributor_id: Optional[str] = None  # required when role == 'distributor_staff'

class LoginRequest(BaseModel):
    email: str
    password: str

class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    user_id: str
    name: str
    role: str
    pharmacy_id: Optional[str] = None
    distributor_id: Optional[str] = None

def hash_password(password: str) -> str:
    return pwd_context.hash(password)

def verify_password(plain: str, hashed: str) -> bool:
    return pwd_context.verify(plain, hashed)

def create_access_token(data: dict) -> str:
    to_encode = data.copy()
    expire = datetime.now(timezone.utc) + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    to_encode["exp"] = expire
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)

def decode_token(token: str) -> dict:
    return jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])

async def get_current_user(
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(bearer_scheme),
) -> dict:
    exc = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid or missing authentication token",
        headers={"WWW-Authenticate": "Bearer"},
    )
    if not credentials:
        raise exc
    try:
        payload = decode_token(credentials.credentials)
        user_id = payload.get("sub")
        if not user_id:
            raise exc
        user = await get_user_by_id(user_id)
        if not user:
            raise exc
        return user
    except JWTError:
        raise exc

async def register_user(req: RegisterRequest) -> TokenResponse:
    if req.role not in ("distributor_staff", "pharmacy_staff"):
        raise HTTPException(status_code=400, detail="role must be 'distributor_staff' or 'pharmacy_staff'.")
    if req.role == "pharmacy_staff" and not req.pharmacy_id:
        raise HTTPException(status_code=400, detail="pharmacy_id is required for pharmacy_staff.")
    if req.role == "distributor_staff" and not req.distributor_id:
        raise HTTPException(status_code=400, detail="distributor_id is required for distributor_staff.")

    user_id = f"USR{uuid.uuid4().int % 10**6:06d}"
    password_hash = hash_password(req.password)
    ok = await create_user(user_id, req.name, req.email, password_hash, req.role, req.pharmacy_id, req.distributor_id)
    if not ok:
        raise HTTPException(status_code=400, detail="Email already registered.")
    token_data = {"sub": user_id, "role": req.role, "pharmacy_id": req.pharmacy_id, "distributor_id": req.distributor_id}
    token = create_access_token(token_data)
    return TokenResponse(
        access_token=token,
        user_id=user_id,
        name=req.name,
        role=req.role,
        pharmacy_id=req.pharmacy_id,
        distributor_id=req.distributor_id,
    )

async def login_user(req: LoginRequest) -> TokenResponse:
    user = await get_user_by_email(req.email)
    if not user or not verify_password(req.password, user["password_hash"]):
        raise HTTPException(status_code=401, detail="Invalid email or password.")
    token_data = {
        "sub": user["user_id"],
        "role": user["role"],
        "pharmacy_id": user.get("pharmacy_id"),
        "distributor_id": user.get("distributor_id"),
    }
    token = create_access_token(token_data)
    return TokenResponse(
        access_token=token,
        user_id=user["user_id"],
        name=user["name"],
        role=user["role"],
        pharmacy_id=user.get("pharmacy_id"),
        distributor_id=user.get("distributor_id"),
    )
