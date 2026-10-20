from __future__ import annotations
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from .db import init_db
from .grpc_clients.distributor_client import init_channels, close_channels
from .mq.publisher import init_rabbitmq, close_rabbitmq
from .routers import medicines, orders, documents, pharmacy, distributor_admin, network, requirements, internal, image_requirements, storage
from .ws.stock_ws import router as stock_ws_router
from .ws.webrtc_signaling import router as webrtc_router
from .auth.auth import RegisterRequest, LoginRequest, register_user, login_user

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("MediConnect Main API starting up...")
    await init_db()
    await init_channels()
    await init_rabbitmq()
    yield
    logger.info("MediConnect Main API shutting down...")
    await close_channels()
    await close_rabbitmq()

app = FastAPI(
    title="MediConnect API",
    description="Distributed Pharmacy Procurement & Multi-Distributor Medicine Supply Network",
    version="2.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.post("/api/auth/register")
async def register(req: RegisterRequest):
    return await register_user(req)

@app.post("/api/auth/login")
async def login(req: LoginRequest):
    return await login_user(req)

app.include_router(medicines.router)
app.include_router(orders.router)
app.include_router(documents.router)
app.include_router(pharmacy.router)
app.include_router(distributor_admin.router)
app.include_router(network.router)
app.include_router(requirements.router)
app.include_router(internal.router)
app.include_router(image_requirements.router)
app.include_router(storage.router)

app.include_router(stock_ws_router)
app.include_router(webrtc_router)

@app.get("/health")
async def health():
    return {"status": "healthy", "service": "main-api"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=False)
