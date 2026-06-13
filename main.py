import logging
import sys
import os
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import settings
from database import init_db, close_db
from rabbitmq import setup_infrastructure, connection_manager
from api import tasks_router

logging.basicConfig(
    level=logging.INFO if settings.DEBUG else logging.WARNING,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting up...")
    await init_db()
    logger.info("Database initialized")
    try:
        await connection_manager.connect()
        await setup_infrastructure()
        logger.info("RabbitMQ infrastructure initialized")
    except Exception as e:
        logger.warning(f"RabbitMQ not available at startup: {e}")
    yield
    logger.info("Shutting down...")
    await connection_manager.close()
    await close_db()


app = FastAPI(
    title=settings.APP_NAME,
    version=settings.APP_VERSION,
    description="FastAPI with RabbitMQ demonstration - Multiple patterns: Basic, Work Queues, Pub/Sub, Routing with DLQ",
    lifespan=lifespan,
    debug=settings.DEBUG,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(tasks_router)


@app.get("/")
async def root():
    return {
        "name": settings.APP_NAME,
        "version": settings.APP_VERSION,
        "description": "FastAPI with RabbitMQ demonstration",
        "patterns": ["basic", "work_queue", "fanout", "routing"],
        "docs": "/docs",
        "rabbitmq_management": "http://localhost:15672",
    }


@app.get("/health")
async def health_check():
    rabbitmq_connected = await connection_manager.is_connected()
    return {
        "status": "healthy" if rabbitmq_connected else "degraded",
        "rabbitmq": "connected" if rabbitmq_connected else "disconnected",
        "database": "connected",
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=settings.DEBUG,
    )