import logging
import sys
import os
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import settings
from database import init_db
from rabbitmq import setup_infrastructure
from api import tasks_router

logging.basicConfig(
    level=logging.INFO if settings.DEBUG else logging.WARNING,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting up...")
    init_db()
    logger.info("Database initialized")
    setup_infrastructure()
    logger.info("RabbitMQ infrastructure initialized")
    yield
    logger.info("Shutting down...")
    from rabbitmq.connection import connection_manager
    connection_manager.close()


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
    from rabbitmq.connection import connection_manager
    rabbitmq_connected = connection_manager.is_connected()
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