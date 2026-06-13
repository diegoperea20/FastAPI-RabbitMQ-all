from pydantic_settings import BaseSettings
from functools import lru_cache


class Settings(BaseSettings):
    APP_NAME: str = "FastAPI RabbitMQ Demo"
    APP_VERSION: str = "0.1.0"
    DEBUG: bool = True

    DATABASE_URL: str = "sqlite+aiosqlite:///./tasks.db"

    RABBITMQ_HOST: str = "localhost"
    RABBITMQ_PORT: int = 5672
    RABBITMQ_USER: str = "guest"
    RABBITMQ_PASSWORD: str = "guest"
    RABBITMQ_VHOST: str = "/"

    RABBITMQ_CONNECTION_TIMEOUT: int = 10
    RABBITMQ_HEARTBEAT: int = 60
    RABBITMQ_BLOCKED_CONNECTION_TIMEOUT: int = 300

    RABBITMQ_POOL_SIZE: int = 2
    RABBITMQ_CHANNEL_POOL_SIZE: int = 10

    MAX_RETRIES: int = 3
    RETRY_DELAY_BASE: float = 2.0

    WORKER_PREFETCH_COUNT: int = 1

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = True


@lru_cache()
def get_settings() -> Settings:
    return Settings()


settings = get_settings()