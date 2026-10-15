import json
from typing import List, Union
from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=True,
        extra="ignore",
    )

    # Runtime & Project Metadata
    PROJECT_NAME: str = "Timechamp Production Engine"
    ENVIRONMENT: str = "development"
    DEBUG: bool = True
    API_V1_STR: str = "/api/v1"
    SECRET_KEY: str = "change-this-to-a-super-secret-random-32-byte-hex-string-in-production"
    ALLOWED_HOSTS: Union[List[str], str] = ["*"]

    # Server Network Binding
    SERVER_HOST: str = "0.0.0.0"
    SERVER_PORT: int = 8000
    WORKERS_COUNT: int = 2

    # ClickHouse Analytics DB Settings
    CLICKHOUSE_HOST: str = "clickhouse"
    CLICKHOUSE_PORT: int = 8123
    CLICKHOUSE_NATIVE_PORT: int = 9000
    CLICKHOUSE_USER: str = "default"
    CLICKHOUSE_PASSWORD: str = "clickhouse_secret"
    CLICKHOUSE_DB: str = "timechamp_analytics"
    CLICKHOUSE_SECURE: bool = False

    # Redis Broker & Cache Settings
    REDIS_HOST: str = "redis"
    REDIS_PORT: int = 6379
    REDIS_PASSWORD: str = ""
    REDIS_DB: int = 0
    CELERY_BROKER_URL: str = "redis://redis:6379/0"
    CELERY_RESULT_BACKEND: str = "redis://redis:6379/1"

    # Lightweight Event Streaming (Redis Streams)
    REDIS_STREAM_EVENTS: str = "timechamp:events:stream"
    REDIS_STREAM_GROUP: str = "timechamp-consumer-group"


    # Timechamp External API Credentials
    TIMECHAMP_API_BASE_URL: str = "https://wyzmindz7570.v5.timechamp.io"
    TIMECHAMP_USERNAME: str = "command-center@autosherpas.com"
    TIMECHAMP_PASSWORD: str = "Admin$123"
    TIMECHAMP_SYNC_INTERVAL_MINUTES: int = 15


    @field_validator("ALLOWED_HOSTS", mode="before")
    @classmethod
    def assemble_cors_origins(cls, v: Union[str, List[str]]) -> List[str]:
        if isinstance(v, str):
            try:
                parsed = json.loads(v)
                if isinstance(parsed, list):
                    return parsed
            except Exception:
                return [i.strip() for i in v.split(",") if i.strip()]
        return v


settings = Settings()
