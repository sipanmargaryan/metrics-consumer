from typing import Literal

from pydantic import BaseModel, Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy.engine import URL


class KafkaSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="KAFKA_", env_file=".env", extra="ignore"
    )

    brokers: str = "kafka-1:9092,kafka-2:9092,kafka-3:9092"
    topic: str = Field(default="orders", min_length=1)
    group_id: str = Field(
        default="metrics-orders-consumer", min_length=1, max_length=200
    )
    auto_offset_reset: Literal["earliest", "latest", "none"] = "earliest"
    retry_backoff_seconds: float = Field(default=5, gt=0)
    processing_timeout_seconds: float = Field(default=30, gt=0, lt=240)

    @field_validator("brokers", "topic", "group_id")
    @classmethod
    def not_blank(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("must not be blank")
        return value.strip()

    @field_validator("brokers")
    @classmethod
    def has_brokers(cls, value: str) -> str:
        if not value.replace(",", " ").split():
            raise ValueError("at least one broker is required")
        return value

    @property
    def bootstrap_servers(self) -> list[str]:
        return self.brokers.replace(",", " ").split()


class DatabaseSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="POSTGRES_", env_file=".env", extra="ignore"
    )

    host: str = "localhost"
    port: int = Field(default=5433, ge=1, le=65535)
    user: str = "consumer"
    password: str = Field(default="consumer", repr=False)
    database: str = "metrics_consumer"
    pool_size: int = Field(default=5, gt=0)
    max_overflow: int = Field(default=0, ge=0)

    @property
    def database_url(self) -> URL:
        return URL.create(
            "postgresql+asyncpg",
            username=self.user,
            password=self.password,
            host=self.host,
            port=self.port,
            database=self.database,
        )


class AppSettings(BaseModel):
    kafka: KafkaSettings = Field(default_factory=KafkaSettings)
    database: DatabaseSettings = Field(default_factory=DatabaseSettings)


settings = AppSettings()
