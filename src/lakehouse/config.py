from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_prefix="LH_", extra="ignore")

    kafka_brokers: str = "localhost:9092"
    topic: str = "transactions"
    consumer_group: str = "lakehouse-bronze"

    lake_root: str = "s3a://lakehouse"
    checkpoint_root: str = "s3a://lakehouse/_checkpoints"

    # 30s is the shortest trigger that does not shred the table into small
    # files at this volume. A 1s trigger writes thousands of them a day.
    trigger_interval: str = "30 seconds"
    max_offsets_per_trigger: int = 200_000

    bq_project: str = ""
    bq_dataset: str = "analytics"

    late_arrival_watermark: str = "2 hours"

    @property
    def bronze(self) -> str: return f"{self.lake_root}/bronze/transactions"
    @property
    def silver(self) -> str: return f"{self.lake_root}/silver/transactions"
    @property
    def gold(self) -> str: return f"{self.lake_root}/gold"


@lru_cache
def get_settings() -> Settings:
    return Settings()
