from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env")

    gemini_api_key: str = ""
    gemini_model: str = "gemini-3.6-flash"

    kafka_bootstrap_servers: str = "localhost:19092"
    kafka_topic: str = "support-tickets"
    kafka_dlt_topic: str = "support-tickets-dlt"
    max_retries: int = 3

    postgres_host: str = "localhost"
    postgres_port: int = 5433
    postgres_db: str = "triage"
    postgres_user: str = "triage"
    postgres_password: str = "triage"

    @property
    def postgres_dsn(self) -> str:
        return (
            f"host={self.postgres_host} port={self.postgres_port} "
            f"dbname={self.postgres_db} user={self.postgres_user} "
            f"password={self.postgres_password}"
        )


settings = Settings()
