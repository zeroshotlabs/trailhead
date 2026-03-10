from __future__ import annotations

import os
from pathlib import Path

import yaml
from pydantic import BaseModel, Field


class ServerConfig(BaseModel):
    host: str = "0.0.0.0"
    port: int = 8000


class AuthConfig(BaseModel):
    api_keys: list[str] = Field(min_length=1)


class AWSConfig(BaseModel):
    region: str = "us-east-1"
    log_group_prefix: str = "/trailhead"
    auto_create_groups: bool = False


class IngestConfig(BaseModel):
    max_batch_events: int = 10_000
    max_batch_bytes: int = 1_048_576


class Config(BaseModel):
    server: ServerConfig = ServerConfig()
    auth: AuthConfig
    aws: AWSConfig = AWSConfig()
    ingest: IngestConfig = IngestConfig()


_cached: Config | None = None


def load_config(path: str | Path | None = None) -> Config:
    """Load config from YAML. Path resolution: explicit arg > TRAILHEAD_CONFIG env > ./config.yaml"""
    global _cached
    if _cached is not None:
        return _cached

    if path is None:
        path = os.environ.get("TRAILHEAD_CONFIG", "config.yaml")

    with open(path) as f:
        data = yaml.safe_load(f)

    _cached = Config(**data)
    return _cached


def reset_config() -> None:
    global _cached
    _cached = None
