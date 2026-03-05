from __future__ import annotations

import os
from dataclasses import dataclass


def _env_flag(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _env_list(name: str, default: list[str]) -> list[str]:
    raw = os.getenv(name, "")
    if not raw.strip():
        return default
    return [item.strip() for item in raw.split(",") if item.strip()]


@dataclass(frozen=True)
class RuntimeSettings:
    serve_frontend: bool
    allowed_origins: list[str]
    allowed_origin_regex: str | None


def get_runtime_settings() -> RuntimeSettings:
    default_origins = [
        "http://127.0.0.1:8000",
        "http://localhost:8000",
        "http://127.0.0.1:8080",
        "http://localhost:8080",
    ]
    regex = os.getenv("APP_ALLOWED_ORIGIN_REGEX", "").strip() or None
    return RuntimeSettings(
        serve_frontend=_env_flag("APP_SERVE_FRONTEND", True),
        allowed_origins=_env_list("APP_ALLOWED_ORIGINS", default_origins),
        allowed_origin_regex=regex,
    )
