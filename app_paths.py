from __future__ import annotations

import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent


def _resolve_path(value: str | None, default: Path) -> Path:
    if not value:
        return default
    return Path(value).expanduser().resolve()


DATA_DIR = _resolve_path(os.getenv("APP_DATA_DIR"), BASE_DIR)
CONFIG_FILE = _resolve_path(os.getenv("APP_CONFIG_FILE"), BASE_DIR / "config.ini")

SETTINGS_FILE = str(DATA_DIR / "ai_settings.json")
ACCOUNTS_FILE = str(DATA_DIR / "accounts.json")

PROXIES_FILE = str(DATA_DIR / "proxies.txt")


def ensure_data_dir() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
