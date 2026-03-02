from __future__ import annotations

import hashlib
import os
from typing import Any, Dict, Tuple

DEVICE_KEYS = (
    "device_model",
    "system_version",
    "app_version",
    "lang_code",
    "system_lang_code",
)

DEFAULT_LANG_CODE = os.getenv("TG_DEVICE_LANG", "en").strip() or "en"
DEFAULT_SYSTEM_LANG_CODE = os.getenv("TG_DEVICE_SYSTEM_LANG", DEFAULT_LANG_CODE).strip() or DEFAULT_LANG_CODE
DEFAULT_DEVICE_MODE = (os.getenv("TG_DEVICE_MODE", "preserve") or "preserve").strip().lower()


def _default_prefer_mobile() -> bool | None:
    if DEFAULT_DEVICE_MODE in {"mobile", "phone", "android", "ios"}:
        return True
    if DEFAULT_DEVICE_MODE in {"desktop", "pc", "windows", "mac"}:
        return False
    return None


DEFAULT_PREFER_MOBILE = _default_prefer_mobile()

MOBILE_DEVICE_PROFILES = [
    {"device_model": "iPhone 11", "system_version": "16.6", "app_version": "10.0"},
    {"device_model": "iPhone 12", "system_version": "16.7", "app_version": "10.1"},
    {"device_model": "iPhone 13", "system_version": "17.0", "app_version": "10.2"},
    {"device_model": "iPhone 14", "system_version": "17.1", "app_version": "10.3"},
    {"device_model": "iPhone 15", "system_version": "17.2", "app_version": "10.4"},
]

DESKTOP_DEVICE_PROFILES = [
    {"device_model": "PC 64bit", "system_version": "Windows 10", "app_version": "6.4.2 x64"},
    {"device_model": "PC 64bit", "system_version": "Windows 11", "app_version": "6.4.2 x64"},
    {"device_model": "MacBookPro", "system_version": "macOS 13.6", "app_version": "6.4.2"},
]


def _stable_index(seed: str, size: int) -> int:
    if size <= 0:
        return 0
    h = hashlib.sha256(seed.encode("utf-8")).digest()
    return int.from_bytes(h[:4], "big") % size


def _select_mobile_profile(seed: str) -> Dict[str, str]:
    idx = _stable_index(seed or "default", len(MOBILE_DEVICE_PROFILES))
    base = dict(MOBILE_DEVICE_PROFILES[idx])
    base["lang_code"] = DEFAULT_LANG_CODE
    base["system_lang_code"] = DEFAULT_SYSTEM_LANG_CODE
    base["device_type"] = "mobile"
    return base


def _select_desktop_profile(seed: str) -> Dict[str, str]:
    idx = _stable_index(seed or "default", len(DESKTOP_DEVICE_PROFILES))
    base = dict(DESKTOP_DEVICE_PROFILES[idx])
    base["lang_code"] = DEFAULT_LANG_CODE
    base["system_lang_code"] = DEFAULT_SYSTEM_LANG_CODE
    base["device_type"] = "desktop"
    return base


def _ensure_device_profile(account: Dict[str, Any], prefer_mobile: bool | None = None) -> Tuple[Dict[str, Any], bool]:
    if not isinstance(account, dict):
        return {}, False

    if prefer_mobile is None:
        prefer_mobile = DEFAULT_PREFER_MOBILE

    if prefer_mobile is None:
        # Preserve whatever is already in the account without auto-filling.
        current = {k: account.get(k) for k in DEVICE_KEYS}
        if account.get("device_type"):
            current["device_type"] = account.get("device_type")
        return current, False

    before = {k: account.get(k) for k in DEVICE_KEYS + ("device_type",)}

    device_type = str(account.get("device_type") or "").lower().strip()
    missing = [k for k in DEVICE_KEYS if not account.get(k)]
    profile: Dict[str, Any] = {}
    changed = False

    seed = str(account.get("session_name") or account.get("phone") or account.get("user_id") or "")
    if device_type == "mobile" and not prefer_mobile:
        profile = _select_desktop_profile(seed)
        for key in DEVICE_KEYS:
            account[key] = profile.get(key)
        account["device_type"] = "desktop"
        missing = []
    elif device_type == "desktop" and prefer_mobile:
        profile = _select_mobile_profile(seed)
        for key in DEVICE_KEYS:
            account[key] = profile.get(key)
        account["device_type"] = "mobile"
        missing = []
    elif missing:
        if device_type == "desktop":
            profile = _select_desktop_profile(seed)
        elif device_type == "mobile":
            profile = _select_mobile_profile(seed)
        else:
            profile = _select_mobile_profile(seed) if prefer_mobile else _select_desktop_profile(seed)
        for key in missing:
            account[key] = profile.get(key)
        if not account.get("device_type"):
            account["device_type"] = profile.get("device_type")

    after = {k: account.get(k) for k in DEVICE_KEYS + ("device_type",)}
    if before != after:
        changed = True

    result = {k: account.get(k) for k in DEVICE_KEYS}
    if account.get("device_type"):
        result["device_type"] = account.get("device_type")
    return result, changed


def ensure_device_profile(account: Dict[str, Any], prefer_mobile: bool | None = None) -> bool:
    _, changed = _ensure_device_profile(account, prefer_mobile=prefer_mobile)
    return changed


def device_kwargs(account: Dict[str, Any], prefer_mobile: bool | None = None) -> Dict[str, str]:
    profile, _ = _ensure_device_profile(account, prefer_mobile=prefer_mobile)
    return {k: v for k, v in profile.items() if k in DEVICE_KEYS and v}
