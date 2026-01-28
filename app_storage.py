from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from typing import Any, Tuple


def load_json(path: str | Path, default: Any = None) -> Any:
    data, _ = load_json_with_error(path, default)
    return data


def load_json_with_error(path: str | Path, default: Any = None) -> Tuple[Any, str | None]:
    path = Path(path)
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f), None
    except FileNotFoundError:
        return default, None
    except json.JSONDecodeError as e:
        return default, f"JSONDecodeError: {e}"


def save_json(path: str | Path, data: Any, *, indent: int = 2) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    encoded = json.dumps(data, ensure_ascii=False, indent=indent)

    tmp_fd = None
    tmp_path: str | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=str(path.parent),
            delete=False,
            prefix=f".{path.name}.",
            suffix=".tmp",
        ) as tmp:
            tmp_fd = tmp.fileno()
            tmp_path = tmp.name
            tmp.write(encoded)
            tmp.write("\n")
            tmp.flush()
            os.fsync(tmp_fd)

        os.replace(tmp_path, path)
    finally:
        if tmp_path and os.path.exists(tmp_path):
            try:
                os.unlink(tmp_path)
            except OSError:
                pass

