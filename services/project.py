"""Project and target management functions.

Extracted from commentator.py — project filtering, target getters,
manual task management, discussion queue helpers.
"""

import json
import time
import uuid

from db.connection import get_connection as _get_connection

DEFAULT_PROJECT_ID = "default"


def _db_connect():
    return _get_connection()


# ---------------------------------------------------------------------------
# Project / target helpers
# ---------------------------------------------------------------------------

def _active_project_id(settings=None):
    if isinstance(settings, dict):
        raw = settings.get("active_project_id")
    else:
        raw = None
    pid = str(raw or "").strip()
    return pid or DEFAULT_PROJECT_ID


def _project_id_for(item):
    if not isinstance(item, dict):
        return DEFAULT_PROJECT_ID
    pid = str(item.get("project_id") or "").strip()
    return pid or DEFAULT_PROJECT_ID


def _filter_project_items(items, project_id):
    if not isinstance(items, list):
        return []
    return [i for i in items if isinstance(i, dict) and _project_id_for(i) == project_id]


def get_project_targets(settings=None):
    s = settings if isinstance(settings, dict) else {}
    pid = _active_project_id(s)
    return _filter_project_items((s or {}).get("targets", []) or [], pid)


def get_project_discussion_targets(settings=None):
    s = settings if isinstance(settings, dict) else {}
    pid = _active_project_id(s)
    return _filter_project_items((s or {}).get("discussion_targets", []) or [], pid)


def ensure_discussion_targets_schema(settings: dict) -> bool:
    targets = settings.get("discussion_targets")
    if not isinstance(targets, list):
        return False
    changed = False
    used: set[str] = set()
    for t in targets:
        if not isinstance(t, dict):
            continue
        target_id = str(t.get("id") or "").strip()
        if not target_id or target_id in used:
            target_id = uuid.uuid4().hex
            while target_id in used:
                target_id = uuid.uuid4().hex
            t["id"] = target_id
            changed = True
        used.add(target_id)
        if "title" not in t or t.get("title") is None:
            t["title"] = ""
            changed = True

        scenes = t.get("scenes")
        if scenes is None:
            continue
        if not isinstance(scenes, list):
            t["scenes"] = []
            changed = True
            continue
        used_scene_ids: set[str] = set()
        cleaned_scenes: list[dict] = []
        for sc in scenes:
            if not isinstance(sc, dict):
                changed = True
                continue
            scene_id = str(sc.get("id") or "").strip()
            if not scene_id or scene_id in used_scene_ids:
                scene_id = uuid.uuid4().hex
                while scene_id in used_scene_ids:
                    scene_id = uuid.uuid4().hex
                sc["id"] = scene_id
                changed = True
            used_scene_ids.add(scene_id)

            if "title" not in sc or sc.get("title") is None:
                sc["title"] = ""
                changed = True
            if "operator_text" not in sc or sc.get("operator_text") is None:
                sc["operator_text"] = ""
                changed = True
            if "vector_prompt" not in sc or sc.get("vector_prompt") is None:
                sc["vector_prompt"] = ""
                changed = True
            cleaned_scenes.append(sc)
        if len(cleaned_scenes) != len(scenes):
            t["scenes"] = cleaned_scenes
            changed = True
    return changed


def get_project_reaction_targets(settings=None):
    s = settings if isinstance(settings, dict) else {}
    pid = _active_project_id(s)
    return _filter_project_items((s or {}).get("reaction_targets", []) or [], pid)


def get_project_monitor_targets(settings=None):
    s = settings if isinstance(settings, dict) else {}
    pid = _active_project_id(s)
    return _filter_project_items((s or {}).get("monitor_targets", []) or [], pid)


def get_project_manual_queue(settings=None):
    s = settings if isinstance(settings, dict) else {}
    pid = _active_project_id(s)
    return _filter_project_items((s or {}).get("manual_queue", []) or [], pid)


def _parse_manual_overrides(raw_overrides):
    if not raw_overrides:
        return {}
    if isinstance(raw_overrides, dict):
        return raw_overrides
    try:
        parsed = json.loads(raw_overrides)
        if isinstance(parsed, dict):
            return parsed
    except Exception:
        pass
    return {}


def _claim_project_manual_tasks(project_id, limit=50):
    project_id = str(project_id or DEFAULT_PROJECT_ID).strip() or DEFAULT_PROJECT_ID
    limit = max(1, int(limit))
    now_ts = time.time()
    claimed = []
    with _db_connect() as conn:
        rows = conn.execute(
            """
            SELECT id, project_id, chat_id, message_chat_id, post_id, overrides_json
            FROM manual_tasks
            WHERE project_id = ? AND status = 'pending'
            ORDER BY id ASC
            LIMIT ?
            """,
            (project_id, limit),
        ).fetchall()
        for row in rows:
            task_id = int(row["id"])
            cur = conn.execute(
                """
                UPDATE manual_tasks
                SET status = 'processing', started_at = ?, last_error = NULL
                WHERE id = ? AND status = 'pending'
                """,
                (now_ts, task_id),
            )
            if int(cur.rowcount or 0) != 1:
                continue
            claimed.append(
                {
                    "id": task_id,
                    "project_id": str(row["project_id"] or DEFAULT_PROJECT_ID),
                    "chat_id": str(row["chat_id"] or "").strip(),
                    "message_chat_id": str(row["message_chat_id"] or "").strip(),
                    "post_id": row["post_id"],
                    "overrides": _parse_manual_overrides(row["overrides_json"]),
                }
            )
    return claimed


def _set_manual_task_status(task_id, status, error=None):
    if not task_id:
        return
    status = str(status or "").strip().lower()
    if status not in {"pending", "processing", "done", "failed"}:
        status = "failed"
    now_ts = time.time()
    with _db_connect() as conn:
        if status == "pending":
            conn.execute(
                """
                UPDATE manual_tasks
                SET status = 'pending', started_at = NULL, finished_at = NULL, last_error = ?
                WHERE id = ?
                """,
                (str(error or "")[:1000] or None, int(task_id)),
            )
            return
        conn.execute(
            """
            UPDATE manual_tasks
            SET status = ?, finished_at = ?, last_error = ?
            WHERE id = ?
            """,
            (status, now_ts, str(error or "")[:1000] or None, int(task_id)),
        )


def migrate_legacy_manual_queue_to_db(current_settings, save_data_fn, settings_file):
    """Migrate legacy manual_queue from JSON settings to DB.

    Args:
        current_settings: The settings dict (will be mutated: manual_queue set to []).
        save_data_fn: Callable(file_path, data) to persist settings.
        settings_file: Path to the settings file.

    Returns:
        Number of tasks migrated.
    """
    legacy_queue = current_settings.get("manual_queue")
    if not isinstance(legacy_queue, list) or not legacy_queue:
        return 0
    moved = 0
    now_ts = time.time()
    with _db_connect() as conn:
        for task in legacy_queue:
            if not isinstance(task, dict):
                continue
            chat_id = str(task.get("chat_id") or "").strip()
            post_id_raw = task.get("post_id")
            if not chat_id or post_id_raw in (None, ""):
                continue
            try:
                post_id = int(post_id_raw)
            except Exception:
                continue
            message_chat_id = str(task.get("message_chat_id") or "").strip() or chat_id
            project_id = _project_id_for(task)
            overrides = task.get("overrides") if isinstance(task.get("overrides"), dict) else {}
            conn.execute(
                """
                INSERT INTO manual_tasks (
                    project_id, chat_id, message_chat_id, post_id,
                    overrides_json, status, created_at
                )
                VALUES (?, ?, ?, ?, ?, 'pending', ?)
                """,
                (
                    project_id,
                    chat_id,
                    message_chat_id,
                    post_id,
                    json.dumps(overrides, ensure_ascii=False),
                    now_ts,
                ),
            )
            moved += 1
    if moved:
        current_settings["manual_queue"] = []
        save_data_fn(settings_file, current_settings)
    return moved


def get_project_discussion_queue(settings=None):
    s = settings if isinstance(settings, dict) else {}
    pid = _active_project_id(s)
    return _filter_project_items((s or {}).get("discussion_queue", []) or [], pid)


def get_project_discussion_start_queue(settings=None):
    s = settings if isinstance(settings, dict) else {}
    pid = _active_project_id(s)
    return _filter_project_items((s or {}).get("discussion_start_queue", []) or [], pid)
