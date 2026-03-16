"""
OpenClaw 任务调度模块
功能：任务队列（文件回退）+ Session派发（openclaw CLI）+ 状态追踪
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
import hashlib
import json
import os
import re
import shutil
import subprocess
import time


def _agents_root() -> Path:
    return Path(__file__).resolve().parent


def _system_dir() -> Path:
    return _agents_root() / "_system"


def _read_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8-sig")
    except Exception:
        return path.read_text(encoding="utf-8", errors="ignore")


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8-sig")


def _load_json(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        return json.loads(_read_text(path))
    except Exception:
        return {}


def _save_json(path: Path, data: dict) -> None:
    _write_text(path, json.dumps(data, ensure_ascii=False, indent=2))


def _openclaw_bin() -> str | None:
    for name in ["openclaw", "openclaw.cmd", "openclaw.exe"]:
        p = shutil.which(name)
        if p:
            return p
    return None


def _now_ts() -> int:
    return int(time.time())


def _ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def queue_dir() -> Path:
    return _system_dir() / "task_queue"


def enqueue(agent_id: str, payload: dict, qdir: Path | None = None) -> str:
    qdir = qdir or queue_dir()
    agent_dir = qdir / agent_id / "pending"
    agent_dir.mkdir(parents=True, exist_ok=True)
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True)
    hid = hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]
    key = f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{hid}"
    _save_json(agent_dir / f"{key}.json", payload)
    return key


def dequeue(agent_id: str, limit: int = 5, qdir: Path | None = None) -> list[dict]:
    qdir = qdir or queue_dir()
    agent_dir = qdir / agent_id / "pending"
    if not agent_dir.exists():
        return []
    files = sorted(agent_dir.glob("*.json"), key=lambda p: p.stat().st_mtime)
    out: list[dict] = []
    for p in files[: max(0, int(limit))]:
        data = _load_json(p)
        if not data:
            continue
        data["_queue_key"] = p.stem
        data["_queue_path"] = str(p)
        out.append(data)
    return out


def ack(agent_id: str, queue_key: str, qdir: Path | None = None) -> bool:
    qdir = qdir or queue_dir()
    src = qdir / agent_id / "pending" / f"{queue_key}.json"
    if not src.exists():
        return False
    dst = qdir / agent_id / "done" / f"{queue_key}.json"
    dst.parent.mkdir(parents=True, exist_ok=True)
    try:
        src.replace(dst)
        return True
    except Exception:
        return False


def a2a_session_send(
    agent_id: str,
    message: str,
    session_id: str | None = None,
    profile: str = "mybot",
    timeout: int = 180,
) -> dict:
    bin_path = _openclaw_bin()
    if not bin_path:
        return {"enabled": False, "error": "openclaw cli not found"}

    sid = session_id or f"a2a-{datetime.now().strftime('%Y%m%d%H%M%S')}-{agent_id}"
    cmd = [
        bin_path,
        "--profile",
        profile,
        "agent",
        "--agent",
        agent_id,
        "--session-id",
        sid,
        "--message",
        message,
    ]
    try:
        p = subprocess.run(cmd, check=False, capture_output=True, text=True, timeout=timeout)
        return {"enabled": True, "session_id": sid, "returncode": p.returncode}
    except Exception as e:
        return {"enabled": False, "error": str(e)}


def a2a_enqueue_and_notify(
    agent_id: str,
    message: str,
    payload: dict | None = None,
    profile: str = "mybot",
    session_id: str | None = None,
) -> dict:
    payload = payload or {}
    payload = {
        **payload,
        "agent": agent_id,
        "message": message,
        "created_at": _ts(),
    }
    qkey = enqueue(agent_id=agent_id, payload=payload)
    send_res = a2a_session_send(agent_id=agent_id, message=message, session_id=session_id, profile=profile)
    return {"enabled": True, "queue_key": qkey, "send": send_res}


def parse_tasks_table(tasks_md: str) -> list[dict]:
    out: list[dict] = []
    for line in (tasks_md or "").splitlines():
        s = line.strip()
        if not s.startswith("|"):
            continue
        if "---" in s:
            continue
        cols = [c.strip() for c in s.strip("|").split("|")]
        if len(cols) < 6:
            continue
        if cols[0] == "任务ID":
            continue
        task_id, title, owner, status, deadline, next_step = cols[:6]
        if not task_id or not re.match(r"\d{4}-\d{2}-\d{2}-\d{3}", task_id):
            continue
        out.append(
            {
                "task_id": task_id,
                "title": title,
                "owner": owner,
                "status": status,
                "deadline": deadline,
                "next_step": next_step,
            }
        )
    return out


def build_task_message(task: dict) -> str:
    return (
        f"任务ID：{task.get('task_id')}\n"
        f"任务：{task.get('title')}\n"
        f"截止：{task.get('deadline')}\n"
        f"下一步：{task.get('next_step')}\n"
        f"交付：短汇报5-10行 + 长结果写入 shared/（给出文件名）"
    )


def dispatch_from_tasks_file(
    tasks_file: Path,
    state_file: Path,
    profile: str,
    allowed_agent_ids: set[str],
    min_gap_seconds: int,
) -> dict:
    if not tasks_file.exists():
        return {"enabled": False, "error": "TASKS.md not found"}

    tasks = parse_tasks_table(_read_text(tasks_file))
    active = [t for t in tasks if "进行中" in (t.get("status") or "")]

    state = _load_json(state_file)
    now_ts = _now_ts()
    sent = 0
    skipped = 0

    for t in active:
        task_id = t["task_id"]
        owners = [x.strip() for x in re.split(r"[+/，, ]+", t.get("owner") or "") if x.strip()]
        targets = [x for x in owners if x in allowed_agent_ids and x != "assistant"]
        if not targets:
            skipped += 1
            continue

        last = int((state.get(task_id) or 0))
        if now_ts - last < min_gap_seconds:
            skipped += 1
            continue

        message = build_task_message(t)
        for agent_id in targets:
            session_id = f"task-{task_id}-{agent_id}"
            a2a_enqueue_and_notify(
                agent_id=agent_id,
                message=message,
                payload={"task_id": task_id, "kind": "task", "source": str(tasks_file)},
                profile=profile,
                session_id=session_id,
            )
            sent += 1

        state[task_id] = now_ts

    _save_json(state_file, state)
    return {"enabled": True, "sent": sent, "skipped": skipped}

