"""
OpenClaw 心跳和定时任务模块
功能：定期检查各专家状态、同步数据、自动总结
"""

from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime, timezone
from pathlib import Path
import argparse
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import time


# ============ 配置区 ============
# 心跳间隔（小时）
HEARTBEAT_INTERVAL_HOURS = 1
# 数据同步间隔（小时）
SYNC_INTERVAL_HOURS = 2
# 记忆总结间隔（小时）
MEMORY_SUMMARY_INTERVAL_HOURS = 24

MAX_EXTRACT_PER_AGENT = 120
SUMMARY_TOP_K = 20

DISPATCH_INTERVAL_HOURS = 1
DISPATCH_MIN_GAP_SECONDS = 60 * 30
DISPATCH_PROFILE = os.environ.get("OPENCLAW_PROFILE", "mybot")
DISPATCH_AGENT_IDS = {"assistant", "coder", "writer", "ops", "designer", "finance", "video"}

INTAKE_INTERVAL_MINUTES = 10
INTAKE_MAX_MESSAGES_PER_RUN = 6
INTAKE_INIT_TAIL_BYTES = 2_000_000
INTAKE_INIT_MAX_AGE_HOURS = 6

# 专家列表
AGENTS = [
    "1_全能助理",
    "2_编程专家",
    "3_写作专家",
    "4_运营专家",
    "5_设计专家",
    "6_财务专家",
    "7_视频专家",
]


def _repo_root() -> Path:
    return Path(__file__).resolve().parent.parent


def _agents_root() -> Path:
    return Path(__file__).resolve().parent


def _system_dir() -> Path:
    return _agents_root() / "_system"


def _now() -> datetime:
    return datetime.now()


def _ts() -> str:
    return _now().strftime("%Y-%m-%d %H:%M:%S")


def _slug_ts() -> str:
    return _now().strftime("%Y%m%d_%H%M%S")


def _read_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8-sig")
    except Exception:
        return path.read_text(encoding="utf-8", errors="ignore")


def _is_sensitive(text: str) -> bool:
    if not text:
        return False
    if "github_pat_" in text:
        return True
    if "appsecret" in text.lower():
        return True
    if re.search(r"\bsk-[A-Za-z0-9]{20,}\b", text):
        return True
    if re.search(r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b", text, flags=re.I):
        return True
    if re.search(r"\b1\d{10}\b", text):
        return True
    if re.search(r"\b[0-9a-f]{32}\b", text, flags=re.I):
        return True
    return False


def _extract_candidates(md: str) -> list[str]:
    lines = md.splitlines()
    out: list[str] = []
    for raw in lines:
        s = raw.strip()
        if not s:
            continue
        if "[待填写]" in s or "用于临时记录" in s:
            continue
        if re.search(r"\[[^\]]+\]", s) and "(" not in s:
            continue
        if s.startswith("- ") or s.startswith("* "):
            out.append(s[2:].strip())
            continue
        if s.startswith("|") and s.count("|") >= 3 and "---" not in s:
            if "任务ID" in s or "任务名称" in s or "日期" in s:
                continue
            out.append(s)
            continue
    dedup: list[str] = []
    seen: set[str] = set()
    for s in out:
        t = re.sub(r"\s+", " ", s).strip()
        if not t:
            continue
        if t in seen:
            continue
        seen.add(t)
        dedup.append(t)
    return dedup


def _score_importance(text: str) -> int:
    t = text.lower()
    score = 3
    if any(k in text for k in ["必须", "铁律", "禁止", "红线"]):
        score += 5
    if any(k in text for k in ["用户强调", "重点", "结论", "最终"]):
        score += 2
    if any(k in text for k in ["风险", "异常", "阻塞", "延期"]):
        score += 2
    if any(k in text for k in ["截止", "里程碑", "任务", "交付"]):
        score += 2
    if any(k in text for k in ["投资人", "融资", "bp", "邮件", "发布"]):
        score += 2
    if re.search(r"\b20\d{2}-\d{2}-\d{2}\b", text):
        score += 2
    if "todo" in t or "tbd" in t:
        score -= 2
    return max(1, min(10, score))


def _upsert_chroma(items: list[dict]) -> dict:
    if str(_repo_root()) not in sys.path:
        sys.path.insert(0, str(_repo_root()))
    try:
        from openclaw_agents.memory_system import semantic_memory
    except Exception as e:
        return {"enabled": False, "error": str(e)}

    ok = 0
    for it in items:
        try:
            semantic_memory.upsert(
                memory_id=it["id"],
                text=it["content"],
                metadata=it.get("metadata") or {},
            )
            ok += 1
        except Exception:
            continue
    return {"enabled": True, "upserted": ok}


def _openclaw_bin() -> str | None:
    for name in ["openclaw", "openclaw.cmd", "openclaw.exe"]:
        p = shutil.which(name)
        if p:
            return p
    return None


def _load_dispatch_state(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        return json.loads(_read_text(path))
    except Exception:
        return {}


def _save_dispatch_state(path: Path, state: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8-sig")


def _load_json_file(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        return json.loads(_read_text(path))
    except Exception:
        return {}


def _write_json_file(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8-sig")


def _find_assistant_sessions_dir(profile: str) -> Path | None:
    override = os.environ.get("OPENCLAW_SESSIONS_DIR")
    if override:
        p = Path(override)
        if p.exists() and p.is_dir():
            return p
    home = Path.home()
    candidates = [
        home / f".openclaw-{profile}" / "agents" / "assistant" / "sessions",
        home / ".openclaw-mybot" / "agents" / "assistant" / "sessions",
        home / ".openclaw" / "agents" / "assistant" / "sessions",
    ]
    for p in candidates:
        if p.exists() and p.is_dir():
            return p
    return None


def _get_main_session_jsonl(sessions_dir: Path) -> Path | None:
    sessions_json = sessions_dir / "sessions.json"
    if sessions_json.exists():
        data = _load_json_file(sessions_json)
        main = data.get("agent:assistant:main") or {}
        sid = main.get("sessionId")
        if sid:
            p = sessions_dir / f"{sid}.jsonl"
            if p.exists():
                return p

    jsonls = sorted(sessions_dir.glob("*.jsonl"), key=lambda x: x.stat().st_mtime, reverse=True)
    return jsonls[0] if jsonls else None


def _parse_iso_ts(ts: str) -> datetime | None:
    if not ts:
        return None
    try:
        if ts.endswith("Z"):
            ts = ts[:-1] + "+00:00"
        return datetime.fromisoformat(ts)
    except Exception:
        return None


def _clean_user_text(raw: str) -> str:
    s = (raw or "").strip()
    if not s:
        return ""
    m = re.search(r"@_user_\d+\s+([\s\S]+)", s)
    if m:
        s = m.group(1).strip()
    s = re.sub(r"^Conversation info \(untrusted metadata\):[\s\S]*?Sender \(untrusted metadata\):[\s\S]*?\n\n", "", s).strip()
    s = re.sub(r"^Sender \(untrusted metadata\):[\s\S]*?\n\n", "", s).strip()
    cut_markers = [
        "\n\nWhen creating or updating a cron task",
        "\n\nSECURITY NOTICE:",
        "\n\n<<<EXTERNAL_UNTRUSTED_CONTENT",
    ]
    for mk in cut_markers:
        idx = s.find(mk)
        if idx != -1:
            s = s[:idx].strip()
    s = re.split(r"\n{3,}", s)[0].strip()
    return s


def _route_targets(text: str) -> list[str]:
    s = (text or "").strip()
    if not s:
        return []

    prefix_map = {
        "/code": ["coder"],
        "/write": ["writer"],
        "/ops": ["ops"],
        "/design": ["designer"],
        "/finance": ["finance"],
        "/video": ["video"],
    }
    for k, v in prefix_map.items():
        if s.startswith(k):
            return v

    rules: list[tuple[str, list[str]]] = [
        ("coder", ["报错", "实现", "接口", "部署", "脚手架", "依赖", "调试", "日志", "性能", "重构", "前后端"]),
        ("writer", ["文案", "脚本", "邮件", "总结", "润色", "公文", "PRD", "说明书", "公众号"]),
        ("ops", ["增长", "投放", "活动", "用户分层", "数据复盘", "渠道", "留存", "转化", "运营"]),
        ("designer", ["UI", "UX", "视觉", "交互", "规范", "组件", "落地清单", "稿件", "设计"]),
        ("finance", ["预算", "现金流", "ROI", "定价", "成本", "口径", "报表", "融资", "投资人", "BP"]),
        ("video", ["短视频", "抖音", "视频号", "分镜", "剪辑", "镜头", "节奏", "封面", "脚本结构", "拍摄"]),
    ]
    hits: list[str] = []
    for owner, keys in rules:
        if any(k in s for k in keys):
            hits.append(owner)
    if not hits:
        return ["assistant"]
    uniq: list[str] = []
    for x in hits:
        if x not in uniq:
            uniq.append(x)
    return uniq[:2]


def _extract_deadline(text: str) -> str:
    m = re.search(r"\b(20\d{2}-\d{2}-\d{2})\b", text or "")
    if m:
        return m.group(1)
    return _now().strftime("%Y-%m-%d")


def _split_action_items(text: str) -> list[str]:
    s = (text or "").strip()
    if not s:
        return []
    parts = re.split(r"(?:\n+|[；;。!?！？])\s*", s)
    out: list[str] = []
    for p in parts:
        p = p.strip()
        if not p:
            continue
        if len(p) < 4:
            continue
        if re.fullmatch(r"[谢谢好的OKok嗯啊]+", p):
            continue
        out.append(p)
    return out


def _next_task_id(tasks_md: str, day: str) -> str:
    max_n = 0
    for m in re.finditer(rf"\b{re.escape(day)}-(\d{{3}})\b", tasks_md or ""):
        try:
            max_n = max(max_n, int(m.group(1)))
        except Exception:
            continue
    return f"{day}-{max_n + 1:03d}"


def _insert_rows_into_ongoing(tasks_md: str, rows: list[str]) -> str:
    lines = (tasks_md or "").splitlines()
    if not lines:
        return tasks_md

    start_idx = None
    for i, line in enumerate(lines):
        if line.strip() == "## 正在进行":
            start_idx = i
            break
    if start_idx is None:
        return tasks_md

    table_header_idx = None
    for i in range(start_idx, min(len(lines), start_idx + 80)):
        if lines[i].lstrip().startswith("| 任务ID |"):
            table_header_idx = i
            break
    if table_header_idx is None:
        return tasks_md

    insert_at = None
    for i in range(table_header_idx + 2, len(lines)):
        if not lines[i].lstrip().startswith("|"):
            insert_at = i
            break
    if insert_at is None:
        insert_at = len(lines)

    existing = "\n".join(lines[table_header_idx:insert_at])
    filtered_rows: list[str] = []
    for r in rows:
        if r.strip() and r.strip() not in existing:
            filtered_rows.append(r)
    if not filtered_rows:
        return tasks_md

    new_lines = lines[:insert_at] + filtered_rows + lines[insert_at:]
    return "\n".join(new_lines).rstrip() + "\n"


def _tasks_file() -> Path:
    return _agents_root() / "1_全能助理" / "workspace" / "TASKS.md"


def intake_user_requirements() -> dict:
    print(f"\n{'='*50}")
    print(f"🧾 需求摄取 - {_ts()}")
    print(f"{'='*50}")

    sessions_dir = _find_assistant_sessions_dir(DISPATCH_PROFILE)
    if not sessions_dir:
        print("⚠️ 未找到 assistant sessions 目录，跳过需求摄取")
        return {"enabled": False, "error": "sessions dir not found"}

    session_path = _get_main_session_jsonl(sessions_dir)
    if not session_path:
        print("⚠️ 未找到 main session jsonl，跳过需求摄取")
        return {"enabled": False, "error": "session jsonl not found"}

    tasks_file = _tasks_file()
    if not tasks_file.exists():
        print("⚠️ 未找到 TASKS.md，跳过需求摄取")
        return {"enabled": False, "error": "TASKS.md not found"}

    state_file = _system_dir() / "intake" / "state.json"
    state = _load_json_file(state_file)
    sessions_state = state.get("sessions")
    if not isinstance(sessions_state, dict):
        sessions_state = {}
        prev_session = state.get("session")
        if prev_session:
            sessions_state[str(prev_session)] = {
                "cursor": int(state.get("cursor") or 0),
                "seen": state.get("seen") or [],
                "processed": state.get("processed") or {},
                "updated_at": state.get("updated_at") or "",
            }

    session_key = str(session_path)
    session_state = sessions_state.get(session_key) or {}

    cursor = int(session_state.get("cursor") or 0)
    seen = session_state.get("seen") or []
    if not isinstance(seen, list):
        seen = []
    seen_set = set([str(x) for x in seen][-5000:])
    processed = session_state.get("processed") or {}
    if not isinstance(processed, dict):
        processed = {}

    file_size = session_path.stat().st_size
    if cursor > file_size:
        cursor = 0

    start_pos = cursor
    init_mode = False
    if not session_state:
        init_mode = True
        start_pos = max(0, file_size - INTAKE_INIT_TAIL_BYTES)

    try:
        with session_path.open("rb") as f:
            f.seek(start_pos)
            chunk = f.read()
    except Exception as e:
        return {"enabled": False, "error": str(e)}

    text = chunk.decode("utf-8", errors="ignore")
    raw_lines = [ln for ln in text.splitlines() if ln.strip()]

    now = _now()
    messages: list[dict] = []
    for ln in raw_lines:
        try:
            rec = json.loads(ln)
        except Exception:
            continue
        if rec.get("type") != "message":
            continue
        msg = rec.get("message") or {}
        if (msg.get("role") or "") != "user":
            continue
        parts = msg.get("content") or []
        texts: list[str] = []
        for p in parts:
            if isinstance(p, dict) and p.get("type") == "text":
                texts.append(p.get("text") or "")
        raw = "\n".join([t for t in texts if t]).strip()
        cleaned = _clean_user_text(raw)
        if not cleaned:
            continue
        if _is_sensitive(cleaned):
            continue

        rid = str(rec.get("id") or "")
        ts = _parse_iso_ts(str(rec.get("timestamp") or ""))
        if init_mode and ts:
            if ts.tzinfo:
                age_h = (datetime.now(tz=timezone.utc) - ts.astimezone(timezone.utc)).total_seconds() / 3600.0
            else:
                age_h = (now - ts).total_seconds() / 3600.0
            if age_h > INTAKE_INIT_MAX_AGE_HOURS:
                continue

        h = hashlib.sha1((rid + "\n" + cleaned).encode("utf-8")).hexdigest()
        if h in seen_set:
            continue
        messages.append({"hash": h, "text": cleaned, "timestamp": str(rec.get("timestamp") or "")})

    if messages:
        messages = messages[-INTAKE_MAX_MESSAGES_PER_RUN:]

    tasks_md = _read_text(tasks_file)
    new_rows: list[str] = []
    created = 0
    processed_now: list[dict] = []
    day = _now().strftime("%Y-%m-%d")
    for m in messages:
        task_ids_for_msg: list[str] = []
        action_items = _split_action_items(m["text"])[:3]
        for it in action_items:
            owners = _route_targets(it)
            deadline = _extract_deadline(it)
            for owner in owners:
                if owner == "assistant":
                    owner_cell = "assistant"
                else:
                    owner_cell = f"assistant/{owner}"
                task_id = _next_task_id(tasks_md, day)
                title = re.sub(r"\s+", " ", it).strip()
                if len(title) > 22:
                    title = title[:22].rstrip() + "…"
                next_step = "按派单规范交付：短汇报 + shared/落盘 + sessions_send回传"
                row = f"| {task_id} | {title} | {owner_cell} | 进行中 | {deadline} | {next_step} |"
                if task_id in tasks_md:
                    continue
                new_rows.append(row)
                tasks_md += "\n" + task_id
                created += 1
                task_ids_for_msg.append(task_id)
        if task_ids_for_msg:
            processed_now.append({"hash": m["hash"], "timestamp": m.get("timestamp") or "", "text": m["text"], "task_ids": task_ids_for_msg})

    if created:
        updated = _insert_rows_into_ongoing(_read_text(tasks_file), new_rows)
        tasks_file.write_text(updated, encoding="utf-8-sig")

    for m in messages:
        seen_set.add(m["hash"])
        if m["hash"] not in processed:
            processed[m["hash"]] = {"timestamp": m.get("timestamp") or "", "text": m["text"], "task_ids": []}

    for p in processed_now:
        h = p["hash"]
        cur = processed.get(h) or {}
        cur["timestamp"] = p.get("timestamp") or cur.get("timestamp") or ""
        cur["text"] = p.get("text") or cur.get("text") or ""
        cur_ids = cur.get("task_ids") or []
        if not isinstance(cur_ids, list):
            cur_ids = []
        for tid in p.get("task_ids") or []:
            if tid not in cur_ids:
                cur_ids.append(tid)
        cur["task_ids"] = cur_ids[-50:]
        processed[h] = cur

    try:
        if str(_repo_root()) not in sys.path:
            sys.path.insert(0, str(_repo_root()))
        from openclaw_agents.memory_system import remember
    except Exception:
        remember = None

    if remember:
        for p in processed_now:
            content = json.dumps(
                {
                    "timestamp": p.get("timestamp") or "",
                    "text": p.get("text") or "",
                    "task_ids": p.get("task_ids") or [],
                },
                ensure_ascii=False,
            )
            try:
                remember("1_全能助理", "需求摄取", content, importance=8)
            except Exception:
                continue

    sessions_state[session_key] = {
        "cursor": file_size,
        "seen": list(seen_set)[-5000:],
        "processed": dict(list(processed.items())[-2000:]),
        "updated_at": _ts(),
    }
    if len(sessions_state) > 10:
        keys = sorted(
            sessions_state.keys(),
            key=lambda k: (sessions_state.get(k) or {}).get("updated_at") or "",
            reverse=True,
        )[:10]
        sessions_state = {k: sessions_state[k] for k in keys if k in sessions_state}

    _write_json_file(
        state_file,
        {
            "active_session": session_key,
            "sessions": sessions_state,
            "updated_at": _ts(),
        },
    )

    print(f"✅ 需求摄取完成：messages={len(messages)}, created_tasks={created}")
    return {"enabled": True, "messages": len(messages), "created_tasks": created, "session": str(session_path)}


def _parse_tasks_table(tasks_md: str) -> list[dict]:
    out: list[dict] = []
    for line in tasks_md.splitlines():
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


def _dispatch_tasks_legacy() -> dict:
    print(f"\n{'='*50}")
    print(f"📌 任务派单 - {_ts()}")
    print(f"{'='*50}")

    bin_path = _openclaw_bin()
    if not bin_path:
        print("⚠️ 未找到 openclaw CLI，跳过派单")
        return {"enabled": False, "error": "openclaw cli not found"}

    tasks_file = _agents_root() / "1_全能助理" / "workspace" / "TASKS.md"
    if not tasks_file.exists():
        print("⚠️ 未找到 TASKS.md，跳过派单")
        return {"enabled": False, "error": "TASKS.md not found"}

    tasks = _parse_tasks_table(_read_text(tasks_file))
    active = [t for t in tasks if "进行中" in (t.get("status") or "")]

    state_file = _system_dir() / "dispatcher" / "state.json"
    state = _load_dispatch_state(state_file)
    now_ts = int(time.time())
    sent = 0
    skipped = 0

    for t in active:
        task_id = t["task_id"]
        owners = [x.strip() for x in re.split(r"[+/，, ]+", t.get("owner") or "") if x.strip()]
        targets = [x for x in owners if x in DISPATCH_AGENT_IDS and x != "assistant"]
        if not targets:
            skipped += 1
            continue

        last = int((state.get(task_id) or 0))
        if now_ts - last < DISPATCH_MIN_GAP_SECONDS:
            skipped += 1
            continue

        message = (
            f"任务ID：{task_id}\n"
            f"任务：{t.get('title')}\n"
            f"截止：{t.get('deadline')}\n"
            f"下一步：{t.get('next_step')}\n"
            f"交付：短汇报5-10行 + 长结果写入 shared/（给出文件名）"
        )

        for agent_id in targets:
            session_id = f"task-{task_id}-{agent_id}"
            cmd = [
                bin_path,
                "--profile",
                DISPATCH_PROFILE,
                "agent",
                "--agent",
                agent_id,
                "--session-id",
                session_id,
                "--message",
                message,
            ]
            try:
                subprocess.run(cmd, check=False, capture_output=True, text=True, timeout=180)
                sent += 1
            except Exception:
                continue

        state[task_id] = now_ts

    _save_dispatch_state(state_file, state)
    print(f"✅ 派单完成：sent={sent}, skipped={skipped}")
    return {"enabled": True, "sent": sent, "skipped": skipped}


def dispatch_tasks() -> dict:
    print(f"\n{'='*50}")
    print(f"📌 任务派单 - {_ts()}")
    print(f"{'='*50}")

    bin_path = _openclaw_bin()
    if not bin_path:
        print("⚠️ 未找到 openclaw CLI，跳过派单")
        return {"enabled": False, "error": "openclaw cli not found"}

    tasks_file = _agents_root() / "1_全能助理" / "workspace" / "TASKS.md"
    state_file = _system_dir() / "dispatcher" / "state.json"

    try:
        if str(_repo_root()) not in sys.path:
            sys.path.insert(0, str(_repo_root()))
        from openclaw_agents.task_dispatcher import dispatch_from_tasks_file

        res = dispatch_from_tasks_file(
            tasks_file=tasks_file,
            state_file=state_file,
            profile=DISPATCH_PROFILE,
            allowed_agent_ids=DISPATCH_AGENT_IDS,
            min_gap_seconds=DISPATCH_MIN_GAP_SECONDS,
        )
        if res.get("enabled"):
            print(f"✅ 派单完成：sent={res.get('sent')}, skipped={res.get('skipped')}")
        else:
            print("⚠️ 派单未执行")
        return res
    except Exception:
        return _dispatch_tasks_legacy()

def get_agent_status(agent_name: str) -> dict:
    """检查各专家状态"""
    workspace_path = _agents_root() / agent_name / "workspace"
    memory_path = _agents_root() / agent_name / "memory" / "memory.md"
    tasks_path = _agents_root() / agent_name / "workspace" / "TASKS.md"

    status = {
        "name": agent_name,
        "last_heartbeat": _ts(),
        "workspace_exists": workspace_path.exists(),
        "memory_exists": memory_path.exists(),
        "tasks_exists": tasks_path.exists(),
        "workspace_path": str(workspace_path),
        "memory_path": str(memory_path),
        "status": "active",
    }

    if not status["workspace_exists"] or not status["memory_exists"]:
        status["status"] = "missing"

    return status


def heartbeat_check() -> list[dict]:
    """心跳检查 - 记录各专家状态"""
    print(f"\n{'='*50}")
    print(f"💓 心跳检查 - {_ts()}")
    print(f"{'='*50}")

    results: list[dict] = []
    for agent in AGENTS:
        status = get_agent_status(agent)
        results.append(status)
        if status["status"] == "active":
            print(f"✅ {agent}: {status['status']}")
        else:
            print(f"⚠️ {agent}: {status['status']}")

    out_dir = _system_dir() / "heartbeat"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / f"{_slug_ts()}.json"
    out_file.write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding="utf-8-sig")

    print(f"✅ 心跳记录已保存: {out_file}")
    return results


def sync_agent_data() -> dict:
    """数据同步 - 汇总各专家memory文件"""
    print(f"\n{'='*50}")
    print(f"🔄 数据同步 - {_ts()}")
    print(f"{'='*50}")

    summary: dict = {
        "sync_time": _ts(),
        "repo_root": str(_repo_root()),
        "agents": {},
    }

    extracted_items: list[dict] = []
    for agent in AGENTS:
        memory_path = _agents_root() / agent / "memory" / "memory.md"
        if memory_path.exists():
            md = _read_text(memory_path)
            candidates = _extract_candidates(md)[:MAX_EXTRACT_PER_AGENT]
            filtered: list[dict] = []
            for c in candidates:
                if _is_sensitive(c):
                    continue
                importance = _score_importance(c)
                hid = hashlib.sha1((agent + "\n" + c).encode("utf-8")).hexdigest()[:16]
                filtered.append(
                    {
                        "id": f"{agent}:{hid}",
                        "content": c,
                        "metadata": {
                            "agent": agent,
                            "source": str(memory_path),
                            "importance": importance,
                            "ts": _ts(),
                        },
                        "importance": importance,
                    }
                )

            extracted_items.extend(filtered)
            summary["agents"][agent] = {
                "has_memory": True,
                "last_modified": os.path.getmtime(str(memory_path)),
                "size": memory_path.stat().st_size,
                "extracted": len(filtered),
            }
            print(f"✅ {agent}: 数据已同步")
        else:
            summary["agents"][agent] = {"has_memory": False}
            print(f"⚠️ {agent}: 尚未初始化")

    out_dir = _system_dir() / "sync"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / "latest.json"
    out_file.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8-sig")

    extracted_items.sort(key=lambda x: int(x.get("importance") or 0), reverse=True)
    extracted_latest = out_dir / "extracted_latest.json"
    extracted_latest.write_text(json.dumps(extracted_items, ensure_ascii=False, indent=2), encoding="utf-8-sig")
    extracted_ts = out_dir / f"extracted_{_slug_ts()}.json"
    extracted_ts.write_text(json.dumps(extracted_items, ensure_ascii=False, indent=2), encoding="utf-8-sig")

    chroma_res = _upsert_chroma(extracted_items)
    chroma_file = out_dir / "chroma_latest.json"
    chroma_file.write_text(json.dumps(chroma_res, ensure_ascii=False, indent=2), encoding="utf-8-sig")

    print(f"✅ 同步完成: {out_file}")
    return summary


def memory_summary() -> str:
    """记忆总结 - 定期总结重要信息"""
    print(f"\n{'='*50}")
    print(f"🧠 记忆总结 - {_ts()}")
    print(f"{'='*50}")

    sync_dir = _system_dir() / "sync"
    extracted_latest = sync_dir / "extracted_latest.json"
    extracted: list[dict] = []
    if extracted_latest.exists():
        try:
            extracted = json.loads(_read_text(extracted_latest))
        except Exception:
            extracted = []

    extracted = [x for x in extracted if int((x.get("importance") or 0)) >= 6]
    top = sorted(extracted, key=lambda x: int(x.get("importance") or 0), reverse=True)[:SUMMARY_TOP_K]
    summary_notes: list[str] = []
    for it in top:
        meta = it.get("metadata") or {}
        agent = meta.get("agent") or "unknown"
        imp = meta.get("importance") or it.get("importance") or 0
        summary_notes.append(f"[{agent}] (重要性 {imp}) {it.get('content')}")

    out_dir = _system_dir() / "summary"
    out_dir.mkdir(parents=True, exist_ok=True)
    summary_file = out_dir / f"{_now().strftime('%Y%m%d')}.md"

    lines: list[str] = []
    lines.append(f"# 记忆总结 - {_ts()}")
    lines.append("")
    lines.append("## 重要片段（自动提取）")
    lines.append("")
    if summary_notes:
        for note in summary_notes:
            lines.append(f"- {note}")
    else:
        lines.append("- [暂无提取结果]")
    lines.append("")
    lines.append("## 待处理事项")
    lines.append("")
    lines.append("- [待填写]")
    lines.append("")

    summary_file.write_text("\n".join(lines), encoding="utf-8-sig")

    print(f"✅ 总结已保存到: {summary_file}")
    return str(summary_file)


def start_scheduler(run_once: bool = False) -> BackgroundScheduler | None:
    """启动定时任务调度器"""
    print("\n🚀 OpenClaw 定时任务系统启动\n")

    if run_once:
        heartbeat_check()
        sync_agent_data()
        memory_summary()
        intake_user_requirements()
        dispatch_tasks()
        return None

    scheduler = BackgroundScheduler()

    scheduler.add_job(
        heartbeat_check,
        "interval",
        hours=HEARTBEAT_INTERVAL_HOURS,
        id="heartbeat",
        name="心跳检查",
        max_instances=1,
        coalesce=True,
        misfire_grace_time=60 * 10,
    )

    scheduler.add_job(
        sync_agent_data,
        "interval",
        hours=SYNC_INTERVAL_HOURS,
        id="sync",
        name="数据同步",
        max_instances=1,
        coalesce=True,
        misfire_grace_time=60 * 30,
    )

    scheduler.add_job(
        memory_summary,
        "interval",
        hours=MEMORY_SUMMARY_INTERVAL_HOURS,
        id="summary",
        name="记忆总结",
        max_instances=1,
        coalesce=True,
        misfire_grace_time=60 * 60,
    )

    scheduler.add_job(
        dispatch_tasks,
        "interval",
        hours=DISPATCH_INTERVAL_HOURS,
        id="dispatch",
        name="任务派单",
        max_instances=1,
        coalesce=True,
        misfire_grace_time=60 * 10,
    )

    scheduler.add_job(
        intake_user_requirements,
        "interval",
        minutes=INTAKE_INTERVAL_MINUTES,
        id="intake",
        name="需求摄取",
        max_instances=1,
        coalesce=True,
        misfire_grace_time=60 * 5,
    )

    scheduler.start()
    print("✅ 调度器已启动")
    print(f"⏰ 心跳检查: 每 {HEARTBEAT_INTERVAL_HOURS} 小时")
    print(f"⏰ 数据同步: 每 {SYNC_INTERVAL_HOURS} 小时")
    print(f"⏰ 记忆总结: 每 {MEMORY_SUMMARY_INTERVAL_HOURS} 小时")

    heartbeat_check()
    sync_agent_data()
    intake_user_requirements()

    return scheduler


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()

    scheduler = start_scheduler(run_once=args.once)
    if args.once:
        return 0

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        if scheduler:
            scheduler.shutdown(wait=False)
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
