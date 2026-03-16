"""
OpenClaw 记忆系统
功能：短期记忆 + 长期记忆 + 语义检索
"""

import json
import os
from datetime import datetime
from pathlib import Path


# ============ 配置区 ============
MEMORY_DIR = "openclaw_agents/_system/memory"
SHORT_TERM_FILE = f"{MEMORY_DIR}/short_term.json"
LONG_TERM_FILE = f"{MEMORY_DIR}/long_term.json"


def _ensure_dir() -> None:
    os.makedirs(MEMORY_DIR, exist_ok=True)


def _write_json(path: str, data: dict) -> None:
    Path(path).write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8-sig")


def _read_json(path: str) -> dict:
    return json.loads(Path(path).read_text(encoding="utf-8-sig"))


def _now_ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


# ============ 短期记忆（当前会话） ============
class ShortTermMemory:
    """短期记忆 - 当前会话的上下文"""

    def __init__(self):
        self.conversations = []
        self.current_context = {}
        _ensure_dir()
        self.load()

    def load(self):
        """加载短期记忆"""
        if os.path.exists(SHORT_TERM_FILE):
            data = _read_json(SHORT_TERM_FILE)
            self.conversations = data.get("conversations", [])
            self.current_context = data.get("context", {})

    def save(self):
        """保存短期记忆"""
        data = {
            "conversations": self.conversations[-50:],
            "context": self.current_context,
            "last_update": _now_ts(),
        }
        _write_json(SHORT_TERM_FILE, data)

    def add_message(self, role, content):
        """添加消息"""
        self.conversations.append(
            {
                "role": role,
                "content": content,
                "timestamp": _now_ts(),
            }
        )
        self.save()

    def update_context(self, key, value):
        """更新上下文"""
        self.current_context[key] = value
        self.save()

    def get_context(self, key):
        """获取上下文"""
        return self.current_context.get(key)

    def get_recent_conversations(self, count=10):
        """获取最近会话"""
        return self.conversations[-count:]


# ============ 长期记忆（重要信息） ============
class LongTermMemory:
    """长期记忆 - 重要信息和经验"""

    def __init__(self):
        self.memories = []
        _ensure_dir()
        self.load()

    def load(self):
        """加载长期记忆"""
        if os.path.exists(LONG_TERM_FILE):
            data = _read_json(LONG_TERM_FILE)
            self.memories = data.get("memories", [])

    def save(self):
        """保存长期记忆"""
        data = {
            "memories": self.memories,
            "last_update": _now_ts(),
        }
        _write_json(LONG_TERM_FILE, data)

    def add_memory(self, category, content, importance=5):
        """添加记忆"""
        memory = {
            "category": category,
            "content": content,
            "importance": importance,
            "created_at": _now_ts(),
            "access_count": 0,
        }
        self.memories.append(memory)
        self.save()
        print(f"✅ 已添加记忆: {category}")

    def search_memories(self, keyword):
        """搜索记忆"""
        results = []
        needle = (keyword or "").lower()
        for memory in self.memories:
            hay_content = (memory.get("content") or "").lower()
            hay_category = (memory.get("category") or "").lower()
            if needle and (needle in hay_content or needle in hay_category):
                memory["access_count"] = int(memory.get("access_count") or 0) + 1
                results.append(memory)
        self.save()
        return results

    def get_memories_by_category(self, category):
        """按类别获取记忆"""
        return [m for m in self.memories if m.get("category") == category]

    def get_important_memories(self, min_importance=7):
        """获取重要记忆"""
        return sorted(
            [m for m in self.memories if int(m.get("importance") or 0) >= min_importance],
            key=lambda x: int(x.get("importance") or 0),
            reverse=True,
        )


# ============ 语义检索（可选增强） ============
class SemanticMemory:
    def __init__(self):
        self._enabled = False
        self._collection = None
        self._init()

    def _init(self):
        try:
            import chromadb  # noqa: F401
            from chromadb import PersistentClient
        except Exception:
            return

        persist_dir = Path(MEMORY_DIR) / "chroma"
        persist_dir.mkdir(parents=True, exist_ok=True)
        try:
            client = PersistentClient(path=str(persist_dir))
            self._collection = client.get_or_create_collection(name="openclaw_memories")
            self._enabled = True
        except Exception:
            self._enabled = False
            self._collection = None

    def upsert(self, memory_id: str, text: str, metadata: dict | None = None):
        if not self._enabled or not self._collection:
            return
        if not memory_id or not text:
            return
        try:
            self._collection.upsert(ids=[memory_id], documents=[text], metadatas=[metadata or {}])
        except Exception:
            return

    def query(self, text: str, top_k: int = 5):
        if not self._enabled or not self._collection:
            return []
        if not text:
            return []
        try:
            res = self._collection.query(query_texts=[text], n_results=top_k)
            ids = (res.get("ids") or [[]])[0]
            docs = (res.get("documents") or [[]])[0]
            metas = (res.get("metadatas") or [[]])[0]
            out = []
            for i in range(min(len(ids), len(docs))):
                out.append({"id": ids[i], "content": docs[i], "metadata": metas[i] if i < len(metas) else {}})
            return out
        except Exception:
            return []


# ============ 全局实例 ============
short_memory = ShortTermMemory()
long_memory = LongTermMemory()
semantic_memory = SemanticMemory()


# ============ 便捷函数 ============
def remember(agent_name, memory_type, content, importance=5):
    """快速添加记忆"""
    category = f"{agent_name}_{memory_type}"
    long_memory.add_memory(category=category, content=content, importance=importance)
    memory_id = f"{category}:{int(datetime.now().timestamp())}"
    semantic_memory.upsert(memory_id=memory_id, text=content, metadata={"category": category, "importance": importance})


def recall(keyword):
    """快速检索记忆"""
    return long_memory.search_memories(keyword)


def semantic_recall(text, top_k: int = 5):
    return semantic_memory.query(text=text, top_k=top_k)


def update_context(agent_name, key, value):
    """更新当前上下文"""
    short_memory.update_context(f"{agent_name}_{key}", value)


def get_context(agent_name):
    """获取某专家的上下文"""
    return {
        k.replace(f"{agent_name}_", ""): v
        for k, v in short_memory.current_context.items()
        if k.startswith(agent_name)
    }


if __name__ == "__main__":
    print("🧠 记忆系统测试\n")

    remember("1_全能助理", "偏好", "用户喜欢简洁直接的沟通", importance=8)
    remember("2_编程专家", "技术", "擅长Python和React", importance=7)

    results = recall("偏好")
    print(f"\n🔍 搜索'偏好'结果: {len(results)}条")
    for r in results:
        print(f"  - {r['content']}")

    sem = semantic_recall("沟通偏好", top_k=5)
    print(f"\n🔎 语义检索结果: {len(sem)}条")
    for r in sem:
        print(f"  - {r.get('content')}")
