"""
API Pool — 聚合 API 自动切换模块（GUI 版）

启动: python api_pool_server.py
访问: http://localhost:5200
"""

import os
import json
import time
import random
import threading
import sqlite3
import secrets
import hashlib
import hmac
import http.cookies
import urllib.request
import urllib.error
from dataclasses import dataclass, field
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, unquote
from concurrent.futures import ThreadPoolExecutor, as_completed
import queue
from datetime import datetime, timedelta
from collections import deque

LATENCY_OK_MAX = 2000     
LATENCY_SLOW_MAX = 5000   
HEALTH_CHECK_INTERVAL = 600
HEALTH_STAGGER_MIN = 1.0   # 同站相邻两次探测的最小间隔（秒）
HEALTH_STAGGER_MAX = 5.0   # 同站相邻两次探测的最大间隔（秒）

class LogManager:
    def __init__(self, max_history=300):
        self.history = []
        self.lock = threading.Lock()
        self.max_history = max_history
        self._counter = 0

    def log(self, level, msg):
        ts = time.time()
        time_str = datetime.fromtimestamp(ts).strftime('%H:%M:%S')
        with self.lock:
            self._counter += 1
            entry = {"id": self._counter, "time": time_str, "level": level, "msg": msg, "timestamp": ts}
            self.history.append(entry)
            if len(self.history) > self.max_history:
                self.history.pop(0)

    def get_logs_since(self, last_id):
        with self.lock:
            return [log for log in self.history if log["id"] > last_id]

    def clear_logs(self):
        with self.lock:
            self.history.clear()

sys_logger = LogManager()
def sys_log(msg, level="INFO"):
    sys_logger.log(level, msg)
    print(f"[{time.strftime('%H:%M:%S')}] [{level}] {msg}")

# ============================================================
#  SQLite 连接池（线程本地 + WAL 模式 + 单线程写队列）
# ============================================================

class SQLitePool:
    """
    每个线程复用同一个 SQLite 连接（thread-local），
    WAL 模式允许读写并发，写操作通过单一后台线程串行执行消除锁争用。
    """
    def __init__(self, db_path, timeout=5):
        self.db_path = db_path
        self.timeout = timeout
        self._local = threading.local()          # 读连接：每线程独立
        self._write_queue = queue.Queue()        # 写队列：单线程消费
        self._writer = threading.Thread(target=self._write_worker, daemon=True)
        self._writer.start()

    def _make_conn(self):
        conn = sqlite3.connect(self.db_path, timeout=self.timeout, check_same_thread=False)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA cache_size=-8000")  # 8MB 页缓存
        return conn

    def _write_worker(self):
        """单一后台线程：顺序消费写队列，持久复用同一连接"""
        conn = self._make_conn()
        while True:
            try:
                sql, params, done_ev = self._write_queue.get()
                conn.execute(sql, params)
                conn.commit()
            except Exception as e:
                try: conn.rollback()
                except Exception: pass
                try:
                    conn = self._make_conn()   # 连接损坏时重建
                except Exception: pass
                sys_log(f"SQLitePool 写入失败 ({self.db_path}): {e}", "WARN")
            finally:
                if done_ev:
                    done_ev.set()

    def write(self, sql, params=(), wait=False):
        """异步写入；wait=True 时阻塞直到写完"""
        ev = threading.Event() if wait else None
        self._write_queue.put((sql, params, ev))
        if ev:
            ev.wait(timeout=10)

    def read_conn(self):
        """返回当前线程的读连接，不存在则新建"""
        c = getattr(self._local, 'conn', None)
        if c is None:
            c = self._make_conn()
            self._local.conn = c
        return c

    def query(self, sql, params=()):
        """执行查询，返回 cursor"""
        return self.read_conn().execute(sql, params)


class TokenTracker:
    def __init__(self, db_path="token_stats.db"):
        self.db_path = db_path
        self._pool = SQLitePool(db_path)
        self._init_db()

    def _init_db(self):
        # 初始化表结构
        self._pool.write("""
            CREATE TABLE IF NOT EXISTS token_usage (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                model TEXT,
                prompt_tokens INTEGER,
                completion_tokens INTEGER,
                total_tokens INTEGER
            )
        """, wait=True)
        self._pool.write("CREATE INDEX IF NOT EXISTS idx_timestamp ON token_usage(timestamp)", wait=True)
        # 检查列是否已存在再添加，避免重复 ALTER 警告
        existing = {r[1] for r in self._pool.query("PRAGMA table_info(token_usage)").fetchall()}
        if "endpoint_name" not in existing:
            self._pool.write("ALTER TABLE token_usage ADD COLUMN endpoint_name TEXT DEFAULT ''", wait=True)
        if "cached_tokens" not in existing:
            self._pool.write("ALTER TABLE token_usage ADD COLUMN cached_tokens INTEGER DEFAULT 0", wait=True)

    def add_usage(self, endpoint_name, model, prompt_tokens, completion_tokens, total_tokens, cached_tokens=0):
        # 异步写入：直接推入写队列，不再另起线程
        self._pool.write(
            "INSERT INTO token_usage (endpoint_name, model, prompt_tokens, completion_tokens, total_tokens, cached_tokens) VALUES (?, ?, ?, ?, ?, ?)",
            (endpoint_name, model, prompt_tokens, completion_tokens, total_tokens, cached_tokens),
        )

    def get_today_usage_by_endpoint(self, endpoint_name):
        try:
            row = self._pool.query(
                "SELECT SUM(total_tokens) FROM token_usage WHERE endpoint_name = ? AND timestamp >= datetime(date('now', 'localtime'), 'utc')",
                (endpoint_name,)
            ).fetchone()
            return row[0] or 0
        except Exception:
            return 0

    def rename_endpoint(self, old_name: str, new_name: str):
        try:
            self._pool.write(
                "UPDATE token_usage SET endpoint_name = ? WHERE endpoint_name = ?",
                (new_name, old_name), wait=True
            )
        except Exception as e:
            sys_log(f"重命名端点统计数据失败: {e}", "WARN")

    def get_stats(self, endpoint_filter=None):
        conn = self._pool.read_conn()  # 复用当前线程的读连接
        cursor = conn.cursor()
        ep_cond = " AND endpoint_name = ?" if endpoint_filter and endpoint_filter != "all" else ""
        params = (endpoint_filter,) if (endpoint_filter and endpoint_filter != "all") else ()
            
        cursor.execute(f"SELECT SUM(total_tokens), SUM(cached_tokens), SUM(prompt_tokens), COUNT(*) FROM token_usage WHERE timestamp >= datetime(date('now', 'localtime'), 'utc'){ep_cond}", params)
        today_row = cursor.fetchone()
        today = today_row[0] or 0
        today_cached = today_row[1] or 0
        today_prompt = today_row[2] or 0
        today_calls = today_row[3] or 0
        today_cache_hit_rate = round(today_cached / today_prompt * 100, 1) if today_prompt > 0 else 0
        
        cursor.execute(f"SELECT SUM(total_tokens) FROM token_usage WHERE timestamp >= datetime(date('now', '-2 days', 'localtime'), 'utc'){ep_cond}", params)
        last_3_days = cursor.fetchone()[0] or 0
        cursor.execute(f"SELECT SUM(total_tokens) FROM token_usage WHERE timestamp >= datetime(date('now', '-6 days', 'localtime'), 'utc'){ep_cond}", params)
        last_7_days = cursor.fetchone()[0] or 0
        cursor.execute(f"SELECT SUM(total_tokens), SUM(cached_tokens), SUM(prompt_tokens), COUNT(*) FROM token_usage WHERE timestamp >= datetime(date('now', '-29 days', 'localtime'), 'utc'){ep_cond}", params)
        month_row = cursor.fetchone()
        last_30_days = month_row[0] or 0
        month_cached = month_row[1] or 0
        month_prompt = month_row[2] or 0
        month_calls = month_row[3] or 0
        month_cache_hit_rate = round(month_cached / month_prompt * 100, 1) if month_prompt > 0 else 0
        
        cursor.execute(f"""
            SELECT date(timestamp, 'localtime') as d, SUM(total_tokens), SUM(prompt_tokens), SUM(cached_tokens), SUM(completion_tokens)
            FROM token_usage
            WHERE timestamp >= datetime(date('now', '-13 days', 'localtime'), 'utc'){ep_cond}
            GROUP BY d
        """, params)
        raw_trend = {r[0]: {"total": r[1] or 0, "prompt": r[2] or 0, "cached": r[3] or 0, "completion": r[4] or 0} for r in cursor.fetchall()}
        trend_14d = []
        now = datetime.now()
        for i in range(13, -1, -1):
            d_str = (now - timedelta(days=i)).strftime('%Y-%m-%d')
            data = raw_trend.get(d_str, {"total": 0, "prompt": 0, "cached": 0, "completion": 0})
            trend_14d.append({"date": d_str, "tokens": data["total"], "prompt": data["prompt"], "cached": data["cached"], "completion": data["completion"]})
            
        cursor.execute(f"""
            SELECT strftime('%H', datetime(timestamp, 'localtime')) as h, SUM(total_tokens), COUNT(*), SUM(prompt_tokens), SUM(cached_tokens)
            FROM token_usage
            WHERE timestamp >= datetime(date('now', 'localtime'), 'utc'){ep_cond}
            GROUP BY h
        """, params)
        raw_hourly = {r[0]: (r[1], r[2], r[3] or 0, r[4] or 0) for r in cursor.fetchall()}
        trend_today_hourly = []
        for i in range(24):
            h_str = f"{i:02d}"
            val = raw_hourly.get(h_str, (0, 0, 0, 0))
            missed = max(0, val[2] - val[3])
            trend_today_hourly.append({"date": f"{h_str}:00", "tokens": val[0] or 0, "calls": val[1] or 0, "missed": missed})
            
        cursor.execute(f"""
            SELECT endpoint_name, model, SUM(total_tokens), COUNT(*), SUM(prompt_tokens), SUM(cached_tokens)
            FROM token_usage
            WHERE timestamp >= datetime(date('now', 'localtime'), 'utc'){ep_cond}
            GROUP BY endpoint_name, model
            ORDER BY SUM(total_tokens) DESC
        """, params)
        today_endpoints = [{"endpoint": r[0] or "未知端点", "model": r[1], "tokens": r[2] or 0, "calls": r[3] or 0, "cache_hit_rate": round((r[5] or 0)/(r[4] or 1)*100, 1) if (r[4] or 0) > 0 else 0} for r in cursor.fetchall()]
        
        cursor.execute(f"""
            SELECT endpoint_name, model, SUM(total_tokens), COUNT(*), SUM(prompt_tokens), SUM(cached_tokens)
            FROM token_usage
            WHERE strftime('%Y-%m', timestamp, 'localtime') = strftime('%Y-%m', 'now', 'localtime'){ep_cond}
            GROUP BY endpoint_name, model
            ORDER BY SUM(total_tokens) DESC
        """, params)
        month_endpoints = [{"endpoint": r[0] or "未知端点", "model": r[1], "tokens": r[2] or 0, "calls": r[3] or 0, "cache_hit_rate": round((r[5] or 0)/(r[4] or 1)*100, 1) if (r[4] or 0) > 0 else 0} for r in cursor.fetchall()]

        def _model_rows(sql):
            cursor.execute(sql, params)
            rows = []
            for r in cursor.fetchall():
                prompt = r[3] or 0
                cached = r[4] or 0
                completion = r[5] or 0
                tokens = r[1] or 0
                rows.append({
                    "model": (r[0] or "未知模型"),
                    "tokens": tokens,
                    "calls": r[2] or 0,
                    "prompt_tokens": prompt,
                    "cached_tokens": cached,
                    "completion_tokens": completion,
                    "cache_hit_rate": round(cached / prompt * 100, 1) if prompt > 0 else 0,
                })
            total_tokens = sum(x["tokens"] for x in rows) or 0
            for x in rows:
                x["share"] = round(x["tokens"] / total_tokens * 100, 1) if total_tokens > 0 else 0
            return rows

        today_models = _model_rows(f"""
            SELECT COALESCE(NULLIF(TRIM(model), ''), '未知模型') as m,
                   SUM(total_tokens), COUNT(*), SUM(prompt_tokens), SUM(cached_tokens), SUM(completion_tokens)
            FROM token_usage
            WHERE timestamp >= datetime(date('now', 'localtime'), 'utc'){ep_cond}
            GROUP BY m
            ORDER BY SUM(total_tokens) DESC
        """)
        month_models = _model_rows(f"""
            SELECT COALESCE(NULLIF(TRIM(model), ''), '未知模型') as m,
                   SUM(total_tokens), COUNT(*), SUM(prompt_tokens), SUM(cached_tokens), SUM(completion_tokens)
            FROM token_usage
            WHERE strftime('%Y-%m', timestamp, 'localtime') = strftime('%Y-%m', 'now', 'localtime'){ep_cond}
            GROUP BY m
            ORDER BY SUM(total_tokens) DESC
        """)

        cursor.execute("SELECT DISTINCT endpoint_name FROM token_usage WHERE endpoint_name IS NOT NULL")
        all_endpoints_list = [r[0] for r in cursor.fetchall()]

        return {
            "today": today,
            "today_cached": today_cached,
            "today_missed": max(0, today_prompt - today_cached),
            "today_calls": today_calls,
            "today_cache_hit_rate": today_cache_hit_rate,
            "last_3_days": last_3_days,
            "last_7_days": last_7_days,
            "last_30_days": last_30_days,
            "month_cached": month_cached,
            "month_missed": max(0, month_prompt - month_cached),
            "month_calls": month_calls,
            "month_cache_hit_rate": month_cache_hit_rate,
            "trend_14d": trend_14d,
            "trend_today_hourly": trend_today_hourly,
            "today_endpoints": today_endpoints,
            "month_endpoints": month_endpoints,
            "today_models": today_models,
            "month_models": month_models,
            "all_endpoints_list": all_endpoints_list
        }

    def export_csv(self):
        import csv
        import io
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(["ID", "Timestamp", "Endpoint", "Model", "Prompt Tokens", "Completion Tokens", "Total Tokens", "Cached Tokens"])
        cursor = self._pool.query("SELECT id, timestamp, endpoint_name, model, prompt_tokens, completion_tokens, total_tokens, cached_tokens FROM token_usage ORDER BY id DESC")
        for row in cursor.fetchall():
            writer.writerow(row)
        return output.getvalue()

    def clear_data(self):
        self._pool.write("DELETE FROM token_usage", wait=True)

token_tracker = TokenTracker()

class ChatLogger:
    def __init__(self, db_path="chat_logs.db", max_records=10000, max_text_length=2000, cleanup_days=30):
        self.db_path = db_path
        self._lock = threading.Lock()
        self._pool = SQLitePool(db_path)
        self.max_records = max_records
        self.max_text_length = max_text_length
        self.cleanup_days = cleanup_days
        self._init_db()
        self._startup_cleanup()

    def _init_db(self):
        with self._lock:
            self._pool.write('''CREATE TABLE IF NOT EXISTS chat_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                endpoint_name TEXT,
                model TEXT,
                prompt TEXT,
                completion TEXT,
                total_tokens INTEGER,
                latency_ms INTEGER
            )''', wait=True)
            self._pool.write('CREATE INDEX IF NOT EXISTS idx_timestamp ON chat_logs(timestamp)', wait=True)
            self._pool.write('CREATE INDEX IF NOT EXISTS idx_endpoint ON chat_logs(endpoint_name)', wait=True)

    def _truncate_text(self, text, max_length=None):
        """截断文本到指定长度，保留摘要"""
        if max_length is None:
            max_length = self.max_text_length
        if not text or len(text) <= max_length:
            return text
        # 保留前面大部分和结尾一小部分
        keep_start = int(max_length * 0.8)
        keep_end = max_length - keep_start - 20
        return text[:keep_start] + "\n...[截断]...\n" + text[-keep_end:] if keep_end > 0 else text[:keep_start] + "\n...[截断]"

    def _startup_cleanup(self):
        """启动时自动清理旧数据"""
        def _cleanup():
            try:
                with self._lock:
                    conn = self._pool.read_conn()
                    # 1. 删除超过保留天数的记录
                    self._pool.write("DELETE FROM chat_logs WHERE timestamp < datetime('now', ?)", (f'-{self.cleanup_days} days',), wait=True)
                    # 2. 如果记录数超过限制，删除最旧的
                    row = conn.execute("SELECT COUNT(*) FROM chat_logs").fetchone()
                    total = row[0]
                    deleted_excess = 0
                    if total > self.max_records:
                        self._pool.write("DELETE FROM chat_logs WHERE id IN (SELECT id FROM chat_logs ORDER BY timestamp ASC LIMIT ?)", (total - self.max_records,), wait=True)
                        deleted_excess = total - self.max_records
                    # 3. 执行 VACUUM 压缩数据库（在写线程中执行）
                    self._pool.write("VACUUM", wait=True)
                    if deleted_excess > 0:
                        sys_log(f"数据库自动清理: 删除 {deleted_excess} 条超限记录", "INFO")
            except Exception as e:
                sys_log(f"数据库启动清理失败: {e}", "WARN")
        threading.Thread(target=_cleanup, daemon=True).start()

    def add_log(self, endpoint_name, model, prompt, completion, total_tokens, latency_ms):
        # 截断过长的文本，异步写入队列
        truncated_prompt = self._truncate_text(prompt)
        truncated_completion = self._truncate_text(completion)
        self._pool.write(
            "INSERT INTO chat_logs (endpoint_name, model, prompt, completion, total_tokens, latency_ms) VALUES (?, ?, ?, ?, ?, ?)",
            (endpoint_name, model, truncated_prompt, truncated_completion, total_tokens, latency_ms)
        )

    def get_logs(self, limit=50, offset=0):
        try:
            conn = self._pool.read_conn()
            rows = conn.execute(
                "SELECT id, datetime(timestamp, 'localtime'), endpoint_name, model, prompt, completion, total_tokens, latency_ms FROM chat_logs ORDER BY id DESC LIMIT ? OFFSET ?",
                (limit, offset)
            ).fetchall()
            total = conn.execute("SELECT COUNT(*) FROM chat_logs").fetchone()[0]
            return {
                "total": total,
                "logs": [
                    {
                        "id": r[0], "timestamp": r[1], "endpoint_name": r[2],
                        "model": r[3], "prompt": r[4], "completion": r[5],
                        "total_tokens": r[6], "latency_ms": r[7]
                    } for r in rows
                ]
            }
        except Exception as e:
            return {"total": 0, "logs": [], "error": str(e)}

    def clear_logs(self):
        try:
            self._pool.write("DELETE FROM chat_logs", wait=True)
        except Exception:
            pass

chat_logger = ChatLogger()

def extract_prompt_text(payload):
    try:
        messages = payload.get("messages", [])
        output = []
        for m in messages:
            role = m.get("role", "unknown")
            content = m.get("content", "")
            if isinstance(content, str):
                output.append(f"[{role.upper()}]\n{content}")
            elif isinstance(content, list):
                parts = []
                for part in content:
                    ptype = part.get("type", "")
                    if ptype == "text":
                        parts.append(part.get("text", ""))
                    elif ptype == "image_url":
                        parts.append("[Base64 Image Omitted]")
                output.append(f"[{role.upper()}]\n" + "\n".join(parts))
        return "\n\n".join(output)
    except Exception:
        return str(payload)[:2000]

# ============================================================
#  数据结构
# ============================================================

@dataclass
class Endpoint:
    id: str = ""
    name: str = "unnamed"
    base_url: str = ""
    api_key: str = ""
    model: str = "gpt-4o-mini"
    public_model: str = ""
    upstream_model: str = ""
    priority: int = 999
    timeout: int = 60
    max_retries: int = 1
    enabled: bool = True
    cooldown_minutes: int = 5
    daily_limit: int = 0
    rpm_limit: int = 0
    use_proxy: bool = True
    protocol: str = "openai"
    extra_headers: dict = field(default_factory=dict)
    is_vision: bool = True
    in_pool: bool = True
    billing_mode: str = "subscription"

    _fail_count: int = field(default=0, repr=False)
    _req_timestamps: deque = field(default_factory=deque, repr=False)
    _rpm_lock: threading.Lock = field(default_factory=threading.Lock, repr=False)
    _last_error: str = field(default="", repr=False)
    _last_error_ts: float = field(default=0, repr=False)
    _last_success_ts: float = field(default=0, repr=False)
    _total_calls: int = field(default=0, repr=False)
    _total_failures: int = field(default=0, repr=False)
    _cooldown_until: float = field(default=0, repr=False)
    
    _today_used: int = field(default=0, repr=False)
    _today_date: str = field(default="", repr=False)
    health_mode: str = field(default="chat")

    _transient_count: int = field(default=0, repr=False)
    _transient_window_start: float = field(default=0, repr=False)
    _health: str = field(default="unknown", repr=False)
    _health_latency_ms: int = field(default=-1, repr=False)
    _health_last_check: float = field(default=0, repr=False)
    _health_error: str = field(default="", repr=False)

    # 性能可视化：实际请求延迟样本（最多保留50个）
    _latency_samples: deque = field(default_factory=lambda: deque(maxlen=50), repr=False)


def station_key(base_url):
    """中转站标识：按 base_url 的域名（host）归组，同一服务商的多个端点视为一个中转站。"""
    try:
        url = base_url if "://" in (base_url or "") else "https://" + (base_url or "")
        host = urlparse(url).netloc.lower().strip()
        return host or (base_url or "").strip().lower()
    except Exception:
        return (base_url or "").strip().lower()


class AllEndpointsFailed(Exception):
    def __init__(self, errors: list):
        self.errors = errors
        super().__init__(f"All endpoints failed: {errors}")


class ModelRouteError(Exception):
    def __init__(self, model, message, status=503, error_type="model_unavailable"):
        self.model = model
        self.status = status
        self.error_type = error_type
        super().__init__(message)


# ============================================================
#  API Pool
# ============================================================

class APIPool:
    def __init__(self, endpoints=None, default_payload=None):
        self._lock = threading.Lock()
        self.default_payload = default_payload or {}
        self._endpoints: list[Endpoint] = []
        self._current_idx = 0
        self._manual_override_id = None
        self._last_reasoning_content = None
        # 全局模型映射: {对外别名: 池内真实模型}，客户端用别名请求时自动路由
        self.model_aliases = {}
        # 中转站级设置: {station_key: {"health_paused": bool, "connect_paused": bool}}
        self.station_settings = {}
        # 健康检测结果缓存（5秒TTL，减少重复计算开销）
        self._list_cache = None
        self._list_cache_ts = 0
        self._chain_cache = None
        self._chain_cache_ts = 0
        if endpoints:
            for ep in endpoints:
                self.add_endpoint(ep)

    def add_endpoint(self, ep):
        if isinstance(ep, dict):
            ep = Endpoint(**{k: v for k, v in ep.items() if k in Endpoint.__dataclass_fields__})
        self._normalize_model_names(ep)
        if not ep.id:
            import uuid
            ep.id = str(uuid.uuid4())
        ep._today_date = datetime.now().strftime("%Y-%m-%d")
        ep._today_used = token_tracker.get_today_usage_by_endpoint(ep.name)
        with self._lock:
            self._endpoints.append(ep)
            self._endpoints.sort(key=lambda e: e.priority)
            self._current_idx = 0

    @staticmethod
    def _normalize_model_names(ep):
        # `model` remains the backward-compatible, public routing name.
        public_model = str(getattr(ep, "public_model", "") or ep.model or "").strip()
        upstream_model = str(getattr(ep, "upstream_model", "") or ep.model or public_model).strip()
        ep.model = public_model
        ep.public_model = public_model
        ep.upstream_model = upstream_model

    def remove_endpoint(self, ep_id):
        with self._lock:
            self._endpoints = [e for e in self._endpoints if e.id != ep_id]
            self._current_idx = 0
            if self._manual_override_id == ep_id:
                self._manual_override_id = None

    def set_enabled(self, ep_id, enabled):
        with self._lock:
            for ep in self._endpoints:
                if ep.id == ep_id:
                    ep.enabled = enabled
                    break
            if not enabled and self._manual_override_id == ep_id:
                self._manual_override_id = None

    def set_pool(self, ep_id, in_pool):
        with self._lock:
            for ep in self._endpoints:
                if ep.id == ep_id:
                    ep.in_pool = in_pool
                    break
            if not in_pool and self._manual_override_id == ep_id:
                self._manual_override_id = None

    def switch_to_endpoint(self, ep_id):
        with self._lock:
            for ep in self._endpoints:
                if ep.id == ep_id and ep.enabled and ep.in_pool:
                    self._manual_override_id = ep_id
                    return True
        return False

    # ================= 中转站（按 base_url 域名分组） =================
    def _station_conf(self, key):
        return self.station_settings.get(key, {})

    def is_station_health_paused(self, ep):
        return bool(self._station_conf(station_key(ep.base_url)).get("health_paused"))

    def is_station_connect_paused(self, ep):
        return bool(self._station_conf(station_key(ep.base_url)).get("connect_paused"))

    def set_station_setting(self, key, health_paused=None, connect_paused=None):
        with self._lock:
            conf = self.station_settings.setdefault(key, {})
            if health_paused is not None:
                conf["health_paused"] = bool(health_paused)
            if connect_paused is not None:
                conf["connect_paused"] = bool(connect_paused)
            if not conf.get("health_paused") and not conf.get("connect_paused"):
                self.station_settings.pop(key, None)
            # 若当前手动指定的端点属于被停用连接的站点，清除手动覆盖
            if connect_paused and self._manual_override_id:
                for ep in self._endpoints:
                    if ep.id == self._manual_override_id and station_key(ep.base_url) == key:
                        self._manual_override_id = None
                        break

    def list_stations(self):
        now = time.time()
        groups = {}
        with self._lock:
            endpoints = list(self._endpoints)
            settings = {k: dict(v) for k, v in self.station_settings.items()}
        for ep in endpoints:
            key = station_key(ep.base_url)
            g = groups.setdefault(key, {
                "key": key,
                "names": [],
                "base_urls": [],
                "total": 0, "enabled": 0, "in_pool": 0,
                "healthy": 0, "bad": 0, "in_cooldown": 0,
                "total_calls": 0,
                "_conf_sets": {f: set() for f in ("name", "api_key", "timeout", "max_retries", "cooldown_minutes", "daily_limit", "rpm_limit", "use_proxy", "protocol", "health_mode", "billing_mode")},
            })
            if ep.name and ep.name not in g["names"]:
                g["names"].append(ep.name)
            if ep.base_url and ep.base_url not in g["base_urls"]:
                g["base_urls"].append(ep.base_url)
            g["total"] += 1
            if ep.enabled: g["enabled"] += 1
            if ep.in_pool: g["in_pool"] += 1
            if ep._health == "ok": g["healthy"] += 1
            elif ep._health == "bad": g["bad"] += 1
            if ep._cooldown_until > now: g["in_cooldown"] += 1
            g["total_calls"] += ep._total_calls
            for f in g["_conf_sets"]:
                g["_conf_sets"][f].add(getattr(ep, f, None))
        result = []
        for key, g in groups.items():
            conf_sets = g.pop("_conf_sets")
            # 各端点一致的配置返回具体值，不一致的返回 None（前端显示"多个值"）
            g["config"] = {f: (next(iter(vals)) if len(vals) == 1 else None) for f, vals in conf_sets.items()}
            ak = g["config"].get("api_key")
            g["config"]["api_key_hint"] = (ak[:8] + "***" if ak and len(ak) > 8 else ("***" if ak else None)) if ak is not None else None
            g["config"].pop("api_key", None)
            conf = settings.get(key, {})
            g["health_paused"] = bool(conf.get("health_paused"))
            g["connect_paused"] = bool(conf.get("connect_paused"))
            result.append(g)
        result.sort(key=lambda x: (-x["total"], x["key"]))
        return result

    def update_endpoint(self, ep_id, updates: dict):
        with self._lock:
            for ep in self._endpoints:
                if ep.id == ep_id:
                    updates = dict(updates)
                    if "model" in updates and "public_model" not in updates and "upstream_model" not in updates:
                        # Preserve the old one-field API contract for existing automation.
                        updates["public_model"] = updates["model"]
                        updates["upstream_model"] = updates["model"]
                    for k, v in updates.items():
                        if hasattr(ep, k) and not k.startswith("_") and k != "id":
                            setattr(ep, k, v)
                    self._normalize_model_names(ep)
                    self._endpoints.sort(key=lambda e: e.priority)
                    break

    def list_endpoints(self):
        now = time.time()
        # 5秒缓存，减少频繁轮询时的锁争用
        if self._list_cache is not None and (now - self._list_cache_ts) < 5.0:
            return self._list_cache
        with self._lock:
            active = [ep for ep in self._endpoints if ep.enabled]
            current_ep = active[self._current_idx] if active and self._current_idx < len(active) else None
            result = [self._ep_to_dict(ep, ep is current_ep, now) for ep in self._endpoints]
        self._list_cache = result
        self._list_cache_ts = now
        return result

    def _invalidate_cache(self):
        """端点状态变化时清除缓存"""
        self._list_cache = None
        self._chain_cache = None

    def _ep_to_dict(self, ep, is_current, now):
        return {
            "id": ep.id,
            "name": ep.name,
            "base_url": ep.base_url,
            "api_key": ep.api_key[:8] + "***" if len(ep.api_key) > 8 else "***",
            "api_key_full": ep.api_key,
            "model": ep.model,
            "public_model": ep.model,
            "upstream_model": ep.upstream_model,
            "priority": ep.priority,
            "timeout": ep.timeout,
            "max_retries": ep.max_retries,
            "enabled": ep.enabled,
            "cooldown_minutes": ep.cooldown_minutes,
            "daily_limit": ep.daily_limit,
            "today_used": ep._today_used,
            "rpm_limit": ep.rpm_limit,
            "use_proxy": ep.use_proxy,
            "protocol": ep.protocol,
            "health_mode": ep.health_mode,
            "is_vision": ep.is_vision,
            "in_pool": ep.in_pool,
            "billing_mode": ep.billing_mode,
            "is_rpm_limited": self._is_rpm_limited(ep),
            "fail_count": ep._fail_count,
            "last_error": ep._last_error,
            "last_success": ep._last_success_ts,
            "total_calls": ep._total_calls,
            "total_failures": ep._total_failures,
            "is_current": is_current,
            "in_cooldown": ep._cooldown_until > now,
            "cooldown_remaining": max(0, int(ep._cooldown_until - now)),
            "cooldown_until": ep._cooldown_until,
            "health": ep._health,
            "health_latency_ms": ep._health_latency_ms,
            "health_last_check": ep._health_last_check,
            "health_error": ep._health_error,
            "station": station_key(ep.base_url),
            "station_health_paused": self.is_station_health_paused(ep),
            "station_connect_paused": self.is_station_connect_paused(ep),
            # 性能可视化：成功率和平均延迟
            "success_rate": round(ep._total_calls / max(1, ep._total_calls + ep._total_failures) * 100, 1),
            "avg_latency_ms": int(sum(ep._latency_samples) / len(ep._latency_samples)) if ep._latency_samples else None,
        }

    def get_active_chain(self):
        now = time.time()
        # 5秒缓存
        if self._chain_cache is not None and (now - self._chain_cache_ts) < 5.0:
            return self._chain_cache
        health_rank = {"ok": 0, "slow": 1, "testing": 2, "unknown": 3, "bad": 4}
        with self._lock:
            active = list(self._active_endpoints())
            current_ep = None
            if self._manual_override_id:
                current_ep = next((ep for ep in active if ep.id == self._manual_override_id), None)
            if current_ep is None:
                current_ep = active[self._current_idx] if active and self._current_idx < len(active) else None
            active.sort(key=lambda ep: (
                1 if ep._cooldown_until > now else 0,
                health_rank.get(ep._health or "unknown", 3),
                ep.priority if ep.priority is not None else 999,
                ep._health_latency_ms if ep._health_latency_ms is not None and ep._health_latency_ms >= 0 else 10**9,
                ep.name or "",
            ))
            result = [
                {
                    "name": ep.name,
                    "model": ep.model,
                    "upstream_model": ep.upstream_model,
                    "priority": ep.priority,
                    "is_current": ep is current_ep,
                    "fail_count": ep._fail_count,
                    "in_cooldown": ep._cooldown_until > now,
                    "cooldown_remaining": max(0, int(ep._cooldown_until - now)),
                    "daily_limit": ep.daily_limit,
                    "today_used": ep._today_used,
                    "rpm_limit": ep.rpm_limit,
                    "use_proxy": ep.use_proxy,
                    "is_rpm_limited": self._is_rpm_limited(ep),
                    "is_vision": ep.is_vision,
                    "health": ep._health,
                    "health_latency_ms": ep._health_latency_ms,
                    "health_error": ep._health_error,
                }
                for ep in active
            ]
        self._chain_cache = result
        self._chain_cache_ts = now
        return result

    def reset(self):
        with self._lock:
            for ep in self._endpoints:
                ep._fail_count = 0
                ep._last_error = ""
                ep._last_error_ts = 0
                ep._cooldown_until = 0
                ep._transient_count = 0
                ep._transient_window_start = 0
                with ep._rpm_lock:
                    ep._req_timestamps.clear()
            self._current_idx = 0
            self._manual_override_id = None

    def _check_one_health(self, ep):
        if self.is_station_health_paused(ep):
            return ep.id, "unknown", -1, "中转站已暂停测活"
        if ep.health_mode == "none":
            return ep.id, "unknown", -1, "已禁用健康检测"
            
        if ep.health_mode == "models":
            t0 = time.time()
            try:
                models = self.fetch_models(ep.base_url, ep.api_key, timeout=10, use_proxy=ep.use_proxy, protocol=ep.protocol)
                latency = int((time.time() - t0) * 1000)
                if models:
                    return ep.id, "ok", latency, ""
                else:
                    return ep.id, "bad", latency, "获取模型列表失败"
            except Exception as e:
                return ep.id, "bad", int((time.time() - t0) * 1000), f"Models接口错误: {e}"[:100]
                
        payload = {"model": ep.model, "messages": [{"role": "user", "content": "ping"}], "max_tokens": 3}
        
        # Attempt 1
        t0 = time.time()
        reply, err = self._try_endpoint(ep, payload, timeout=10, log_usage=False, force_no_retry=True)
        latency = int((time.time() - t0) * 1000)
        
        if reply is not None and latency <= LATENCY_OK_MAX:
            return ep.id, "ok", latency, ""
            
        # Evaluate if we should retry
        err_str = err[:100] if err else ""
        hard_errors = ["auth error", "400", "401", "403", "404", "429"]
        if any(code in err_str for code in hard_errors):
            return ep.id, "bad", latency, err_str
            
        # Attempt 2 (Retry for cold start or transient glitch)
        t1 = time.time()
        reply2, err2 = self._try_endpoint(ep, payload, timeout=10, log_usage=False, force_no_retry=True)
        latency2 = int((time.time() - t1) * 1000)
        
        if reply2 is not None and latency2 <= LATENCY_OK_MAX:
            return ep.id, "ok", latency2, ""
            
        # If retry also fails or isn't fast enough, return the original attempt's status
        if reply is not None:
            if latency <= LATENCY_SLOW_MAX:
                return ep.id, "slow", latency, ""
            else:
                return ep.id, "bad", latency, f"延迟过高: {latency}ms"
        else:
            return ep.id, "bad", latency, err_str or "未知错误"

    def _has_images(self, messages):
        if not messages: return False
        for m in messages:
            content = m.get("content")
            if isinstance(content, list):
                for c in content:
                    if c.get("type") == "image_url": return True
        return False

    def _translate_images_sync(self, messages, active_eps):
        vision_eps = [e for e in active_eps if getattr(e, "is_vision", True)]
        if not vision_eps:
            return messages
            
        translation_msgs = []
        for m in messages:
            if isinstance(m.get("content"), list):
                new_content = []
                has_image = False
                for c in m["content"]:
                    if c.get("type") == "image_url":
                        has_image = True
                        new_content.append(c)
                if has_image:
                    translation_msgs.append({"role": "user", "content": new_content})
        
        if not translation_msgs: return messages
        
        sys_prompt = "你是一个专业图像解析器。请将用户提供的图片内容转化为极其详细的文字描述（包括画面细节、OCR文字、代码片段等），只输出文字描述，不要有多余的客套话。"
        translation_msgs.insert(0, {"role": "system", "content": sys_prompt})
        
        description = ""
        for v_ep in vision_eps:
            sys_log(f"启动图片解析 -> 尝试端点 {v_ep.name} ({v_ep.model})", "INFO")
            payload = {"model": v_ep.model, "messages": translation_msgs, "stream": False, "max_tokens": 4096}
            result, error = self._try_endpoint(v_ep, payload, timeout=60, log_usage=True, force_no_retry=True)
            if error:
                sys_log(f"图片解析失败 ({v_ep.name} - {v_ep.model}): {error}", "WARNING")
                continue
                
            description = result if isinstance(result, str) else result.get("choices", [{}])[0].get("message", {}).get("content", "")
            if description:
                break
                
        if not description:
            sys_log("所有图片解析端点均失败", "ERROR")
            return messages
        
        import copy
        new_msgs = copy.deepcopy(messages)
        for m in new_msgs:
            if isinstance(m.get("content"), list):
                has_image = False
                filtered_content = []
                for c in m["content"]:
                    if c.get("type") != "image_url":
                        filtered_content.append(c)
                    else:
                        has_image = True
                if has_image:
                    filtered_content.append({"type": "text", "text": f"\n\n[图片解析内容]: {description}"})
                m["content"] = filtered_content
        sys_log("图片解析完成", "INFO")
        return new_msgs

    def _check_station_health(self, station_eps):
        # 同一中转站内串行探测，且相邻两次探测之间加随机间隔，
        # 避免同域名同 key 在几秒内收到成片请求而触发限流/风控
        results = []
        for i, ep in enumerate(station_eps):
            if i > 0:
                time.sleep(random.uniform(HEALTH_STAGGER_MIN, HEALTH_STAGGER_MAX))
            try:
                results.append(self._check_one_health(ep))
            except Exception as e:
                results.append((ep.id, "bad", -1, str(e)))
        return results

    def check_all_health(self):
        with self._lock:
            endpoints = [ep for ep in self._endpoints if ep.enabled and not self.is_station_health_paused(ep)]
            for ep in endpoints:
                ep._health = "testing"
        if not endpoints:
            return []
        stations = {}
        for ep in endpoints:
            stations.setdefault(station_key(ep.base_url), []).append(ep)
        results = []
        with ThreadPoolExecutor(max_workers=min(len(stations), 10)) as pool_exec:
            futures = {pool_exec.submit(self._check_station_health, eps): key for key, eps in stations.items()}
            for future in as_completed(futures):
                try:
                    results.extend(future.result())
                except Exception as e:
                    for ep in stations[futures[future]]:
                        results.append((ep.id, "bad", -1, str(e)))
        now = time.time()
        with self._lock:
            id_map = {ep.id: ep for ep in self._endpoints}
            for ep_id, health, latency, error in results:
                ep = id_map.get(ep_id)
                if ep:
                    ep._health = health
                    ep._health_latency_ms = latency
                    ep._health_last_check = now
                    ep._health_error = error
        sys_log(f"健康检测完成: 检测了 {len(endpoints)} 个端点（{len(stations)} 个中转站）", "INFO")
        return [{"id": i, "health": h, "latency_ms": l, "error": e} for i, h, l, e in results]

    def _is_in_cooldown(self, ep):
        return ep._cooldown_until > time.time()

    def _is_quota_exceeded(self, ep):
        if ep.daily_limit <= 0: return False
        now_date = datetime.now().strftime("%Y-%m-%d")
        if ep._today_date != now_date:
            ep._today_date = now_date
            ep._today_used = 0
        return ep._today_used >= ep.daily_limit

    def _is_rpm_limited(self, ep):
        if ep.rpm_limit <= 0: return False
        now = time.time()
        with ep._rpm_lock:
            while ep._req_timestamps and ep._req_timestamps[0] < now - 60:
                ep._req_timestamps.popleft()
            return len(ep._req_timestamps) >= ep.rpm_limit

    def _set_cooldown(self, ep):
        if ep.cooldown_minutes > 0:
            ep._cooldown_until = time.time() + ep.cooldown_minutes * 60

    def _clear_cooldown(self, ep):
        ep._cooldown_until = 0

    def _probe_endpoint(self, ep):
        if ep.health_mode == "none" or self.is_station_health_paused(ep):
            return True
        payload = {"model": ep.upstream_model, "messages": [{"role": "user", "content": "ping"}], "max_tokens": 3}
        reply, _ = self._try_endpoint(ep, payload, timeout=10, log_usage=False, force_no_retry=True)
        return reply is not None

    def _cleanup_expired_cooldowns(self):
        now = time.time()
        expired = []
        with self._lock:
            for ep in self._endpoints:
                if ep.in_pool and ep._cooldown_until > 0 and ep._cooldown_until <= now and not self.is_station_connect_paused(ep):
                    expired.append(ep)
        for ep in expired:
            if self._probe_endpoint(ep):
                with self._lock:
                    ep._fail_count = 0
                    ep._last_error = ""
                    ep._last_error_ts = 0
                    ep._cooldown_until = 0
                    ep._health = "ok"
                    ep._transient_count = 0
                    ep._transient_window_start = 0
                sys_log(f"端点 '{ep.name}' 冷却过期探活通过，已恢复", "INFO")
            else:
                with self._lock:
                    self._set_cooldown(ep)
                sys_log(f"端点 '{ep.name}' 冷却过期探活未通过，继续冷却 {ep.cooldown_minutes} 分钟", "WARN")

    def resolve_model_alias(self, name):
        """按全局映射把对外别名解析为池内真实模型名；无映射时原样返回。"""
        key = (name or "").strip()
        if not key:
            return key
        with self._lock:
            aliases = dict(self.model_aliases)
        target = (aliases.get(key) or "").strip()
        return target or key

    def _active_endpoints(self, requested_model=None):
        base = [ep for ep in self._endpoints if not self.is_station_connect_paused(ep)]
        target_model = (requested_model or "").strip()
        if target_model:
            base = [ep for ep in base if (ep.model or "").strip() == target_model]
        available = [ep for ep in base if ep.enabled and ep.in_pool and not self._is_in_cooldown(ep) and not self._is_quota_exceeded(ep) and not self._is_rpm_limited(ep)]
        if available:
            return available
        fallback = [ep for ep in base if ep.enabled and ep.in_pool and not self._is_quota_exceeded(ep)]
        fallback.sort(key=lambda e: e._cooldown_until)
        return fallback

    def _pick_best(self, active):
        """
        在同一优先级层内按历史成功率加权随机选择，
        而非总是选第一个——避免所有流量都打到单一端点。
        权重公式：w = successes / (successes + failures)，
        新端点（调用数 < 10）默认按 70% 成功率估算；
        健康状态 ok/slow 小幅加成，bad/unknown 降权。
        """
        available = [ep for ep in active if not self._is_in_cooldown(ep)]
        if not available:
            # 全部冷却中，选最快恢复的
            return min(active, key=lambda e: e._cooldown_until) if active else None

        # 只在最高优先级层内竞争（数字越小优先级越高）
        min_priority = min(ep.priority for ep in available)
        top_tier = [ep for ep in available if ep.priority == min_priority]

        if len(top_tier) == 1:
            return top_tier[0]

        # 计算权重
        weights = []
        for ep in top_tier:
            total = ep._total_calls + ep._total_failures
            if total < 10:
                w = 0.7   # 新端点：假设70%成功率，给机会试探
            else:
                w = ep._total_calls / total
            # 健康状态修正
            h = ep._health
            if h == "ok":
                w = min(1.0, w * 1.15)
            elif h == "slow":
                w = w * 0.85
            elif h == "bad":
                w = w * 0.4
            # 保底权重 0.05，避免端点完全饿死
            weights.append(max(0.05, w))

        return random.choices(top_tier, weights=weights, k=1)[0]

    def _rotate(self, failed_ep, error_msg, probe_failed=False, requested_model=None):
        failed_ep._fail_count += 1
        failed_ep._total_failures += 1
        failed_ep._last_error = error_msg
        failed_ep._last_error_ts = time.time()
        self._invalidate_cache()
        if probe_failed:
            failed_ep._cooldown_until = time.time() + 30
            sys_log(f"端点 '{failed_ep.name}' 探活失败，短冷却 30 秒", "WARN")
        else:
            self._set_cooldown(failed_ep)
            sys_log(f"端点 '{failed_ep.name}' 触发冷却机制，下次可用时间在 {failed_ep.cooldown_minutes} 分钟后", "WARN")
        active = self._active_endpoints(requested_model)
        if active:
            for i, ep in enumerate(active):
                if ep is failed_ep:
                    self._current_idx = (i + 1) % len(active)
                    self._manual_override_id = None
                    return
            self._current_idx = 0

    def _on_success(self, ep, result=None, requested_model=None, latency_ms=None):
        ep._total_calls += 1
        ep._last_success_ts = time.time()
        ep._health = "ok"
        ep._fail_count = 0
        ep._transient_count = 0
        ep._transient_window_start = 0
        ep._last_error = ""
        self._clear_cooldown(ep)
        self._invalidate_cache()
        # 记录实际请求延迟（用于性能可视化）
        if latency_ms is not None and latency_ms > 0:
            ep._latency_samples.append(latency_ms)
        if self._manual_override_id and self._manual_override_id != ep.id:
            self._manual_override_id = None
        if isinstance(result, dict):
            try:
                msg = result.get("choices", [{}])[0].get("message", {})
                reasoning = msg.get("reasoning_content")
                if reasoning:
                    self._last_reasoning_content = reasoning
            except Exception:
                pass
        active = self._active_endpoints(requested_model)
        best = self._pick_best(active)
        if best and best.priority < ep.priority:
            for i, e in enumerate(active):
                if e is best:
                    self._current_idx = i
                    return
        for i, e in enumerate(active):
            if e is ep:
                self._current_idx = i
                return

    def chat(self, messages, model=None, extra_payload=None, timeout=None, return_endpoint=False):
        self._cleanup_expired_cooldowns()
        requested_model = (model or "").strip()
        if requested_model == "api-pool-aggregated":
            requested_model = ""
        # 全局别名映射：客户端请求别名 → 池内真实模型；响应中仍回显别名
        alias_name = requested_model
        requested_model = self.resolve_model_alias(requested_model)
        target_model = requested_model
        active = self._active_endpoints(target_model)
        if not active:
            if target_model:
                configured = any(
                    ep.in_pool and (ep.model or "").strip() == target_model
                    for ep in self._endpoints
                )
                if configured:
                    raise ModelRouteError(target_model, f"模型 {target_model} 没有可用端点")
                raise ModelRouteError(
                    target_model,
                    f"聚合池中未配置模型 {target_model}",
                    status=404,
                    error_type="model_not_found",
                )
            raise ValueError("没有可用的 API 端点")
        errors = []
        attempted_endpoint_ids = set()
        active.sort(key=lambda e: e.priority)
        with self._lock:
            if self._current_idx >= len(active):
                self._current_idx = 0
            idx = self._current_idx
            if self._manual_override_id:
                override_ep = next((ep for ep in active if ep.id == self._manual_override_id), None)
                if override_ep:
                    active.remove(override_ep)
                    active.insert(0, override_ep)
                    idx = 0
        while True:
            if idx >= len(active):
                idx = 0
            ep = active[idx]
            if ep.id in attempted_endpoint_ids:
                remaining = [candidate for candidate in active if candidate.id not in attempted_endpoint_ids]
                if not remaining:
                    break
                ep = remaining[0]
                idx = active.index(ep)
            ep_timeout = timeout or ep.timeout
            ep_model = ep.upstream_model
            loop_messages = messages
            if getattr(ep, "protocol", "openai") != "anthropic":
                has_assistant = any(m.get("role") == "assistant" for m in messages)
                has_reasoning = any("reasoning_content" in m for m in messages if m.get("role") == "assistant")
                if has_assistant and not has_reasoning:
                    for i in range(len(messages) - 1, -1, -1):
                        if messages[i].get("role") == "assistant":
                            loop_messages = list(messages)
                            loop_messages[i] = dict(loop_messages[i])
                            loop_messages[i]["reasoning_content"] = self._last_reasoning_content or " "
                            break
            payload = {
                "model": ep_model, "messages": loop_messages,
                **self.default_payload, **(extra_payload or {}),
            }
            
            # [VISION TRANSLATION INTERCEPT]
            if self._has_images(payload["messages"]) and getattr(ep, "is_vision", True) is False:
                has_vision = any(getattr(e, "is_vision", True) for e in active)
                if has_vision:
                    if payload.get("stream"):
                        def vision_wrapper(tgt_ep, pld, t_out, a_eps):
                            import json
                            yield f"data: {{'choices':[{{'delta':{{'content':'[API Pool: 检测到图片，当前目标不支持视觉，正在调用视觉模型进行解析...]\\n\\n'}}}}]}}\n\n".replace("'", '"')
                            translated_msgs = self._translate_images_sync(pld["messages"], a_eps)
                            yield f"data: {{'choices':[{{'delta':{{'content':'[图片解析完成，交由目标模型继续处理...]\\n\\n'}}}}]}}\n\n".replace("'", '"')
                            pld["messages"] = translated_msgs
                            gen, err = self._try_endpoint(tgt_ep, pld, t_out)
                            if err:
                                yield f"data: {{'choices':[{{'delta':{{'content':'\\n\\n[API Pool Error: 请求最终目标失败: {err}]'}}}}]}}\n\n".replace("'", '"')
                            else:
                                with self._lock:
                                    self._on_success(tgt_ep, requested_model=target_model)
                                yield from gen
                        return vision_wrapper(ep, payload, ep_timeout, active)
                    else:
                        payload["messages"] = self._translate_images_sync(payload["messages"], active)
            
            if not attempted_endpoint_ids:
                sys_log(f"收到 API 请求，尝试请求端点 '{ep.name}' (模型: {ep_model})", "INFO")
            else:
                sys_log(f"重试请求，尝试端点 '{ep.name}' (模型: {ep_model})", "INFO")

            _t_call_start = time.time()
            result, error = self._try_endpoint(ep, payload, ep_timeout)
            if result is not None:
                if isinstance(result, dict):
                    result["model"] = alias_name or target_model or ep.model
                _call_ms = int((time.time() - _t_call_start) * 1000)
                with self._lock:
                    self._on_success(ep, result, target_model, latency_ms=_call_ms)
                sys_log(f"端点 '{ep.name}' 请求成功 (延迟: {_call_ms}ms)", "INFO")
                if return_endpoint: return result, ep
                return result
            errors.append(f"[{ep.name}] {error}")
            sys_log(f"端点 '{ep.name}' 请求失败: {error}", "ERROR")
            time.sleep(0.3)
            try:
                if self._probe_endpoint(ep):
                    with self._lock:
                        ep._transient_count += 1
                        ep._fail_count += 1
                        ep._total_failures += 1
                        ep._last_error = error
                        ep._last_error_ts = time.time()
                        ep._cooldown_until = 0
                    if ep._transient_count <= 1:
                        sys_log(f"端点 '{ep.name}' 请求失败但探活通过，视为瞬态故障，原地重试", "INFO")
                        continue
                    sys_log(f"端点 '{ep.name}' 连续 {ep._transient_count} 次瞬态故障超限，进入冷却", "WARN")
            except Exception:
                pass
            attempted_endpoint_ids.add(ep.id)
            with self._lock:
                self._rotate(ep, error, requested_model=target_model)
                active = self._active_endpoints(target_model)
                active.sort(key=lambda e: e.priority)
                remaining = [candidate for candidate in active if candidate.id not in attempted_endpoint_ids]
                if not remaining:
                    break
                idx = active.index(remaining[0])
        if target_model:
            raise ModelRouteError(target_model, f"模型 {target_model} 的所有同名端点均不可用: {errors}")
        raise AllEndpointsFailed(errors)

    def _try_endpoint(self, ep, payload, timeout, log_usage=True, force_no_retry=False):
        req_t0 = time.time()
        prompt_text_to_log = extract_prompt_text(payload) if log_usage and not ep.name.startswith("test_") else ""
        is_anthropic = (getattr(ep, "protocol", "openai") == "anthropic")
        
        if is_anthropic:
            url = ep.base_url.rstrip("/") + "/messages"
            anthropic_payload = {
                "model": payload.get("model", ep.model),
                "max_tokens": payload.get("max_tokens", 4096),
            }
            if "temperature" in payload: anthropic_payload["temperature"] = payload["temperature"]
            if "top_p" in payload: anthropic_payload["top_p"] = payload["top_p"]
            if "stream" in payload: anthropic_payload["stream"] = payload["stream"]

            # OpenAI tools -> Anthropic tools
            if payload.get("tools"):
                a_tools = []
                for t in payload["tools"]:
                    fn = t.get("function", {}) if isinstance(t, dict) else {}
                    if not fn.get("name"):
                        continue
                    a_tools.append({
                        "name": fn["name"],
                        "description": fn.get("description", ""),
                        "input_schema": fn.get("parameters") or {"type": "object", "properties": {}},
                    })
                if a_tools:
                    anthropic_payload["tools"] = a_tools
            tc = payload.get("tool_choice")
            if tc == "none":
                anthropic_payload.pop("tools", None)
            elif tc == "required":
                anthropic_payload["tool_choice"] = {"type": "any"}
            elif isinstance(tc, dict) and tc.get("function", {}).get("name"):
                anthropic_payload["tool_choice"] = {"type": "tool", "name": tc["function"]["name"]}

            sys_prompt = ""
            messages = []
            for m in payload.get("messages", []):
                if m.get("role") == "system":
                    sys_prompt += m.get("content", "") + "\n"
                elif m.get("role") == "assistant" and m.get("tool_calls"):
                    # OpenAI assistant tool_calls -> Anthropic tool_use blocks
                    blocks = []
                    if m.get("content"):
                        blocks.append({"type": "text", "text": m["content"] if isinstance(m["content"], str) else str(m["content"])})
                    for t_call in m["tool_calls"]:
                        fn = t_call.get("function", {}) or {}
                        try:
                            t_input = json.loads(fn.get("arguments") or "{}")
                        except Exception:
                            t_input = {}
                        blocks.append({
                            "type": "tool_use",
                            "id": t_call.get("id") or f"toolu_{secrets.token_hex(8)}",
                            "name": fn.get("name", ""),
                            "input": t_input,
                        })
                    messages.append({"role": "assistant", "content": blocks})
                elif m.get("role") == "tool":
                    # OpenAI tool result -> Anthropic tool_result block
                    t_content = m.get("content", "")
                    if not isinstance(t_content, str):
                        t_content = json.dumps(t_content, ensure_ascii=False)
                    messages.append({
                        "role": "user",
                        "content": [{
                            "type": "tool_result",
                            "tool_use_id": m.get("tool_call_id", ""),
                            "content": t_content,
                        }],
                    })
                else:
                    role = m.get("role")
                    content = m.get("content")
                    if isinstance(content, list):
                        new_content = []
                        for c in content:
                            if c.get("type") == "image_url":
                                url_val = c.get("image_url", {}).get("url", "")
                                if url_val.startswith("data:image/"):
                                    try:
                                        media_type = url_val.split(";")[0].replace("data:", "")
                                        b64_data = url_val.split(",")[1]
                                        new_content.append({
                                            "type": "image",
                                            "source": {"type": "base64", "media_type": media_type, "data": b64_data}
                                        })
                                    except Exception:
                                        pass
                                else:
                                    new_content.append({"type": "text", "text": f"[Image URL: {url_val}]"})
                            else:
                                new_content.append(c)
                        messages.append({"role": role, "content": new_content})
                    else:
                        messages.append(m)
            if sys_prompt:
                anthropic_payload["system"] = sys_prompt.strip()
            # 合并连续同角色消息（Anthropic 要求 user/assistant 交替）
            merged = []
            for m in messages:
                if merged and merged[-1]["role"] == m["role"]:
                    prev = merged[-1]
                    def to_blocks(c):
                        if isinstance(c, list): return c
                        return [{"type": "text", "text": c or ""}]
                    prev["content"] = to_blocks(prev.get("content")) + to_blocks(m.get("content"))
                else:
                    merged.append(dict(m))
            anthropic_payload["messages"] = merged
            data = json.dumps(anthropic_payload).encode("utf-8")
        else:
            url = ep.base_url.rstrip("/") + "/chat/completions"
            data = json.dumps(payload).encode("utf-8")
            
        is_stream = payload.get("stream", False)
        
        retries = 0 if force_no_retry else ep.max_retries
        for attempt in range(retries + 1):
            if ep.rpm_limit > 0:
                with ep._rpm_lock:
                    ep._req_timestamps.append(time.time())
                    
            req = urllib.request.Request(url, data=data, method="POST")
            req.add_header("Content-Type", "application/json")
            req.add_header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
            if is_anthropic:
                safe_api_key = ep.api_key.encode('ascii', 'ignore').decode('ascii').strip()
                req.add_header("x-api-key", safe_api_key)
                req.add_header("Authorization", f"Bearer {safe_api_key}")
                req.add_header("anthropic-version", "2023-06-01")
            else:
                safe_api_key = ep.api_key.encode('ascii', 'ignore').decode('ascii').strip()
                req.add_header("Authorization", f"Bearer {safe_api_key}")
                
            for k, v in ep.extra_headers.items():
                req.add_header(k, v)
                
            try:
                if getattr(ep, "use_proxy", True) is False:
                    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
                    resp = opener.open(req, timeout=timeout)
                else:
                    resp = urllib.request.urlopen(req, timeout=timeout)
                
                if is_stream:
                    def stream_generator():
                        stream_id = f"chatcmpl-{int(time.time()*1000)}"
                        final_prompt_tokens = 0
                        final_completion_tokens = 0
                        final_total_tokens = 0
                        final_cached_tokens = 0
                        has_usage = False
                        final_completion_text = ""
                        done_sent = False
                        a_tool_idx = -1          # 当前 tool_calls 的 OpenAI index
                        a_block_is_tool = {}     # anthropic block index -> 是否 tool_use
                        a_has_tool_calls = False
                        try:
                            for line in resp:
                                if is_anthropic:
                                    if not line.strip() or not line.startswith(b"data: "):
                                        continue
                                    if line.startswith(b"data: [DONE]"):
                                        if not done_sent:
                                            yield b"data: [DONE]\n\n"
                                            done_sent = True
                                        break
                                    chunk = {}
                                    try:
                                        chunk = json.loads(line[6:].decode("utf-8"))
                                    except Exception:
                                        continue
                                    ctype = chunk.get("type")
                                    if ctype == "content_block_start":
                                        blk = chunk.get("content_block", {}) or {}
                                        b_idx = chunk.get("index", 0)
                                        if blk.get("type") == "tool_use":
                                            a_block_is_tool[b_idx] = True
                                            a_tool_idx += 1
                                            a_has_tool_calls = True
                                            o_chunk = {
                                                "id": stream_id,
                                                "object": "chat.completion.chunk",
                                                "created": int(time.time()),
                                                "model": ep.model,
                                                "choices": [{"index": 0, "delta": {"tool_calls": [{
                                                    "index": a_tool_idx,
                                                    "id": blk.get("id") or f"call_{secrets.token_hex(8)}",
                                                    "type": "function",
                                                    "function": {"name": blk.get("name", ""), "arguments": ""},
                                                }]}, "finish_reason": None}]
                                            }
                                            yield b"data: " + json.dumps(o_chunk).encode("utf-8") + b"\n\n"
                                        else:
                                            a_block_is_tool[b_idx] = False
                                    elif ctype == "content_block_delta":
                                        delta = chunk.get("delta", {}) or {}
                                        if delta.get("type") == "input_json_delta":
                                            partial = delta.get("partial_json", "")
                                            if partial:
                                                o_chunk = {
                                                    "id": stream_id,
                                                    "object": "chat.completion.chunk",
                                                    "created": int(time.time()),
                                                    "model": ep.model,
                                                    "choices": [{"index": 0, "delta": {"tool_calls": [{
                                                        "index": a_tool_idx,
                                                        "function": {"arguments": partial},
                                                    }]}, "finish_reason": None}]
                                                }
                                                yield b"data: " + json.dumps(o_chunk).encode("utf-8") + b"\n\n"
                                        else:
                                            text = delta.get("text", "")
                                            final_completion_text += text
                                            if text:
                                                o_chunk = {
                                                    "id": stream_id,
                                                    "object": "chat.completion.chunk",
                                                    "created": int(time.time()),
                                                    "model": ep.model,
                                                    "choices": [{"index": 0, "delta": {"content": text}}]
                                                }
                                                yield b"data: " + json.dumps(o_chunk).encode("utf-8") + b"\n\n"
                                    elif ctype == "message_stop":
                                        finish_chunk = {
                                            "id": stream_id,
                                            "object": "chat.completion.chunk",
                                            "created": int(time.time()),
                                            "model": ep.model,
                                            "choices": [{"index": 0, "delta": {}, "finish_reason": "tool_calls" if a_has_tool_calls else "stop"}],
                                        }
                                        yield b"data: " + json.dumps(finish_chunk).encode("utf-8") + b"\n\n"
                                        usage_chunk = {
                                            "id": stream_id,
                                            "object": "chat.completion.chunk",
                                            "created": int(time.time()),
                                            "model": ep.model,
                                            "choices": [],
                                            "usage": {
                                                "prompt_tokens": final_prompt_tokens,
                                                "completion_tokens": final_completion_tokens,
                                                "total_tokens": final_total_tokens
                                            }
                                        }
                                        yield b"data: " + json.dumps(usage_chunk).encode("utf-8") + b"\n\n"
                                        yield b"data: [DONE]\n\n"
                                        done_sent = True
                                        break
                                    elif ctype == "message_delta" and "usage" in chunk:
                                        u = chunk["usage"]
                                        final_completion_tokens += u.get("output_tokens", 0)
                                        final_total_tokens += u.get("output_tokens", 0)
                                        has_usage = True
                                    elif ctype == "message_start" and "message" in chunk and "usage" in chunk["message"]:
                                        u = chunk["message"]["usage"]
                                        prompt_t = u.get("input_tokens", 0) + u.get("cache_read_input_tokens", 0) + u.get("cache_creation_input_tokens", 0)
                                        final_prompt_tokens += prompt_t
                                        final_total_tokens += prompt_t
                                        final_cached_tokens += u.get("cache_read_input_tokens", 0)
                                        has_usage = True
                                else:
                                    if not line:
                                        continue
                                    yield line
                                    stripped = line.strip()
                                    if stripped == b"data: [DONE]":
                                        done_sent = True
                                        break
                                    try:
                                        if stripped and stripped.startswith(b"data: "):
                                            chunk = json.loads(line[6:].decode("utf-8"))
                                            if "choices" in chunk and len(chunk["choices"]) > 0:
                                                delta = chunk["choices"][0].get("delta", {})
                                                if "content" in delta:
                                                    final_completion_text += delta.get("content", "")
                                            if "usage" in chunk and chunk["usage"]:
                                                u = chunk["usage"]
                                                final_prompt_tokens = u.get("prompt_tokens", 0)
                                                final_completion_tokens = u.get("completion_tokens", 0)
                                                final_total_tokens = u.get("total_tokens", 0)
                                                if "prompt_tokens_details" in u and isinstance(u["prompt_tokens_details"], dict):
                                                    final_cached_tokens = u["prompt_tokens_details"].get("cached_tokens", 0)
                                                has_usage = True
                                    except Exception:
                                        pass
                        except Exception as e:
                            sys_log(f"流读取错误: {e}", "ERROR")
                            if not done_sent:
                                err_chunk = {
                                    "id": stream_id,
                                    "object": "chat.completion.chunk",
                                    "created": int(time.time()),
                                    "model": ep.model,
                                    "choices": [{"index": 0, "delta": {"content": f"\n\n[API Pool Error: 流读取中断: {e}]"}}],
                                }
                                yield b"data: " + json.dumps(err_chunk, ensure_ascii=False).encode("utf-8") + b"\n\n"
                                yield b"data: [DONE]\n\n"
                                done_sent = True
                        finally:
                            if not done_sent:
                                yield b"data: [DONE]\n\n"
                            if has_usage and log_usage and not ep.name.startswith("test_"):
                                token_tracker.add_usage(ep.name, ep.model, final_prompt_tokens, final_completion_tokens, final_total_tokens, final_cached_tokens)
                                chat_logger.add_log(ep.name, ep.model, prompt_text_to_log, final_completion_text, final_total_tokens, int((time.time() - req_t0) * 1000))
                                ep._today_used += final_total_tokens
                            resp.close()
                    return stream_generator(), ""
                else:
                    body = json.loads(resp.read().decode("utf-8"))
                    if is_anthropic:
                        reply = ""
                        out_tool_calls = []
                        for c in body.get("content", []):
                            if c.get("type") == "text": reply += c.get("text", "")
                            elif c.get("type") == "tool_use":
                                out_tool_calls.append({
                                    "id": c.get("id") or f"call_{secrets.token_hex(8)}",
                                    "type": "function",
                                    "function": {
                                        "name": c.get("name", ""),
                                        "arguments": json.dumps(c.get("input", {}), ensure_ascii=False),
                                    },
                                })
                        u = body.get("usage", {})
                        prompt_t = 0
                        output_t = 0
                        tot = 0
                        cached = 0
                        if u:
                            prompt_t = u.get("input_tokens", 0) + u.get("cache_read_input_tokens", 0) + u.get("cache_creation_input_tokens", 0)
                            output_t = u.get("output_tokens", 0)
                            tot = prompt_t + output_t
                            cached = u.get("cache_read_input_tokens", 0)
                            if log_usage and not ep.name.startswith("test_"):
                                token_tracker.add_usage(ep.name, ep.model, prompt_t, output_t, tot, cached)
                                chat_logger.add_log(ep.name, ep.model, prompt_text_to_log, reply.strip(), tot, int((time.time() - req_t0) * 1000))
                                ep._today_used += tot
                        out_message = {"role": "assistant", "content": reply.strip()}
                        if out_tool_calls:
                            out_message["tool_calls"] = out_tool_calls
                        return {
                            "id": f"chatcmpl-{int(time.time()*1000)}",
                            "object": "chat.completion",
                            "created": int(time.time()),
                            "model": ep.model,
                            "choices": [{
                                "index": 0,
                                "message": out_message,
                                "finish_reason": "tool_calls" if out_tool_calls else "stop",
                            }],
                            "usage": {
                                "prompt_tokens": prompt_t,
                                "completion_tokens": output_t,
                                "total_tokens": tot,
                                "prompt_tokens_details": {"cached_tokens": cached} if cached else {},
                            },
                        }, ""
                    else:
                        u = body.get("usage", {})
                        if u:
                            tot = u.get("total_tokens", 0)
                            cached = 0
                            if "prompt_tokens_details" in u and isinstance(u["prompt_tokens_details"], dict):
                                cached = u["prompt_tokens_details"].get("cached_tokens", 0)
                            if log_usage and not ep.name.startswith("test_"):
                                token_tracker.add_usage(ep.name, ep.model, u.get("prompt_tokens", 0), u.get("completion_tokens", 0), tot, cached)
                                content = body["choices"][0]["message"].get("content", "")
                                reasoning = body["choices"][0]["message"].get("reasoning_content", "")
                                chat_logger.add_log(ep.name, ep.model, prompt_text_to_log, (content or reasoning).strip(), tot, int((time.time() - req_t0) * 1000))
                                ep._today_used += tot
                        return body, ""
                    
                    
            except urllib.error.HTTPError as e:
                err_body = ""
                try: err_body = e.read().decode("utf-8", errors="ignore")[:200]
                except Exception: pass
                msg = f"HTTP {e.code}: {err_body}"
                if e.code == 429: return None, msg + " (429 rate-limited)"
                if e.code in (401, 403): return None, msg + " (auth error)"
                if e.code >= 500:
                    if attempt < retries:
                        time.sleep(1.5 * (attempt + 1))
                        continue
                    return None, msg
                return None, msg
            except (urllib.error.URLError, TimeoutError, OSError) as e:
                msg = f"连接/超时错误: {e}"
                if attempt < retries:
                    time.sleep(1.5 * (attempt + 1))
                    continue
                return None, msg
            except Exception as e:
                return None, f"未知错误: {e}"
        return None, "重试次数用尽"

    def _model_url_candidates(self, base_url):
        raw = (base_url or "").strip().rstrip("/")
        if not raw:
            return []
        if "://" not in raw:
            raw = "https://" + raw
        candidates = []

        def add(url):
            if url and url not in candidates:
                candidates.append(url)

        for suffix in ("/chat/completions", "/completions", "/models"):
            if raw.endswith(suffix):
                add(raw[:-len(suffix)] + "/models")
                break
        else:
            add(raw + "/models")
            parsed = urlparse(raw)
            if "v1" not in parsed.path.rstrip("/").split("/"):
                add(raw + "/v1/models")
        return candidates

    def fetch_models(self, base_url, api_key, timeout=10, use_proxy=True, protocol="openai"):
        safe_api_key = api_key.encode('ascii', 'ignore').decode('ascii').strip()
        if not safe_api_key:
            raise Exception("API Key 为空")

        last_error = ""
        for url in self._model_url_candidates(base_url):
            req = urllib.request.Request(url, method="GET")
            req.add_header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
            if protocol == "anthropic":
                req.add_header("x-api-key", safe_api_key)
                req.add_header("Authorization", f"Bearer {safe_api_key}")
                req.add_header("anthropic-version", "2023-06-01")
            else:
                req.add_header("Authorization", f"Bearer {safe_api_key}")

            try:
                if not use_proxy:
                    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
                    resp = opener.open(req, timeout=timeout)
                else:
                    resp = urllib.request.urlopen(req, timeout=timeout)

                with resp:
                    data = json.loads(resp.read().decode("utf-8"))
                    raw = data.get("data", data if isinstance(data, list) else [])
                    if not isinstance(raw, list):
                        raise Exception("模型列表返回格式不兼容")
                    models = []
                    for m in raw:
                        if isinstance(m, str):
                            m = {"id": m}
                        if not isinstance(m, dict):
                            continue
                        mid = m.get("id", "")
                        if not mid:
                            continue
                        info = {"id": mid}
                        if "pricing" in m: info["pricing"] = m["pricing"]
                        if "description" in m and isinstance(m["description"], str): info["description"] = m["description"][:120]
                        info["modality"] = "unknown"
                        info["modality_source"] = "none"
                        models.append(info)
                    models.sort(key=lambda x: x["id"])
                    return models
            except urllib.error.HTTPError as e:
                err_body = ""
                try:
                    err_body = e.read().decode("utf-8", errors="ignore")[:200]
                except Exception:
                    pass
                last_error = f"{url} -> HTTP {e.code}: {err_body}"
                if e.code in (401, 403):
                    raise Exception(f"鉴权失败，请检查 API Key。HTTP {e.code}: {err_body}")
                if e.code == 404:
                    continue
                raise Exception(last_error)
            except Exception as e:
                last_error = f"{url} -> {e}"
                continue

        if protocol == "anthropic":
            raise Exception("该端点尚未支持获取模型列表，或 Base URL 不正确")
        raise Exception(last_error or "未能获取模型列表，请检查 Base URL")

    def test_vision(self, base_url, api_key, model, timeout=15, use_proxy=True, protocol="openai"):
        ep = Endpoint(name="test_vision", base_url=base_url, api_key=api_key, model=model, max_retries=0, use_proxy=use_proxy, protocol=protocol)
        tiny_png = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8BQDwADhQGAWjR9awAAAABJRU5ErkJggg=="
        payload = {
            "model": model,
            "messages": [{"role": "user", "content": [{"type": "text", "text": "describe this image in 3 words"}, {"type": "image_url", "image_url": {"url": f"data:image/png;base64,{tiny_png}"}}]}],
            "max_tokens": 10,
        }
        t0 = time.time()
        reply, err = self._try_endpoint(ep, payload, timeout)
        latency = int((time.time() - t0) * 1000)
        
        if reply is not None:
            if isinstance(reply, dict):
                reply_text = reply.get("choices", [{}])[0].get("message", {}).get("content", "").lower()
            else:
                reply_text = str(reply).lower()
            unsupported_keywords = ["cannot see", "can't see", "not able to see", "unable to see", "text-based", "language model", "无法查看", "无法读取", "无法看到", "不具备", "不支持", "抱歉", "sorry", "没有上传", "没上传"]
            if any(k in reply_text for k in unsupported_keywords):
                return {"ok": True, "supports_vision": False, "latency_ms": latency, "reply": reply, "error": f"模型疑似无法读图: {reply_text[:50]}..."}
            return {"ok": True, "supports_vision": True, "latency_ms": latency, "reply": reply, "error": ""}
        else:
            unsupported = "image" in err.lower() or "vision" in err.lower() or "content" in err.lower() or "400" in err
            return {"ok": not unsupported, "supports_vision": not unsupported, "latency_ms": latency, "reply": "", "error": err}

    def test_model_latency(self, base_url, api_key, model, timeout=15, use_proxy=True, protocol="openai"):
        ep = Endpoint(name="test_latency", base_url=base_url, api_key=api_key, model=model, max_retries=0, use_proxy=use_proxy, protocol=protocol)
        payload = {"model": model, "messages": [{"role": "user", "content": "hi"}], "max_tokens": 5}
        t0 = time.time()
        reply, err = self._try_endpoint(ep, payload, timeout)
        latency = int((time.time() - t0) * 1000)
        
        if reply is not None:
            status = "ok" if latency <= LATENCY_OK_MAX else ("slow" if latency <= LATENCY_SLOW_MAX else "bad")
            return {"ok": True, "status": status, "latency_ms": latency, "reply": reply, "error": ""}
        else:
            return {"ok": False, "status": "bad", "latency_ms": latency, "reply": "", "error": err}

CONFIG_FILE = "api_config.json"
SECURITY_CONFIG_FILE = "security_config.json"
SESSION_COOKIE = "api_pool_session"
SESSION_MAX_AGE = 24 * 60 * 60

class SecurityManager:
    def __init__(self, config_path=SECURITY_CONFIG_FILE):
        self.config_path = config_path
        self._lock = threading.Lock()
        self._sessions = {}
        self.bootstrap = None
        self.config = self._load_or_create()

    def _load_or_create(self):
        if os.path.exists(self.config_path):
            try:
                with open(self.config_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                if data.get("admin_username") and data.get("password") and data.get("client_api_key_hash"):
                    return data
            except Exception:
                pass

        username = os.environ.get("API_POOL_ADMIN_USER", "admin").strip() or "admin"
        password = os.environ.get("API_POOL_ADMIN_PASSWORD") or secrets.token_urlsafe(14)
        client_api_key = os.environ.get("API_POOL_CLIENT_API_KEY") or self.generate_client_api_key_value()
        data = {
            "admin_username": username,
            "password": self._hash_password(password),
            "client_api_key_hash": self._hash_api_key(client_api_key),
            "client_api_key_hint": self._key_hint(client_api_key),
            "client_api_key_plain": client_api_key,
        }
        self._save(data)
        self.bootstrap = {
            "username": username,
            "password": None if os.environ.get("API_POOL_ADMIN_PASSWORD") else password,
            "client_api_key": None if os.environ.get("API_POOL_CLIENT_API_KEY") else client_api_key,
        }
        return data

    def _save(self, data=None):
        with open(self.config_path, "w", encoding="utf-8") as f:
            json.dump(data or self.config, f, ensure_ascii=False, indent=2)

    def _hash_password(self, password, salt=None, iterations=200000):
        salt = salt or secrets.token_hex(16)
        digest = hashlib.pbkdf2_hmac(
            "sha256",
            password.encode("utf-8"),
            bytes.fromhex(salt),
            iterations,
        ).hex()
        return {"salt": salt, "hash": digest, "iterations": iterations}

    def _verify_password(self, password, password_data=None):
        password_data = password_data or self.config.get("password", {})
        try:
            hashed = self._hash_password(
                password,
                salt=password_data["salt"],
                iterations=int(password_data.get("iterations", 200000)),
            )
            return hmac.compare_digest(hashed["hash"], password_data.get("hash", ""))
        except Exception:
            return False

    def _hash_api_key(self, api_key):
        return hashlib.sha256(("api-pool:" + api_key).encode("utf-8")).hexdigest()

    def _key_hint(self, api_key):
        if len(api_key) <= 12:
            return "*" * len(api_key)
        return f"{api_key[:8]}...{api_key[-6:]}"

    def public_config(self):
        return {
            "username": self.config.get("admin_username", "admin"),
            "client_api_key_hint": self.config.get("client_api_key_hint", ""),
            "client_api_key_available": bool(self.config.get("client_api_key_plain")),
        }

    def verify_login(self, username, password):
        expected_user = self.config.get("admin_username", "")
        return hmac.compare_digest(username or "", expected_user) and self._verify_password(password or "")

    def create_session(self):
        token = secrets.token_urlsafe(32)
        with self._lock:
            self._sessions[token] = time.time() + SESSION_MAX_AGE
        return token

    def destroy_session(self, cookie_header):
        token = self._session_token_from_cookie(cookie_header)
        if token:
            with self._lock:
                self._sessions.pop(token, None)

    def _session_token_from_cookie(self, cookie_header):
        if not cookie_header:
            return ""
        try:
            cookie = http.cookies.SimpleCookie()
            cookie.load(cookie_header)
            morsel = cookie.get(SESSION_COOKIE)
            return morsel.value if morsel else ""
        except Exception:
            return ""

    def is_authenticated(self, cookie_header):
        token = self._session_token_from_cookie(cookie_header)
        if not token:
            return False
        now = time.time()
        with self._lock:
            expires_at = self._sessions.get(token, 0)
            if expires_at <= now:
                self._sessions.pop(token, None)
                return False
            self._sessions[token] = now + SESSION_MAX_AGE
            return True

    def verify_client_api_key(self, headers):
        auth = headers.get("Authorization", "")
        api_key = ""
        if auth.lower().startswith("bearer "):
            api_key = auth[7:].strip()
        if not api_key:
            api_key = headers.get("X-API-Key", "").strip()
        if not api_key:
            return False
        return hmac.compare_digest(self._hash_api_key(api_key), self.config.get("client_api_key_hash", ""))

    def update_admin(self, current_password, username=None, password=None):
        if not self._verify_password(current_password or ""):
            return False, "当前密码不正确"
        username = (username or self.config.get("admin_username", "admin")).strip()
        if not username or len(username) > 64:
            return False, "账号长度不合法"
        if password:
            if len(password) < 8:
                return False, "新密码至少 8 位"
            self.config["password"] = self._hash_password(password)
        self.config["admin_username"] = username
        self._save()
        return True, ""

    def set_client_api_key(self, api_key):
        api_key = (api_key or "").strip()
        if len(api_key) < 12:
            return False, "API Key 至少 12 位", ""
        self.config["client_api_key_hash"] = self._hash_api_key(api_key)
        self.config["client_api_key_hint"] = self._key_hint(api_key)
        self.config["client_api_key_plain"] = api_key
        self._save()
        return True, "", self.config["client_api_key_hint"]

    def generate_client_api_key_value(self):
        return "sk-apipool-" + secrets.token_urlsafe(32)

    def rotate_client_api_key(self):
        api_key = self.generate_client_api_key_value()
        self.set_client_api_key(api_key)
        return api_key, self.config["client_api_key_hint"]

    def get_client_api_key(self):
        return self.config.get("client_api_key_plain", "")

security_manager = SecurityManager()

def load_config():
    try:
        with open(CONFIG_FILE, "r", encoding="utf-8") as f: return json.load(f).get("api_endpoints", [])
    except Exception:
        return []


def load_model_aliases():
    try:
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            val = json.load(f).get("model_aliases")
            if isinstance(val, dict):
                return {str(k).strip(): str(v).strip() for k, v in val.items()
                        if str(k).strip() and str(v).strip()}
    except Exception:
        pass
    return {}

def load_station_settings():
    try:
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            val = json.load(f).get("station_settings")
            if isinstance(val, dict):
                return {k: {"health_paused": bool(v.get("health_paused")), "connect_paused": bool(v.get("connect_paused"))}
                        for k, v in val.items() if isinstance(v, dict)}
    except Exception:
        pass
    return {}

def save_config(endpoints_data, station_settings=None, model_aliases=None):
    data = {"api_endpoints": endpoints_data}
    if station_settings:
        data["station_settings"] = station_settings
    if model_aliases:
        data["model_aliases"] = model_aliases
    with open(CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def ensure_config():
    if not os.path.exists(CONFIG_FILE): save_config([])

ensure_config()

HEALTH_QUICK_INTERVAL = 60   # 健康状态异常端点快速复检间隔（秒）

def _health_check_loop():
    """
    智能健康检测：
    - 每 60 秒：仅对 health==bad 且【不在冷却期】的端点快速复检
      （冷却中的端点已由 _cleanup_expired_cooldowns 在到期时自动探活，不重复打扰）
    - 每 600 秒：对所有端点做一次全量检测
    """
    last_full_check = 0
    while True:
        time.sleep(HEALTH_QUICK_INTERVAL)
        now = time.time()
        try:
            if now - last_full_check >= HEALTH_CHECK_INTERVAL:
                pool.check_all_health()
                last_full_check = now
            else:
                # 只复检 health==bad 且未在冷却期的端点
                # 冷却中 = 因 429/5xx 触发了惩罚冷却，不应再打扰，等冷却到期自动恢复
                with pool._lock:
                    bad_eps = [
                        ep for ep in pool._endpoints
                        if ep.enabled
                        and not pool.is_station_health_paused(ep)
                        and ep.health_mode != "none"
                        and ep._health == "bad"
                        and ep._cooldown_until <= now   # 排除冷却期端点
                    ]
                if bad_eps:
                    sys_log(f"快速复检 {len(bad_eps)} 个异常端点 (排除冷却中)...", "INFO")
                    with ThreadPoolExecutor(max_workers=min(len(bad_eps), 5)) as ex:
                        futures = {ex.submit(pool._check_one_health, ep): ep for ep in bad_eps}
                        results = []
                        for fut in as_completed(futures):
                            try: results.append(fut.result())
                            except Exception as e: results.append((futures[fut].id, "bad", -1, str(e)))
                    ts = time.time()
                    with pool._lock:
                        id_map = {ep.id: ep for ep in pool._endpoints}
                        for ep_id, health, latency, error in results:
                            ep = id_map.get(ep_id)
                            if ep:
                                ep._health = health
                                ep._health_latency_ms = latency
                                ep._health_last_check = ts
                                ep._health_error = error
                    pool._invalidate_cache()
                    recovered = sum(1 for _, h, _, _ in results if h == "ok")
                    if recovered:
                        sys_log(f"快速复检完成：{recovered}/{len(bad_eps)} 个端点已恢复", "INFO")
        except Exception as e:
            sys_log(f"健康检测循环异常: {e}", "WARN")

# 每 24 小时自动清理 + 压缩数据库
DB_MAINTENANCE_INTERVAL = 86400
def _db_maintenance_loop():
    while True:
        time.sleep(DB_MAINTENANCE_INTERVAL)
        try:
            chat_logger._startup_cleanup()
            sys_log("定时数据库维护完成", "INFO")
        except Exception as e:
            sys_log(f"定时数据库维护失败: {e}", "WARN")

pool = APIPool(default_payload={"temperature": 0.7})
for ep_data in load_config(): pool.add_endpoint(ep_data)
pool.station_settings = load_station_settings()
pool.model_aliases = load_model_aliases()

_health_thread = threading.Thread(target=_health_check_loop, daemon=True)
_health_thread.start()
threading.Thread(target=pool.check_all_health, daemon=True).start()

# 启动数据库定期维护线程
_db_maintenance_thread = threading.Thread(target=_db_maintenance_loop, daemon=True)
_db_maintenance_thread.start()


def list_openai_models():
    now = int(time.time())
    seen = set()
    models = []
    with pool._lock:
        endpoints = list(pool._endpoints)

    for ep in endpoints:
        model = (ep.model or "").strip()
        if not ep.enabled or not model or model in seen:
            continue
        seen.add(model)
        models.append({
            "id": model,
            "object": "model",
            "created": now,
            "owned_by": "api-pool",
        })

    # 全局别名也对外可见，方便客户端直接选中
    with pool._lock:
        aliases = dict(pool.model_aliases)
    for alias in aliases:
        alias = (alias or "").strip()
        if alias and alias not in seen:
            seen.add(alias)
            models.append({
                "id": alias,
                "object": "model",
                "created": now,
                "owned_by": "api-pool",
            })

    if "api-pool-aggregated" not in seen:
        models.insert(0, {
            "id": "api-pool-aggregated",
            "object": "model",
            "created": now,
            "owned_by": "api-pool",
        })

    return {"object": "list", "data": models}


def responses_content_to_text(content):
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts = []
        for item in content:
            if isinstance(item, str):
                parts.append(item)
            elif isinstance(item, dict):
                text = item.get("text") or item.get("input_text") or item.get("output_text")
                if text:
                    parts.append(str(text))
        return "\n".join(parts)
    return "" if content is None else str(content)


def responses_input_to_messages(body):
    messages = []
    instructions = body.get("instructions")
    if instructions:
        messages.append({"role": "system", "content": str(instructions)})

    user_input = body.get("input", "")
    if isinstance(user_input, str):
        messages.append({"role": "user", "content": user_input})
    elif isinstance(user_input, list):
        for item in user_input:
            if isinstance(item, str):
                messages.append({"role": "user", "content": item})
            elif isinstance(item, dict):
                itype = item.get("type")
                if itype == "reasoning":
                    continue  # codex 会回传 reasoning 项，chat 协议无对应概念
                if itype in ("function_call", "custom_tool_call"):
                    args = item.get("arguments")
                    if args is None:
                        args = item.get("input", "")  # custom_tool_call 用 input 字段
                    if not isinstance(args, str):
                        args = json.dumps(args, ensure_ascii=False)
                    if itype == "custom_tool_call":
                        args = json.dumps({"input": args}, ensure_ascii=False)
                    messages.append({
                        "role": "assistant",
                        "content": None,
                        "tool_calls": [{
                            "id": item.get("call_id") or item.get("id") or f"call_{secrets.token_hex(8)}",
                            "type": "function",
                            "function": {
                                "name": item.get("name", ""),
                                "arguments": args or "{}",
                            },
                        }],
                    })
                    continue
                if itype in ("function_call_output", "custom_tool_call_output", "local_shell_call_output"):
                    out = item.get("output", "")
                    if not isinstance(out, str):
                        out = json.dumps(out, ensure_ascii=False)
                    messages.append({
                        "role": "tool",
                        "tool_call_id": item.get("call_id") or item.get("id") or "",
                        "content": out,
                    })
                    continue
                role = item.get("role") or "user"
                content = responses_content_to_text(item.get("content"))
                if content:
                    messages.append({"role": role, "content": content})

    if not messages:
        messages.append({"role": "user", "content": ""})
    # 合并连续的 assistant tool_calls（codex 一轮可能发多个 function_call 项）
    merged = []
    for m in messages:
        if (merged and m.get("tool_calls") and merged[-1].get("role") == "assistant"
                and merged[-1].get("tool_calls") and not merged[-1].get("content")):
            merged[-1]["tool_calls"] = list(merged[-1]["tool_calls"]) + list(m["tool_calls"])
        else:
            merged.append(m)
    return merged


def extract_chat_result_text(result):
    if isinstance(result, str):
        return result
    if not isinstance(result, dict):
        return "" if result is None else str(result)
    try:
        message = result.get("choices", [{}])[0].get("message", {})
        return message.get("content") or message.get("reasoning_content") or ""
    except Exception:
        return json.dumps(result, ensure_ascii=False)


def extract_chat_result_tool_calls(result):
    if not isinstance(result, dict):
        return []
    try:
        message = result.get("choices", [{}])[0].get("message", {}) or {}
        return message.get("tool_calls") or []
    except Exception:
        return []


def responses_tools_to_chat_tools(tools):
    """Responses API 的工具定义转 Chat Completions 格式。
    只保留能转成合法 function 工具的条目；local_shell/web_search 等
    Chat Completions 不支持的类型直接丢弃，避免上游 400（tools[N].name 缺失）。"""
    converted = []
    dropped = []
    for t in tools or []:
        if not isinstance(t, dict):
            continue
        fn = t.get("function") if isinstance(t.get("function"), dict) else None
        if fn and fn.get("name"):
            # 已是 chat 格式
            converted.append({
                "type": "function",
                "function": {
                    "name": fn["name"],
                    "description": fn.get("description", ""),
                    "parameters": fn.get("parameters") or {"type": "object", "properties": {}},
                },
            })
        elif t.get("name") and t.get("type") in (None, "function"):
            # Responses 扁平格式
            converted.append({
                "type": "function",
                "function": {
                    "name": t["name"],
                    "description": t.get("description", ""),
                    "parameters": t.get("parameters") or {"type": "object", "properties": {}},
                },
            })
        elif t.get("type") == "custom" and t.get("name"):
            # freeform 工具（如 codex 的 apply_patch）降级为单字符串参数的函数
            converted.append({
                "type": "function",
                "function": {
                    "name": t["name"],
                    "description": t.get("description", ""),
                    "parameters": {
                        "type": "object",
                        "properties": {"input": {"type": "string", "description": "raw tool input"}},
                        "required": ["input"],
                    },
                },
            })
        else:
            dropped.append(t.get("type") or "unknown")
    if dropped:
        sys_log(f"Responses 请求中 {len(dropped)} 个工具类型不受支持已忽略: {dropped}", "WARN")
    return converted


def tool_calls_to_responses_items(tool_calls, created):
    items = []
    for tc in tool_calls or []:
        fn = tc.get("function", {}) or {}
        items.append({
            "id": f"fc_{created}{secrets.token_hex(4)}",
            "type": "function_call",
            "status": "completed",
            "call_id": tc.get("id") or f"call_{secrets.token_hex(8)}",
            "name": fn.get("name", ""),
            "arguments": fn.get("arguments", "") or "{}",
        })
    return items


def make_chat_completion_response(body, result):
    if isinstance(result, dict):
        return result
    return {
        "id": f"chatcmpl-{int(time.time()*1000)}",
        "object": "chat.completion",
        "created": int(time.time()),
        "model": body.get("model") or "api-pool-aggregated",
        "choices": [{"index": 0, "message": {"role": "assistant", "content": result}, "finish_reason": "stop"}],
        "usage": {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
    }


def make_responses_response(body, text, served_model=None, tool_calls=None):
    created = int(time.time())
    response_id = f"resp_{created}{secrets.token_hex(4)}"
    message_id = f"msg_{created}{secrets.token_hex(4)}"
    output = []
    if text or not tool_calls:
        output.append({
            "id": message_id,
            "type": "message",
            "status": "completed",
            "role": "assistant",
            "content": [{"type": "output_text", "text": text, "annotations": []}],
        })
    output.extend(tool_calls_to_responses_items(tool_calls, created))
    return {
        "id": response_id,
        "object": "response",
        "created_at": created,
        "status": "completed",
        "model": served_model or body.get("model") or "api-pool-aggregated",
        "output": output,
        "output_text": text,
        "usage": {"input_tokens": 0, "output_tokens": 0, "total_tokens": 0},
    }


def make_responses_stream(body, text, served_model=None, tool_calls=None):
    response = make_responses_response(body, "", served_model)
    response_id = response["id"]
    model = response["model"]
    message_id = response["output"][0]["id"]
    content_part = {"type": "output_text", "text": "", "annotations": []}

    def gen():
        created = int(time.time())
        yield b"data: " + json.dumps({
            "type": "response.created",
            "response": response,
        }, ensure_ascii=False).encode("utf-8") + b"\n\n"
        yield b"data: " + json.dumps({
            "type": "response.output_item.added",
            "response_id": response_id,
            "output_index": 0,
            "item": {
                "id": message_id,
                "type": "message",
                "status": "in_progress",
                "role": "assistant",
                "content": [],
            },
        }, ensure_ascii=False).encode("utf-8") + b"\n\n"
        yield b"data: " + json.dumps({
            "type": "response.content_part.added",
            "response_id": response_id,
            "item_id": message_id,
            "output_index": 0,
            "content_index": 0,
            "part": content_part,
        }, ensure_ascii=False).encode("utf-8") + b"\n\n"
        yield b"data: " + json.dumps({
            "type": "response.output_text.delta",
            "response_id": response_id,
            "item_id": message_id,
            "output_index": 0,
            "content_index": 0,
            "delta": text,
        }, ensure_ascii=False).encode("utf-8") + b"\n\n"
        yield b"data: " + json.dumps({
            "type": "response.output_text.done",
            "response_id": response_id,
            "item_id": message_id,
            "output_index": 0,
            "content_index": 0,
            "text": text,
        }, ensure_ascii=False).encode("utf-8") + b"\n\n"
        yield b"data: " + json.dumps({
            "type": "response.content_part.done",
            "response_id": response_id,
            "item_id": message_id,
            "output_index": 0,
            "content_index": 0,
            "part": {"type": "output_text", "text": text, "annotations": []},
        }, ensure_ascii=False).encode("utf-8") + b"\n\n"
        yield b"data: " + json.dumps({
            "type": "response.output_item.done",
            "response_id": response_id,
            "output_index": 0,
            "item": {
                "id": message_id,
                "type": "message",
                "status": "completed",
                "role": "assistant",
                "content": [{"type": "output_text", "text": text, "annotations": []}],
            },
        }, ensure_ascii=False).encode("utf-8") + b"\n\n"
        fc_items = tool_calls_to_responses_items(tool_calls, created)
        for i, item in enumerate(fc_items):
            out_idx = i + 1
            yield b"data: " + json.dumps({
                "type": "response.output_item.added",
                "response_id": response_id,
                "output_index": out_idx,
                "item": {**item, "status": "in_progress", "arguments": ""},
            }, ensure_ascii=False).encode("utf-8") + b"\n\n"
            yield b"data: " + json.dumps({
                "type": "response.function_call_arguments.delta",
                "response_id": response_id,
                "item_id": item["id"],
                "output_index": out_idx,
                "delta": item["arguments"],
            }, ensure_ascii=False).encode("utf-8") + b"\n\n"
            yield b"data: " + json.dumps({
                "type": "response.function_call_arguments.done",
                "response_id": response_id,
                "item_id": item["id"],
                "output_index": out_idx,
                "arguments": item["arguments"],
            }, ensure_ascii=False).encode("utf-8") + b"\n\n"
            yield b"data: " + json.dumps({
                "type": "response.output_item.done",
                "response_id": response_id,
                "output_index": out_idx,
                "item": item,
            }, ensure_ascii=False).encode("utf-8") + b"\n\n"
        completed = make_responses_response({"model": model}, text, model, tool_calls)
        completed["id"] = response_id
        completed["created_at"] = created
        if completed["output"] and completed["output"][0].get("type") == "message":
            completed["output"][0]["id"] = message_id
        yield b"data: " + json.dumps({
            "type": "response.completed",
            "response": completed,
        }, ensure_ascii=False).encode("utf-8") + b"\n\n"
        yield b"data: [DONE]\n\n"

    return gen()


def api_handler(method, path, body):
    parsed = urlparse(path)
    cp = parsed.path

    if method == "GET" and cp in ("/v1", "/v1/"):
        return 200, {
            "ok": True,
            "name": "api-pool",
            "models_url": "/v1/models",
            "chat_completions_url": "/v1/chat/completions",
            "responses_url": "/v1/responses",
        }, False

    if method == "GET" and cp in ("/v1/models", "/models"):
        return 200, list_openai_models(), False

    if method == "GET" and cp == "/api/security":
        return 200, {"ok": True, **security_manager.public_config()}, False

    if method == "PUT" and cp == "/api/security/admin":
        ok, err = security_manager.update_admin(
            body.get("current_password", ""),
            username=body.get("username"),
            password=body.get("password") or None,
        )
        if not ok:
            return 400, {"ok": False, "error": err}, False
        return 200, {"ok": True, **security_manager.public_config()}, False

    if method == "PUT" and cp == "/api/security/api-key":
        ok, err, hint = security_manager.set_client_api_key(body.get("api_key", ""))
        if not ok:
            return 400, {"ok": False, "error": err}, False
        return 200, {"ok": True, "client_api_key_hint": hint}, False

    if method == "GET" and cp == "/api/security/api-key/reveal":
        api_key = security_manager.get_client_api_key()
        if not api_key:
            return 404, {"ok": False, "error": "当前 Key 无法直接读取，请先生成新的 API Key"}, False
        return 200, {"ok": True, "api_key": api_key, "client_api_key_hint": security_manager.public_config().get("client_api_key_hint", "")}, False

    if method == "POST" and cp == "/api/security/api-key/generate":
        api_key, hint = security_manager.rotate_client_api_key()
        return 200, {"ok": True, "api_key": api_key, "client_api_key_hint": hint}, False

    # ================= 代理接口 =================
    if method == "POST" and cp in ("/v1/chat/completions", "/chat/completions"):
        messages = body.get("messages", [])
        is_stream = body.get("stream", False)
        
        stream_keys = ("stream", "stream_options") if is_stream else ()
        extra_payload = {k: v for k, v in body.items() if k not in ("messages", "model", *stream_keys)}
        
        try:
            result = pool.chat(messages, model=body.get("model"), extra_payload=extra_payload)
            if is_stream:
                def compat_stream():
                    stream_id = f"chatcmpl-{int(time.time()*1000)}"
                    model = result.get("model") if isinstance(result, dict) else None
                    model = model or body.get("model") or "api-pool-aggregated"
                    message = {}
                    finish_reason = "stop"
                    if isinstance(result, dict):
                        try:
                            choice0 = result.get("choices", [{}])[0]
                            message = choice0.get("message", {}) or {}
                            finish_reason = choice0.get("finish_reason") or "stop"
                        except Exception:
                            pass
                    content = extract_chat_result_text(result)
                    tool_calls = message.get("tool_calls")
                    if tool_calls and finish_reason == "stop":
                        finish_reason = "tool_calls"
                    first_chunk = {
                        "id": stream_id,
                        "object": "chat.completion.chunk",
                        "created": int(time.time()),
                        "model": model,
                        "choices": [{"index": 0, "delta": {"role": "assistant"}, "finish_reason": None}],
                    }
                    yield b"data: " + json.dumps(first_chunk, ensure_ascii=False).encode("utf-8") + b"\n\n"
                    if content:
                        chunk = {
                            "id": stream_id,
                            "object": "chat.completion.chunk",
                            "created": int(time.time()),
                            "model": model,
                            "choices": [{"index": 0, "delta": {"content": content}, "finish_reason": None}],
                        }
                        yield b"data: " + json.dumps(chunk, ensure_ascii=False).encode("utf-8") + b"\n\n"
                    if tool_calls:
                        delta_calls = []
                        for i, tc in enumerate(tool_calls):
                            dc = dict(tc)
                            dc["index"] = i
                            delta_calls.append(dc)
                        chunk = {
                            "id": stream_id,
                            "object": "chat.completion.chunk",
                            "created": int(time.time()),
                            "model": model,
                            "choices": [{"index": 0, "delta": {"tool_calls": delta_calls}, "finish_reason": None}],
                        }
                        yield b"data: " + json.dumps(chunk, ensure_ascii=False).encode("utf-8") + b"\n\n"
                    end_chunk = {
                        "id": stream_id,
                        "object": "chat.completion.chunk",
                        "created": int(time.time()),
                        "model": model,
                        "choices": [{"index": 0, "delta": {}, "finish_reason": finish_reason}],
                    }
                    yield b"data: " + json.dumps(end_chunk, ensure_ascii=False).encode("utf-8") + b"\n\n"
                    yield b"data: [DONE]\n\n"
                return 200, compat_stream(), True
            
            return 200, make_chat_completion_response(body, result), False
            
        except ModelRouteError as e:
            return e.status, {"error": {"message": str(e), "type": e.error_type, "model": e.model}}, False
        except AllEndpointsFailed as e:
            return 500, {"error": {"message": f"所有端点均已失效: {e.errors}", "type": "server_error"}}, False
        except Exception as e:
            return 500, {"error": {"message": str(e), "type": "server_error"}}, False

    if method == "POST" and cp in ("/v1/responses", "/responses"):
        messages = responses_input_to_messages(body)
        is_stream = body.get("stream", False)
        passthrough_keys = {"temperature", "top_p", "max_tokens", "max_output_tokens", "tool_choice", "parallel_tool_calls"}
        extra_payload = {k: v for k, v in body.items() if k in passthrough_keys}
        if "max_output_tokens" in extra_payload and "max_tokens" not in extra_payload:
            extra_payload["max_tokens"] = extra_payload.pop("max_output_tokens")
        if body.get("tools"):
            chat_tools = responses_tools_to_chat_tools(body["tools"])
            if chat_tools:
                extra_payload["tools"] = chat_tools
            else:
                extra_payload.pop("tool_choice", None)
        # Responses 的 tool_choice 对象格式是扁平 {"type":"function","name":...}
        tc = extra_payload.get("tool_choice")
        if isinstance(tc, dict) and tc.get("name") and "function" not in tc:
            extra_payload["tool_choice"] = {"type": "function", "function": {"name": tc["name"]}}

        try:
            result = pool.chat(messages, model=body.get("model"), extra_payload=extra_payload)
            text = extract_chat_result_text(result)
            tool_calls = extract_chat_result_tool_calls(result)
            served_model = result.get("model") if isinstance(result, dict) else None
            if is_stream:
                return 200, make_responses_stream(body, text, served_model, tool_calls), True
            return 200, make_responses_response(body, text, served_model, tool_calls), False
        except ModelRouteError as e:
            return e.status, {"error": {"message": str(e), "type": e.error_type, "model": e.model}}, False
        except AllEndpointsFailed as e:
            return 500, {"error": {"message": f"All endpoints failed: {e.errors}", "type": "server_error"}}, False
        except Exception as e:
            return 500, {"error": {"message": str(e), "type": "server_error"}}, False

    if method == "GET" and cp == "/api/logs":
        qs = dict(q.split("=") for q in parsed.query.split("&") if "=" in q) if parsed.query else {}
        last_id = int(qs.get("since", 0))
        return 200, sys_logger.get_logs_since(last_id), False

    if method == "DELETE" and cp == "/api/logs":
        sys_logger.clear_logs()
        return 200, {"ok": True}, False

    if method == "GET" and cp == "/api/chat-logs":
        qs = dict(q.split("=") for q in parsed.query.split("&") if "=" in q) if parsed.query else {}
        limit = int(qs.get("limit", 50))
        offset = int(qs.get("offset", 0))
        return 200, chat_logger.get_logs(limit=limit, offset=offset), False

    if method == "DELETE" and cp == "/api/chat-logs":
        chat_logger.clear_logs()
        return 200, {"ok": True}, False

    if method == "GET" and cp == "/api/token-stats":
        qs = dict(q.split("=") for q in parsed.query.split("&") if "=" in q) if parsed.query else {}
        ep = qs.get("endpoint", "all")
        # url decode
        ep = urllib.parse.unquote(ep)
        return 200, token_tracker.get_stats(endpoint_filter=ep), False

    if method == "DELETE" and cp == "/api/token-stats":
        token_tracker.clear_data()
        return 200, {"ok": True}, False

    if method == "GET" and cp == "/api/endpoints": return 200, pool.list_endpoints(), False
    if method == "GET" and cp == "/api/chain": return 200, pool.get_active_chain(), False
    if method == "GET" and cp == "/api/endpoint-perf":
        now = time.time()
        with pool._lock:
            endpoints = list(pool._endpoints)
        result = []
        for ep in endpoints:
            samples = list(ep._latency_samples)
            total = ep._total_calls + ep._total_failures
            result.append({
                "id": ep.id,
                "name": ep.name,
                "model": ep.model,
                "priority": ep.priority,
                "enabled": ep.enabled,
                "success_rate": round(ep._total_calls / max(1, total) * 100, 1),
                "total_calls": ep._total_calls,
                "total_failures": ep._total_failures,
                "avg_latency_ms": int(sum(samples) / len(samples)) if samples else None,
                "min_latency_ms": int(min(samples)) if samples else None,
                "max_latency_ms": int(max(samples)) if samples else None,
                "p90_latency_ms": int(sorted(samples)[int(len(samples) * 0.9)]) if len(samples) >= 10 else None,
                "latency_history": samples[-20:],  # 最近20个样本用于趋势图
                "health": ep._health,
                "health_latency_ms": ep._health_latency_ms,
                "last_success": ep._last_success_ts,
            })
        # 按成功率降序排列
        result.sort(key=lambda x: (-x["success_rate"], -(x["total_calls"])))
        return 200, result, False
    if method == "GET" and cp == "/api/stations": return 200, pool.list_stations(), False
    if method == "PUT" and cp.startswith("/api/stations/"):
        key = unquote(cp.split("/api/stations/", 1)[1])
        known = {s["key"] for s in pool.list_stations()}
        if key not in known:
            return 404, {"ok": False, "error": "中转站不存在"}, False
        hp = body.get("health_paused")
        cpause = body.get("connect_paused")
        pool.set_station_setting(key, health_paused=hp, connect_paused=cpause)
        parts = []
        if hp is not None: parts.append("测活已" + ("暂停" if hp else "恢复"))
        if cpause is not None: parts.append("连接已" + ("停止" if cpause else "恢复"))
        # 批量编辑该站所有端点的公共配置（仅应用白名单字段，留空/缺省不改）
        updates = body.get("updates")
        new_key = key
        if isinstance(updates, dict):
            allowed = {"name", "base_url", "api_key", "timeout", "max_retries", "cooldown_minutes",
                       "priority", "daily_limit", "rpm_limit", "use_proxy", "protocol",
                       "health_mode", "billing_mode", "is_vision"}
            clean = {}
            for k, v in updates.items():
                if k not in allowed or v is None or (isinstance(v, str) and not v.strip()):
                    continue
                clean[k] = v.strip() if isinstance(v, str) else v
            if clean:
                targets = [ep for ep in pool.list_endpoints() if station_key(ep["base_url"]) == key]
                for ep in targets:
                    if "name" in clean and clean["name"] != ep["name"]:
                        token_tracker.rename_endpoint(ep["name"], clean["name"])
                    pool.update_endpoint(ep["id"], clean)
                # base_url 改变会导致站点标识（域名）变化，需迁移站点设置
                if "base_url" in clean:
                    new_key = station_key(clean["base_url"])
                    if new_key != key:
                        with pool._lock:
                            conf = pool.station_settings.pop(key, None)
                            if conf:
                                pool.station_settings[new_key] = conf
                parts.append(f"已更新 {len(targets)} 个端点的配置 ({', '.join(clean.keys())})")
        _sync_to_config()
        sys_log(f"中转站 '{key}': {'，'.join(parts) or '设置已更新'}", "INFO")
        return 200, {"ok": True, "station": next((s for s in pool.list_stations() if s["key"] == new_key), None)}, False
    if method == "DELETE" and cp.startswith("/api/stations/"):
        key = unquote(cp.split("/api/stations/", 1)[1])
        targets = [ep for ep in pool.list_endpoints() if station_key(ep["base_url"]) == key]
        if not targets:
            return 404, {"ok": False, "error": "中转站不存在"}, False
        for ep in targets:
            pool.remove_endpoint(ep["id"])
        with pool._lock:
            pool.station_settings.pop(key, None)
        _sync_to_config()
        sys_log(f"中转站 '{key}' 已删除（移除 {len(targets)} 个端点）", "WARN")
        return 200, {"ok": True, "removed": len(targets)}, False
    if method == "GET" and cp == "/api/pool":
        return 200, [ep for ep in pool.list_endpoints() if ep.get("in_pool")], False
    if method == "POST" and cp.startswith("/api/pool/"):
        ep_id = unquote(cp.split("/")[-1])
        pool.set_pool(ep_id, True); _sync_to_config(); return 200, {"ok": True}, False
    if method == "DELETE" and cp.startswith("/api/pool/"):
        ep_id = unquote(cp.split("/")[-1])
        pool.set_pool(ep_id, False); _sync_to_config(); return 200, {"ok": True}, False
    if method == "POST" and cp.startswith("/api/switch-endpoint/"):
        ep_id = unquote(cp.split("/")[-1])
        return 200, {"ok": pool.switch_to_endpoint(ep_id)}, False
    if method == "POST" and cp == "/api/endpoints":
        pool.add_endpoint(body); _sync_to_config(); return 201, {"ok": True}, False
    if method == "POST" and cp == "/api/endpoints/batch":
        items = body.get("endpoints", []); base = body.get("base", {}); added = 0; start_priority = base.get("start_priority", 1)
        for i, item in enumerate(items):
            ep = {
                "name": item.get("name", base.get("name", f"ep_{i}")), "base_url": item.get("base_url", base.get("base_url", "")),
                "api_key": item.get("api_key", base.get("api_key", "")),
                "model": item.get("public_model", item.get("model", "")),
                "public_model": item.get("public_model", item.get("model", "")),
                "upstream_model": item.get("upstream_model", item.get("model", "")),
                "priority": item.get("priority", start_priority + i), "timeout": item.get("timeout", base.get("timeout", 60)),
                "max_retries": item.get("max_retries", base.get("max_retries", 1)), "cooldown_minutes": item.get("cooldown_minutes", base.get("cooldown_minutes", 5)),
                "daily_limit": item.get("daily_limit", base.get("daily_limit", 0)), "rpm_limit": item.get("rpm_limit", base.get("rpm_limit", 0)),
                "use_proxy": item.get("use_proxy", base.get("use_proxy", True)),
                "protocol": item.get("protocol", base.get("protocol", "openai")),
                "health_mode": item.get("health_mode", base.get("health_mode", "chat")),
                "billing_mode": item.get("billing_mode", base.get("billing_mode", "subscription")),
                "is_vision": item.get("is_vision", base.get("is_vision", True)),
                "in_pool": item.get("in_pool", base.get("in_pool", True)),
                "enabled": item.get("enabled", True),
            }
            if ep["model"]: pool.add_endpoint(ep); added += 1
        _sync_to_config(); return 201, {"ok": True, "added": added}, False
    if method == "PUT" and cp.startswith("/api/endpoints/") and not cp.endswith("/toggle"):
        ep_id = unquote(cp.split("/")[-1])
        new_name = body.get("name")
        old_ep = next((e for e in pool.list_endpoints() if e["id"] == ep_id), None)
        if old_ep and new_name and new_name != old_ep["name"]:
            token_tracker.rename_endpoint(old_ep["name"], new_name)
        pool.update_endpoint(ep_id, body); _sync_to_config(); return 200, {"ok": True}, False
    if method == "DELETE" and cp.startswith("/api/endpoints/"):
        ep_id = unquote(cp.split("/")[-1]); pool.remove_endpoint(ep_id); _sync_to_config(); return 200, {"ok": True}, False
    if method == "POST" and cp.endswith("/toggle"):
        ep_id = unquote(cp.split("/")[3])
        for ep in pool.list_endpoints():
            if ep["id"] == ep_id: pool.set_enabled(ep_id, not ep["enabled"]); break
        _sync_to_config(); return 200, {"ok": True}, False
    if method == "POST" and cp == "/api/health-check": return 200, {"ok": True, "results": pool.check_all_health()}, False
    if method == "POST" and cp == "/api/fetch-models":
        base_url = body.get("base_url", ""); api_key = body.get("api_key", "")
        if not base_url or not api_key: return 400, {"error": "需要 base_url 和 api_key"}, False
        try:
            models = pool.fetch_models(base_url, api_key, use_proxy=body.get("use_proxy", True), protocol=body.get("protocol", "openai"))
            sys_log(f"获取模型成功: {base_url} ({len(models)} 个)", "INFO")
            return 200, {"ok": True, "models": models, "count": len(models)}, False
        except urllib.error.HTTPError as e:
            err_body = ""
            try: err_body = e.read().decode("utf-8", errors="ignore")[:200]
            except Exception: pass
            sys_log(f"获取模型失败: {base_url} HTTP {e.code}: {err_body}", "ERROR")
            return 200, {"ok": False, "error": f"HTTP {e.code}: {err_body}"}, False
        except Exception as e:
            sys_log(f"获取模型失败: {base_url} {e}", "ERROR")
            return 200, {"ok": False, "error": str(e)}, False
    if method == "POST" and cp == "/api/test-model": return 200, pool.test_model_latency(body.get("base_url", ""), body.get("api_key", ""), body.get("model", ""), timeout=body.get("timeout", 60), use_proxy=body.get("use_proxy", True), protocol=body.get("protocol", "openai")), False
    if method == "POST" and cp == "/api/test-vision": return 200, pool.test_vision(body.get("base_url", ""), body.get("api_key", ""), body.get("model", ""), timeout=body.get("timeout", 60), use_proxy=body.get("use_proxy", True), protocol=body.get("protocol", "openai")), False
    if method == "POST" and cp == "/api/test":
        ep_id = body.get("id", ""); test_msg = body.get("message", "你好"); target_ep = None
        for ep in pool.list_endpoints():
            if ep["id"] == ep_id: target_ep = ep; break
        if not target_ep: return 404, {"error": "端点不存在"}, False
        test_pool = APIPool(default_payload={"temperature": 0.7})
        test_pool.add_endpoint({"name": target_ep["name"], "base_url": target_ep["base_url"], "api_key": target_ep["api_key_full"], "model": target_ep["model"], "public_model": target_ep.get("public_model", target_ep["model"]), "upstream_model": target_ep.get("upstream_model", target_ep["model"]), "priority": 1, "timeout": target_ep["timeout"], "max_retries": target_ep["max_retries"], "enabled": True, "in_pool": True, "use_proxy": target_ep.get("use_proxy", True), "protocol": target_ep.get("protocol", "openai"), "is_vision": target_ep.get("is_vision", True)})
        
        img = body.get("image")
        if img:
            test_msg = [{"type": "text", "text": test_msg}, {"type": "image_url", "image_url": {"url": img}}]
            
        try:
            res_dict, served_ep = test_pool.chat([{"role": "user", "content": test_msg}], return_endpoint=True)
            res_str = res_dict.get("choices", [{}])[0].get("message", {}).get("content", "") if isinstance(res_dict, dict) else res_dict
            return 200, {"ok": True, "result": res_str, "served_by": f"{served_ep.name} ({served_ep.model})"}, False
        except Exception as e: return 200, {"ok": False, "error": str(e)}, False
    if method == "POST" and cp == "/api/test-pool":
        test_msg = body.get("message", "你好")
        img = body.get("image")
        if img:
            test_msg = [{"type": "text", "text": test_msg}, {"type": "image_url", "image_url": {"url": img}}]
        try:
            res_dict, served_ep = pool.chat([{"role": "user", "content": test_msg}], return_endpoint=True)
            res_str = res_dict.get("choices", [{}])[0].get("message", {}).get("content", "") if isinstance(res_dict, dict) else res_dict
            return 200, {"ok": True, "result": res_str, "served_by": f"{served_ep.name} ({served_ep.model})"}, False
        except AllEndpointsFailed as e: return 200, {"ok": False, "errors": e.errors}, False
        except Exception as e: return 200, {"ok": False, "error": str(e)}, False
    if method == "GET" and cp == "/api/model-aliases":
        seen = set(); models = []
        for ep in pool.list_endpoints():
            m = (ep.get("model") or "").strip()
            if m and m not in seen:
                seen.add(m)
                models.append({"model": m, "enabled": ep.get("enabled", False), "in_pool": ep.get("in_pool", False)})
        with pool._lock:
            aliases = dict(pool.model_aliases)
        return 200, {"ok": True, "model_aliases": aliases, "available_models": models}, False
    if method == "POST" and cp == "/api/model-aliases":
        # 全量替换映射表: {"model_aliases": {"别名": "真实模型", ...}}
        raw = body.get("model_aliases")
        if not isinstance(raw, dict):
            return 400, {"ok": False, "error": "model_aliases 必须是对象 {别名: 真实模型}"}, False
        pool_models = {(ep.get("model") or "").strip() for ep in pool.list_endpoints()}
        cleaned = {}
        for k, v in raw.items():
            alias = str(k).strip(); target = str(v).strip()
            if not alias or not target:
                continue
            if alias == target:
                return 400, {"ok": False, "error": f"别名 {alias} 不能与目标模型同名"}, False
            if alias in pool_models:
                return 400, {"ok": False, "error": f"别名 {alias} 与池内已有模型冲突"}, False
            if target not in pool_models:
                return 400, {"ok": False, "error": f"目标模型 {target} 未在聚合池中配置"}, False
            cleaned[alias] = target
        with pool._lock:
            pool.model_aliases = cleaned
        _sync_to_config()
        sys_log(f"模型映射已更新（{len(cleaned)} 条）", "INFO")
        return 200, {"ok": True, "model_aliases": cleaned}, False
    if method == "POST" and cp == "/api/reset": pool.reset(); return 200, {"ok": True}, False

    return 404, {"error": "Not found"}, False

def _sync_to_config():
    save_config([{"id": ep.get("id"), "name": ep["name"], "base_url": ep["base_url"], "api_key": ep.get("api_key_full", ep.get("api_key", "")), "model": ep["model"], "public_model": ep.get("public_model", ep["model"]), "upstream_model": ep.get("upstream_model", ep["model"]), "priority": ep["priority"], "timeout": ep["timeout"], "max_retries": ep["max_retries"], "enabled": ep["enabled"], "cooldown_minutes": ep["cooldown_minutes"], "daily_limit": ep.get("daily_limit", 0), "rpm_limit": ep.get("rpm_limit", 0), "use_proxy": ep.get("use_proxy", True), "protocol": ep.get("protocol", "openai"), "health_mode": ep.get("health_mode", "chat"), "billing_mode": ep.get("billing_mode", "subscription"), "is_vision": ep.get("is_vision", True), "in_pool": ep.get("in_pool", True)} for ep in pool.list_endpoints()], station_settings=pool.station_settings, model_aliases=pool.model_aliases)


LOGIN_HTML = r"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>登录 API Pool</title>
<style>
*{box-sizing:border-box}body{margin:0;min-height:100vh;display:flex;align-items:center;justify-content:center;background:#090a0f;color:#fff;font-family:-apple-system,BlinkMacSystemFont,'SF Pro Text',system-ui,sans-serif;padding:24px}
.login{width:360px;max-width:100%;border:1px solid rgba(255,255,255,.1);background:rgba(255,255,255,.04);border-radius:16px;padding:26px;box-shadow:0 16px 48px rgba(0,0,0,.45)}
h1{font-size:20px;margin:0 0 6px}.sub{font-size:12px;color:rgba(255,255,255,.55);margin-bottom:22px}
label{display:block;font-size:11px;color:rgba(255,255,255,.6);font-weight:700;margin:12px 0 6px;letter-spacing:.5px;text-transform:uppercase}
input{width:100%;padding:11px 12px;border-radius:10px;border:1px solid rgba(255,255,255,.12);background:rgba(0,0,0,.22);color:#fff;outline:none;font-size:14px}
input:focus{border-color:#7d7aff}button{width:100%;margin-top:18px;padding:11px 12px;border:0;border-radius:10px;background:#5e5ce6;color:#fff;font-weight:700;cursor:pointer}
.err{min-height:18px;margin-top:12px;color:#ff6b62;font-size:12px}
</style>
</head>
<body>
<form class="login" id="loginForm">
  <h1>API Pool</h1>
  <div class="sub">登录后管理端点和访问密钥</div>
  <label>账号</label>
  <input id="username" autocomplete="username" autofocus>
  <label>密码</label>
  <input id="password" type="password" autocomplete="current-password">
  <button type="submit">登录</button>
  <div class="err" id="err"></div>
</form>
<script>
document.getElementById('loginForm').addEventListener('submit', async (e) => {
  e.preventDefault();
  const err = document.getElementById('err');
  err.textContent = '';
  const r = await fetch('/api/auth/login', {
    method:'POST',
    headers:{'Content-Type':'application/json'},
    body:JSON.stringify({
      username:document.getElementById('username').value.trim(),
      password:document.getElementById('password').value
    })
  });
  const data = await r.json().catch(() => ({}));
  if (r.ok && data.ok) location.href = '/';
  else err.textContent = data.error || '账号或密码不正确';
});
</script>
</body>
</html>"""


GUI_HTML = r"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>API Pool 聚合管理</title>
<style>
:root{
  --bg:#090a0f;--card:rgba(255,255,255,0.03);--card-hover:rgba(255,255,255,0.06);--border:rgba(255,255,255,0.08);
  --text:#ffffff;--text-dim:rgba(255,255,255,0.5);--accent:#5e5ce6;--accent-light:#7d7aff;
  --green:#32d74b;--green-dim:rgba(50,215,75,0.15);--red:#ff453a;--red-dim:rgba(255,69,58,0.15);
  --yellow:#ffd60a;--yellow-dim:rgba(255,214,10,0.15);--blue:#0a84ff;--blue-dim:rgba(10,132,255,0.15);
  --radius:16px;--shadow:0 8px 32px 0 rgba(0,0,0,0.3);--glass-blur:blur(24px);
}
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:-apple-system,BlinkMacSystemFont,'SF Pro Text','Inter',system-ui,sans-serif;background-color:var(--bg);background-image:radial-gradient(circle at 15% 50%, rgba(94,92,230,0.2), transparent 50%),radial-gradient(circle at 85% 30%, rgba(10,132,255,0.2), transparent 50%),radial-gradient(circle at 50% 80%, rgba(255,69,58,0.15), transparent 50%);background-attachment:fixed;color:var(--text);min-height:100vh;padding:20px 24px;font-size:14px;line-height:1.5;-webkit-font-smoothing:antialiased}

.header{display:flex;align-items:center;justify-content:space-between;margin-bottom:24px;flex-wrap:wrap;gap:12px}
.header h1{font-size:20px;font-weight:700;letter-spacing:-.3px;display:flex;align-items:center;gap:10px}
.header h1 .logo{width:32px;height:32px;border-radius:8px;background:linear-gradient(135deg,var(--accent),var(--blue));display:flex;align-items:center;justify-content:center;font-size:16px}
.header-actions{display:flex;gap:8px;flex-wrap:wrap}

.btn{padding:7px 14px;border:none;border-radius:7px;font-size:12px;font-weight:600;cursor:pointer;transition:all .12s;display:inline-flex;align-items:center;gap:5px;letter-spacing:.2px}
.btn:hover{transform:translateY(-1px);filter:brightness(1.1)}
.btn:active{transform:translateY(0)}
.btn-primary{background:var(--accent);color:#fff}
.btn-green{background:var(--green);color:#000}
.btn-red{background:var(--red);color:#fff}
.btn-yellow{background:var(--yellow);color:#000}
.btn-ghost{background:transparent;color:var(--text-dim);border:1px solid var(--border)}
.btn-ghost:hover{border-color:var(--accent);color:var(--accent-light)}
.btn-sm{padding:4px 10px;font-size:11px;border-radius:6px}
.btn:disabled{opacity:.35;cursor:not-allowed;transform:none}

.api-info-card {
  background: rgba(94, 92, 230, 0.08); backdrop-filter: var(--glass-blur); -webkit-backdrop-filter: var(--glass-blur);
  border: 1px solid rgba(94, 92, 230, 0.3); border-radius: var(--radius); padding: 14px 18px; margin-bottom: 20px; box-shadow: var(--shadow);
}
.api-info-card code {
  background: var(--bg);
  padding: 3px 8px;
  border-radius: 4px;
  font-family: 'SF Mono', Menlo, Consolas, monospace;
  font-size: 12px;
  color: var(--accent-light);
  user-select: all;
  border: 1px solid var(--border);
}
.client-key-row{display:inline-flex;align-items:center;gap:8px;flex-wrap:wrap}
.client-key-actions{display:inline-flex;align-items:center;gap:6px;margin-left:2px}
.key-action-btn{height:24px;padding:0 9px;border:1px solid rgba(125,122,255,.35);border-radius:7px;background:rgba(94,92,230,.12);color:var(--accent-light);font-size:11px;font-weight:700;cursor:pointer;display:inline-flex;align-items:center;gap:4px;transition:all .12s;white-space:nowrap}
.key-action-btn:hover{background:rgba(94,92,230,.22);border-color:var(--accent-light);color:#fff;transform:translateY(-1px)}
.key-action-btn:active{transform:translateY(0)}
.key-action-btn.copy{border-color:rgba(50,215,75,.28);background:rgba(50,215,75,.1);color:var(--green)}
.key-action-btn.copy:hover{border-color:var(--green);color:#fff;background:rgba(50,215,75,.18)}
#aliasList{display:flex;flex-wrap:wrap;gap:8px;align-items:center;font-size:12px;min-height:28px}
.alias-chip{display:inline-flex;align-items:center;gap:6px;padding:5px 8px 5px 10px;border-radius:999px;border:1px solid rgba(125,122,255,.28);background:linear-gradient(135deg,rgba(94,92,230,.16),rgba(10,132,255,.10));color:var(--text);line-height:1.2;max-width:100%;box-shadow:0 1px 0 rgba(255,255,255,.04) inset}
.alias-chip code{font-size:12px;padding:1px 6px;border-radius:6px;background:rgba(0,0,0,.28);border:1px solid rgba(255,255,255,.08);color:#e8e8ff;white-space:nowrap;max-width:220px;overflow:hidden;text-overflow:ellipsis}
.alias-chip .alias-arrow{color:var(--text-dim);font-size:11px;flex:0 0 auto}
.alias-chip .alias-del{height:20px;width:20px;padding:0;border:none;border-radius:50%;background:rgba(255,69,58,.12);color:var(--red);font-size:12px;font-weight:700;cursor:pointer;display:inline-flex;align-items:center;justify-content:center;line-height:1}
.alias-chip .alias-del:hover{background:rgba(255,69,58,.28);color:#fff}
.alias-empty{color:var(--text-dim);font-size:12px;padding:2px 0}
.model-usage-table th{font-weight:600; letter-spacing:.2px; text-transform:none;}
.model-usage-table td{padding:8px 6px; border-bottom:1px solid rgba(255,255,255,.04); vertical-align:middle;}
.model-usage-table tr:hover td{background:rgba(255,255,255,.03);}
.model-usage-bar-wrap{height:4px; border-radius:99px; background:rgba(255,255,255,.06); margin-top:5px; overflow:hidden;}
.model-usage-bar{height:100%; border-radius:99px; background:linear-gradient(90deg, rgba(94,92,230,.95), rgba(10,132,255,.85));}
.model-usage-name{font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace; font-size:12px; color:#e8e8ff;}
.model-usage-num{font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace; font-weight:600;}
.model-usage-sub{font-size:10px; color:var(--text-dim); margin-top:2px;}

.grid{display:grid;grid-template-columns:1fr 360px;gap:20px;align-items:start}
@media(max-width:920px){.grid{grid-template-columns:1fr}}

.card{background:var(--card);backdrop-filter:var(--glass-blur);-webkit-backdrop-filter:var(--glass-blur);border:1px solid var(--border);border-radius:var(--radius);padding:16px 18px;box-shadow:var(--shadow)}
.card-title{font-size:13px;font-weight:700;margin-bottom:14px;display:flex;align-items:center;gap:7px;color:var(--text-dim);text-transform:uppercase;letter-spacing:.6px}
.card-title .icon{font-size:15px}

.stats{display:grid;grid-template-columns:repeat(4,1fr);gap:8px;margin-bottom:16px}
.stat-item{background:rgba(255,255,255,.02);backdrop-filter:var(--glass-blur);-webkit-backdrop-filter:var(--glass-blur);border:1px solid var(--border);border-radius:12px;padding:12px 10px;text-align:center;transition:transform .2s,box-shadow .2s;box-shadow:0 2px 8px rgba(0,0,0,.1)}
.stat-item:hover{transform:translateY(-2px);box-shadow:0 4px 12px rgba(0,0,0,.2)}
.stat-item .num{font-size:20px;font-weight:700;font-variant-numeric:tabular-nums}
.stat-item .label{font-size:10px;color:var(--text-dim);margin-top:2px;text-transform:uppercase;letter-spacing:.5px}

.dash-stats{display:grid;grid-template-columns:repeat(4,1fr);gap:16px;margin-bottom:24px}
.dash-stat{position:relative;overflow:hidden;background:linear-gradient(145deg, rgba(255,255,255,0.03) 0%, rgba(255,255,255,0.01) 100%);backdrop-filter:var(--glass-blur);-webkit-backdrop-filter:var(--glass-blur);border:1px solid rgba(255,255,255,0.06);border-radius:16px;padding:20px;text-align:left;transition:transform .3s cubic-bezier(0.2,0.8,0.2,1),box-shadow .3s;box-shadow:0 4px 16px rgba(0,0,0,0.2)}
.dash-stat::before{content:'';position:absolute;top:0;left:0;right:0;height:1px;background:linear-gradient(90deg, transparent, rgba(255,255,255,0.15), transparent);opacity:0;transition:opacity .3s}
.dash-stat:hover{transform:translateY(-3px);box-shadow:0 8px 24px rgba(0,0,0,0.3);border-color:rgba(255,255,255,0.12)}
.dash-stat:hover::before{opacity:1}
.dash-stat .stat-icon{position:absolute;right:15px;top:15px;font-size:24px;opacity:0.2;transition:opacity .3s, transform .3s}
.dash-stat:hover .stat-icon{opacity:0.4;transform:scale(1.1)}
.dash-stat .num{font-size:28px;font-weight:800;font-variant-numeric:tabular-nums;margin-bottom:4px;letter-spacing:-0.5px}
.dash-stat .label{font-size:11px;color:var(--text-dim);text-transform:uppercase;letter-spacing:1px;font-weight:600}

.tbl-progress-container{position:relative;width:100%;height:100%;display:flex;align-items:center}
.tbl-progress-bar{position:absolute;left:0;top:0;bottom:0;background:rgba(94,92,230,0.15);border-radius:4px;z-index:0;transition:width 0.5s cubic-bezier(0.2,0.8,0.2,1)}
.tbl-content{position:relative;z-index:1;padding:6px;width:100%;display:flex;justify-content:space-between;align-items:center}


.filter-bar{display:flex;gap:5px;margin-bottom:12px;flex-wrap:wrap;align-items:center}
.filter-btn{padding:4px 12px;border-radius:16px;font-size:11px;font-weight:600;cursor:pointer;border:1px solid var(--border);background:transparent;color:var(--text-dim);transition:all .12s}
.filter-btn:hover{border-color:var(--accent);color:var(--accent-light)}
.filter-btn.active{background:var(--accent);border-color:var(--accent);color:#fff}
.filter-count{font-size:11px;color:var(--text-dim);margin-left:auto}

.ep-list{display:flex;flex-direction:column;gap:6px}
.ep-item{background:rgba(255,255,255,.02);border:1px solid var(--border);border-radius:12px;padding:12px 14px;transition:all .2s cubic-bezier(0.2,0.8,0.2,1)}
.ep-item:hover{border-color:rgba(255,255,255,.15);background:var(--card-hover);transform:translateY(-1px);box-shadow:0 4px 12px rgba(0,0,0,.2)}
.ep-item.disabled{opacity:.4}
.ep-item.current{border-color:var(--green);background:var(--green-dim)}
.ep-item.in-cooldown{border-color:var(--yellow);background:var(--yellow-dim)}
.ep-item.has-error{border-color:var(--red);background:var(--red-dim)}
.ep-header{display:flex;align-items:center;justify-content:space-between;margin-bottom:6px;gap:6px;flex-wrap:wrap}
.ep-name{font-weight:700;font-size:13px;display:flex;align-items:center;gap:6px;flex-wrap:wrap}
.badge{font-size:9px;padding:2px 7px;border-radius:12px;font-weight:700;display:inline-flex;align-items:center;gap:3px;text-transform:uppercase;letter-spacing:.3px}
.badge-current{background:var(--green-dim);color:var(--green)}
.badge-disabled{background:var(--red-dim);color:var(--red)}
.badge-cooldown{background:var(--yellow-dim);color:var(--yellow)}
.badge-priority{background:var(--accent);color:#fff;min-width:20px;justify-content:center}
.badge-h-ok{background:var(--green-dim);color:var(--green)}
.badge-h-slow{background:var(--yellow-dim);color:var(--yellow)}
.badge-h-bad{background:var(--red-dim);color:var(--red)}
.badge-h-unknown{background:rgba(255,255,255,.06);color:var(--text-dim)}
.ep-meta{display:flex;flex-wrap:wrap;gap:4px 14px;font-size:11px;color:var(--text-dim)}
.ep-meta span{display:flex;align-items:center;gap:3px}
.ep-actions{display:flex;gap:4px;flex-wrap:wrap}
.ep-error{margin-top:6px;font-size:11px;color:var(--red);background:var(--red-dim);padding:5px 8px;border-radius:6px;word-break:break-all}

.station-list{display:grid;grid-template-columns:repeat(auto-fill,minmax(300px,1fr));gap:8px}
.station-item{display:flex;flex-direction:column;gap:6px;background:rgba(255,255,255,.02);border:1px solid var(--border);border-radius:12px;padding:10px 12px;cursor:pointer;transition:all .2s cubic-bezier(0.2,0.8,0.2,1)}
.station-item:hover{border-color:rgba(255,255,255,.15);background:var(--card-hover);transform:translateY(-1px);box-shadow:0 4px 12px rgba(0,0,0,.2)}
.station-item.selected{border-color:var(--accent);background:rgba(94,92,230,.12)}
.station-item.paused{border-color:var(--red);background:var(--red-dim);opacity:.75}
.station-item.health-off{border-color:var(--yellow);background:var(--yellow-dim)}
.station-main{min-width:0}
.station-name{font-weight:700;font-size:13px;display:flex;align-items:center;gap:5px;flex-wrap:wrap;margin-bottom:4px;word-break:break-all}
.station-meta{display:flex;flex-wrap:wrap;gap:3px 10px;font-size:11px;color:var(--text-dim)}
.station-meta span{display:flex;align-items:center;gap:2px}
.station-actions{display:flex;flex-direction:row;flex-wrap:wrap;align-items:center;gap:4px;border-top:1px solid var(--border);padding-top:6px;margin-top:auto}
.station-actions .btn{flex:1 1 auto;min-width:62px;justify-content:center;padding:3px 6px;font-size:12px;line-height:1.4;white-space:nowrap}

.chain-list{display:flex;flex-direction:column;gap:0}
.chain-item{display:flex;align-items:center;gap:10px;padding:10px 12px;border-left:2px solid var(--border);font-size:12px;position:relative;transition:all .12s}
.chain-item:last-child{border-left-color:transparent}
.chain-item.active{border-left-color:var(--green);background:var(--green-dim);border-radius:0 8px 8px 0}
.chain-item.cooldown{border-left-color:var(--yellow);background:var(--yellow-dim);border-radius:0 8px 8px 0}
.chain-item.failed{border-left-color:var(--red);background:var(--red-dim);border-radius:0 8px 8px 0}
.chain-dot{width:8px;height:8px;border-radius:50%;background:var(--border);flex-shrink:0;position:absolute;left:-5px}
.chain-item.active .chain-dot{background:var(--green);box-shadow:0 0 6px var(--green)}
.chain-item.cooldown .chain-dot{background:var(--yellow)}
.chain-item.failed .chain-dot{background:var(--red)}
.chain-info{flex:1;min-width:0}
.chain-info .name{font-weight:600;font-size:12px}
.chain-info .model{font-size:10px;color:var(--text-dim);overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.chain-right{display:flex;flex-direction:column;align-items:flex-end;gap:2px;flex-shrink:0}
.chain-health{font-size:11px;font-weight:600}
.chain-err{font-size:10px;color:var(--red);max-width:120px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.chain-connector{height:12px;border-left:2px dashed var(--border);margin-left:0}

.modal-overlay{position:fixed;inset:0;background:rgba(0,0,0,.65);backdrop-filter:blur(6px);display:flex;align-items:center;justify-content:center;z-index:1000;opacity:0;pointer-events:none;transition:opacity .15s}
.modal-overlay.show{opacity:1;pointer-events:all}
.modal{background:var(--card);backdrop-filter:var(--glass-blur);-webkit-backdrop-filter:var(--glass-blur);border:1px solid var(--border);border-radius:16px;padding:24px;width:560px;max-width:94vw;max-height:88vh;overflow-y:auto;box-shadow:0 16px 48px rgba(0,0,0,.5)}
.modal h2{font-size:16px;margin-bottom:18px;font-weight:700}
.form-group{margin-bottom:12px}
.form-group label{display:block;font-size:11px;font-weight:600;color:var(--text-dim);margin-bottom:4px;text-transform:uppercase;letter-spacing:.5px}
.form-group input,.form-group select{width:100%;padding:9px 11px;background:rgba(0,0,0,0.2);border:1px solid var(--border);border-radius:10px;color:var(--text);font-size:13px;outline:none;transition:border-color .12s;box-shadow:inset 0 1px 2px rgba(0,0,0,0.1)}
.form-group input:focus,.form-group select:focus{border-color:var(--accent)}
.form-row{display:grid;grid-template-columns:1fr 1fr;gap:10px}
.form-actions{display:flex;gap:8px;justify-content:flex-end;margin-top:18px}

.model-row{display:flex;gap:8px;align-items:center}
.model-row input{flex:1}
.model-browser{margin-top:8px;border:1px solid var(--border);border-radius:8px;overflow:hidden}
.mb-toolbar{display:flex;gap:6px;padding:8px 10px;background:rgba(255,255,255,.02);border-bottom:1px solid var(--border);align-items:center;flex-wrap:wrap}
.mb-toolbar input[type=text]{flex:1;min-width:100px;padding:6px 9px;background:var(--bg);border:1px solid var(--border);border-radius:6px;color:var(--text);font-size:11px;outline:none}
.mb-toolbar input[type=text]:focus{border-color:var(--accent)}
.mb-toolbar label{font-size:11px;color:var(--text-dim);cursor:pointer;display:flex;align-items:center;gap:3px;white-space:nowrap}
.mb-toolbar .count{font-size:10px;color:var(--text-dim);white-space:nowrap}
.mb-table{max-height:300px;overflow-y:auto}
.mb-head{display:grid;grid-template-columns:28px 1fr 72px 80px 70px;gap:6px;padding:6px 10px;font-size:10px;color:var(--text-dim);text-transform:uppercase;letter-spacing:.4px;font-weight:600;border-bottom:1px solid var(--border);position:sticky;top:0;background:var(--card);z-index:1}
.mb-row{display:grid;grid-template-columns:28px 1fr 72px 80px 70px;gap:6px;padding:6px 10px;align-items:center;border-bottom:1px solid rgba(255,255,255,.03);font-size:12px;cursor:pointer;transition:background .08s}
.mb-row:last-child{border-bottom:none}
.mb-row:hover{background:var(--card-hover)}
.mb-row.selected{background:var(--accent);color:#fff}
.mb-row input[type=checkbox]{accent-color:var(--accent);cursor:pointer}
.mb-row .name-cell{overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.mb-row .mm-cell{text-align:center;font-size:11px}
.mm-yes{color:var(--green)}
.mm-no{color:var(--text-dim)}
.mm-unknown{color:var(--text-dim);opacity:.5}
.mb-row .price-cell{font-size:10px;color:var(--text-dim);white-space:nowrap}
.mb-row.selected .price-cell{color:rgba(255,255,255,.6)}
.free-tag{font-size:9px;background:var(--green-dim);color:var(--green);padding:1px 5px;border-radius:8px}
.mb-row.selected .free-tag{background:rgba(255,255,255,.2);color:#fff}
.mb-row .lat-cell{font-size:10px;white-space:nowrap}
.lat-ok{color:var(--green)}
.lat-slow{color:var(--yellow)}
.lat-bad{color:var(--red)}
.batch-bar{display:flex;align-items:center;justify-content:space-between;padding:8px 10px;background:var(--accent);color:#fff;border-radius:7px;margin-top:8px;font-size:12px;font-weight:600}
.pagination{display:flex;align-items:center;justify-content:center;gap:4px;padding:8px;border-top:1px solid var(--border);background:rgba(255,255,255,.015)}
.pagination .btn{min-width:28px;justify-content:center}
.pagination .page-info{font-size:11px;color:var(--text-dim)}

.toast{position:fixed;bottom:20px;right:20px;padding:10px 16px;border-radius:8px;font-size:12px;font-weight:600;z-index:2000;opacity:0;transform:translateY(8px);transition:all .2s;max-width:320px;word-break:break-all}
.toast.show{opacity:1;transform:translateY(0)}
.toast-success{background:var(--green);color:#000}
.toast-error{background:var(--red);color:#fff}
.toast-info{background:var(--accent);color:#fff}

.empty{text-align:center;padding:32px 16px;color:var(--text-dim);font-size:13px}
.test-input-row{display:flex;gap:6px}
.test-input-row input{flex:1;padding:7px 10px;background:var(--bg);border:1px solid var(--border);border-radius:7px;color:var(--text);font-size:12px;outline:none}
.test-result{margin-top:8px;padding:8px 10px;border-radius:7px;font-size:11px;word-break:break-all;max-height:130px;overflow-y:auto;white-space:pre-wrap;font-family:'SF Mono',Menlo,Consolas,monospace}
.test-result.success{background:var(--green-dim);color:var(--green)}
.test-result.failure{background:var(--red-dim);color:var(--red)}

.log-card { background: var(--card); backdrop-filter: var(--glass-blur); -webkit-backdrop-filter: var(--glass-blur); border: 1px solid var(--border); border-radius: var(--radius); padding: 16px 18px; box-shadow: var(--shadow); display: flex; flex-direction: column; }
.log-container { height: 280px; overflow-y: auto; background: var(--bg); border: 1px solid var(--border); border-radius: 8px; padding: 12px; font-family: 'SF Mono', Menlo, Consolas, monospace; font-size: 11px; display: flex; flex-direction: column; gap: 6px; scroll-behavior: smooth; }
.log-line { display: flex; gap: 8px; line-height: 1.5; word-break: break-all; }
.log-time { color: var(--text-dim); flex-shrink: 0; user-select: none; }
.log-INFO { color: var(--blue); flex-shrink: 0; min-width: 48px; text-align: center; }
.log-WARN { color: var(--yellow); flex-shrink: 0; min-width: 48px; text-align: center; }
.log-ERROR { color: var(--red); flex-shrink: 0; min-width: 48px; text-align: center; }
.log-msg { color: var(--text); }

.tabs { display:flex; background:rgba(255,255,255,0.03); border-radius:10px; padding:3px; border:1px solid var(--border); }
.tab { padding:6px 14px; border-radius:7px; font-size:12px; font-weight:600; cursor:pointer; color:var(--text-dim); transition:all .2s; }
.tab:hover { color:var(--text); }
.tab.active { background:var(--accent); color:#fff; box-shadow:0 2px 8px rgba(0,0,0,0.2); }

select option { background: var(--bg); color: var(--text); }

.seg-ctrl { display:inline-flex; background:rgba(255,255,255,0.03); border-radius:8px; padding:3px; border:1px solid rgba(255,255,255,0.05); }
.seg-btn { padding:3px 12px; border-radius:5px; font-size:11px; font-weight:600; cursor:pointer; color:var(--text-dim); transition:all 0.2s; }
.seg-btn:hover { color:var(--text); }
.seg-btn.active { background:rgba(255,255,255,0.1); color:#fff; box-shadow:0 2px 4px rgba(0,0,0,0.2); }

#testDrawer {
  position: fixed; right: 20px; bottom: 20px; width: 360px; background: rgba(20,20,20,0.85); backdrop-filter: blur(10px); -webkit-backdrop-filter: blur(10px); border: 1px solid var(--border); border-radius: 12px; box-shadow: 0 10px 40px rgba(0,0,0,0.6); z-index: 1000; display: flex; flex-direction: column; transform: translateY(150%); transition: transform 0.3s cubic-bezier(0.175, 0.885, 0.32, 1.275);
}
#testDrawer.show { transform: translateY(0); }
.drawer-header { padding: 12px 16px; background: rgba(255,255,255,0.05); border-bottom: 1px solid var(--border); border-top-left-radius: 12px; border-top-right-radius: 12px; display: flex; justify-content: space-between; align-items: center; font-weight: bold; font-size: 13px; }
.drawer-body { padding: 16px; display: flex; flex-direction: column; gap: 10px; }
</style>

</head>
<body>

<div class="header">
  <div style="display:flex; align-items:center; gap:30px;">
      <h1><span class="logo">⚡</span> API Pool</h1>
      <div class="tabs">
          <div class="tab active" id="tabPool" onclick="switchTab('pool')">🔌 聚合池</div>
          <div class="tab" id="tabAnalytics" onclick="switchTab('analytics')">📊 数据面板</div>
      </div>
  </div>
  <div class="header-actions" id="poolActions">
    <button class="btn btn-ghost" onclick="runHealthCheck()">🩺 健康检测</button>
    <button class="btn btn-ghost" onclick="resetPool()">🔄 重置</button>
    <button class="btn btn-primary" onclick="openAddModal()">＋ 添加端点</button>
    <button class="btn btn-green" onclick="openTestDrawer('pool', '')">🧪 测试聚合池</button>
  </div>
  <div class="header-actions" id="analyticsActions" style="display:none;">
    <select id="analyticsFilter" class="btn btn-ghost" style="appearance:none; cursor:pointer; background:rgba(255,255,255,0.05);" onchange="loadAnalytics()">
        <option value="all">全端点统计</option>
    </select>
    <button class="btn btn-ghost" onclick="clearTokenStats()" style="color:var(--red);">🗑 清空统计</button>
    <button class="btn btn-green" onclick="exportCSV()">📥 导出流水</button>
  </div>
  <div class="header-actions">
    <button class="btn btn-ghost" onclick="openSecurityModal()">🔐 安全设置</button>
    <button class="btn btn-ghost" onclick="logout()">退出</button>
  </div>
</div>

<div id="viewPool">
<div class="api-info-card">
  <div style="font-size: 13px; font-weight: 700; color: var(--accent-light); margin-bottom: 10px; text-transform: uppercase; letter-spacing: 0.5px;">🔗 客户端接入配置 (Client Config)</div>
  <div style="display: flex; gap: 24px; flex-wrap: wrap; font-size: 13px;">
    <div><span style="color: var(--text-dim); margin-right: 6px;">接口地址 (Base URL):</span><code id="displayUrl">http://localhost:5200/v1</code></div>
    <div class="client-key-row">
      <span style="color: var(--text-dim); margin-right: 0;">API Key:</span>
      <code id="clientApiKeyHint">加载中</code>
      <span class="client-key-actions">
        <button class="key-action-btn" onclick="getClientApiKey()" title="显示当前完整 API Key">获取</button>
        <button class="key-action-btn copy" onclick="copyClientApiKey()" title="复制完整 API Key">复制</button>
      </span>
    </div>
  </div>
</div>


<div class="api-info-card">
  <div style="font-size: 13px; font-weight: 700; color: var(--accent-light); margin-bottom: 10px; text-transform: uppercase; letter-spacing: 0.5px;">🔀 模型映射 (Model Aliases)</div>
  <div id="aliasList" class="alias-list" aria-label="模型映射列表"></div>
  <div style="display:flex;gap:12px;flex-wrap:wrap;align-items:center;font-size:13px;margin-top:10px;">
    <input type="text" id="aliasName" placeholder="对外别名，如 gpt-4o" style="padding:6px 10px;border-radius:8px;border:1px solid rgba(255,255,255,.15);background:rgba(0,0,0,.25);color:#fff;outline:none;font-size:13px;min-width:180px;">
    <span style="color:var(--text-dim);">→</span>
    <select id="aliasTarget" style="padding:6px 10px;border-radius:8px;border:1px solid rgba(255,255,255,.15);background:rgba(0,0,0,.25);color:#fff;outline:none;font-size:13px;min-width:200px;"></select>
    <button class="key-action-btn" onclick="addModelAlias()" title="添加映射">添加</button>
  </div>
  <div style="font-size:11px;color:var(--text-dim);margin-top:8px;">客户端用别名请求时，自动路由到映射的池内模型（同模型多上游仍自动故障转移），响应中的模型名保持为别名；别名也会出现在 /v1/models 列表中。</div>
</div>

<div class="api-info-card" id="modelUsageCardMain">
  <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:10px; gap:12px; flex-wrap:wrap;">
    <div style="font-size: 13px; font-weight: 700; color: var(--accent-light); text-transform: uppercase; letter-spacing: 0.5px;">📊 模型 Token 用量统计</div>
    <div style="display:flex; gap:8px; align-items:center; flex-wrap:wrap;">
      <div class="seg-ctrl">
        <div class="seg-btn active" id="btnModelRangeTodayMain" onclick="switchModelRange('today')">今日</div>
        <div class="seg-btn" id="btnModelRangeMonthMain" onclick="switchModelRange('month')">本月</div>
      </div>
      <button class="key-action-btn" onclick="switchTab('analytics')" title="打开完整数据面板">详情</button>
    </div>
  </div>
  <div style="max-height: 280px; overflow:auto;">
    <table class="model-usage-table" style="width:100%; border-collapse:collapse; font-size:12px; text-align:left;">
      <thead>
        <tr style="border-bottom:1px solid var(--border); color:var(--text-dim); position:sticky; top:0; background:rgba(20,20,28,.92); backdrop-filter:blur(8px);">
          <th style="padding:8px 6px; width:28%;">模型</th>
          <th style="padding:8px 6px; text-align:right;">总 Token</th>
          <th style="padding:8px 6px; text-align:right;">请求</th>
          <th style="padding:8px 6px; text-align:right;">输入</th>
          <th style="padding:8px 6px; text-align:right;">输出</th>
          <th style="padding:8px 6px; text-align:right;">缓存命中</th>
          <th style="padding:8px 6px; text-align:right;">占比</th>
        </tr>
      </thead>
      <tbody id="modelUsageTableMain"></tbody>
    </table>
  </div>
  <div id="modelUsageEmptyMain" style="display:none; color:var(--text-dim); font-size:12px; padding:12px 4px;">暂无模型用量数据</div>
</div>

<div class="grid">
  <div>
    <div class="stats" id="stats"></div>
    <div class="card" style="margin-bottom:16px">
      <div class="card-title"><span class="icon">🏢</span> 中转站 <span style="font-size:10px;color:var(--text-dim);text-transform:none;letter-spacing:0;font-weight:400;margin-left:4px">按域名归组 · 点击卡片筛选端点 · 可按站停止测活/连接</span></div>
      <div class="station-list" id="stationList"></div>
    </div>
    <div class="card" style="margin-bottom:16px">
      <div class="card-title"><span class="icon">📋</span> 端点列表</div>
      <div class="filter-bar" id="filterBar"></div>
      <div class="ep-list" id="epList"></div>
    </div>
  </div>
  <div>
    <div class="card" style="margin-bottom:16px">
      <div class="card-title"><span class="icon">🔗</span> 聚合链</div>
      <div style="font-size:10px;color:var(--text-dim);margin-bottom:10px">遇 429/超时自动切换 · 冷却到期自动切回</div>
      <div class="chain-list" id="chainList"></div>
    </div>

  </div>
</div>

<div class="log-card" style="margin-top:20px;">
  <div class="card-title">
    <span class="icon">📝</span> 实时日志
    <button class="btn btn-ghost btn-sm" onclick="clearSysLogs()" style="color:var(--red); float:right; margin-top:-2px; padding:2px 8px;">🗑 清空</button>
  </div>
  <div class="log-container" id="logContainer"></div>
</div>

<div class="log-card" style="margin-top:20px; display:flex; flex-direction:column; padding-bottom:15px;">
    <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:15px;">
      <div class="card-title" style="margin-bottom:0;"><span class="icon">💬</span> 对话日志 (Audit Logs)</div>
      <button class="btn btn-ghost btn-sm" onclick="clearChatLogs()" style="color:var(--red); padding:2px 8px;">🗑 清空记录</button>
    </div>
    <div style="display:flex; gap:15px; height:450px; min-height:300px;">
      <!-- List View -->
      <div style="flex:1; display:flex; flex-direction:column; border:1px solid var(--border); border-radius:8px; overflow:hidden;">
        <div style="background:rgba(255,255,255,0.05); padding:8px 12px; font-weight:bold; border-bottom:1px solid var(--border); display:grid; grid-template-columns: 80px 1fr 1fr 80px; gap:8px; font-size:12px;">
          <span>时间</span><span>端点</span><span>模型</span><span>Tokens</span>
        </div>
        <div id="chatLogsList" style="flex:1; overflow-y:auto;">
          <!-- Items inserted here -->
        </div>
        <div style="padding:8px; text-align:center; border-top:1px solid var(--border);">
            <button class="btn btn-ghost btn-sm" onclick="loadChatLogs(chatLogsPage-1)" id="clPrevBtn">上一页</button>
            <span style="margin:0 10px;font-size:12px;" id="clPageSpan">1</span>
            <button class="btn btn-ghost btn-sm" onclick="loadChatLogs(chatLogsPage+1)" id="clNextBtn">下一页</button>
        </div>
      </div>
      <!-- Detail View -->
      <div style="flex:1; display:flex; flex-direction:column; gap:15px; overflow:hidden; min-height:0;">
        <div style="flex:1; display:flex; flex-direction:column; border:1px solid var(--border); border-radius:8px; background:rgba(0,0,0,0.3); min-height:0;">
          <div style="padding:6px 10px; background:var(--card); font-size:12px; color:var(--text-dim); border-bottom:1px solid var(--border);">Prompt</div>
          <pre id="clPrompt" style="flex:1; overflow-y:auto; padding:10px; margin:0; font-size:12px; white-space:pre-wrap; word-break:break-all; min-height:0;"></pre>
        </div>
        <div style="flex:1; display:flex; flex-direction:column; border:1px solid var(--border); border-radius:8px; background:rgba(0,0,0,0.3); min-height:0;">
          <div style="padding:6px 10px; background:var(--card); font-size:12px; color:var(--text-dim); border-bottom:1px solid var(--border);">Completion <span id="clMeta" style="float:right;"></span></div>
          <pre id="clCompletion" style="flex:1; overflow-y:auto; padding:10px; margin:0; font-size:12px; white-space:pre-wrap; word-break:break-all; min-height:0;"></pre>
        </div>
      </div>
    </div>
</div>

</div>

<div id="viewAnalytics" style="display:none; padding-bottom:40px;">
    <div class="dash-stats" id="tokenStatsOverview"></div>
    
    <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 20px;">
        <div class="card">
            <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:0px;">
                <div class="card-title" style="margin-bottom:0; font-size:11px;">今日各时段消耗趋势</div>
                <div class="seg-ctrl">
                    <div class="seg-btn active" id="btnTrendToken" onclick="switchTrend('tokens')">Token</div>
                    <div class="seg-btn" id="btnTrendMissed" onclick="switchTrend('missed')">未命中</div>
                    <div class="seg-btn" id="btnTrendCall" onclick="switchTrend('calls')">请求数</div>
                </div>
            </div>
            <div id="tokenTodayChart" style="height: 180px; position: relative;"></div>
            <div id="missedTodayChart" style="height: 180px; position: relative; display:none;"></div>
            <div id="callsTodayChart" style="height: 180px; position: relative; display:none;"></div>
        </div>
        <div class="card">
            <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:4px;">
                <div class="card-title" style="margin-bottom:0; font-size:11px;">近 14 天 Token 组成结构</div>
                <div style="display:flex; gap:10px; font-size:11px; color:var(--text-dim);">
                    <label style="cursor:pointer; display:flex; align-items:center; gap:3px;"><input type="checkbox" id="chkCompCache" checked onchange="updateCompChart()"> <span style="color:var(--green)">命中缓存</span></label>
                    <label style="cursor:pointer; display:flex; align-items:center; gap:3px;"><input type="checkbox" id="chkCompMissed" checked onchange="updateCompChart()"> <span style="color:var(--blue)">未命中</span></label>
                    <label style="cursor:pointer; display:flex; align-items:center; gap:3px;"><input type="checkbox" id="chkCompGen" checked onchange="updateCompChart()"> <span style="color:#aaa">生成</span></label>
                </div>
            </div>
            <div id="tokenTrendChart" style="height: 180px; margin-bottom: 0px; position: relative;"></div>
        </div>
    </div>
    
    <div class="card" style="margin-top:20px;">
        <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:10px; gap:12px; flex-wrap:wrap;">
            <div class="card-title" style="margin-bottom:0; font-size:11px;">模型 Token 用量统计</div>
            <div class="seg-ctrl">
                <div class="seg-btn active" id="btnModelRangeToday" onclick="switchModelRange('today')">今日</div>
                <div class="seg-btn" id="btnModelRangeMonth" onclick="switchModelRange('month')">本月</div>
            </div>
        </div>
        <div style="max-height: 320px; overflow:auto;">
          <table class="model-usage-table" style="width:100%; border-collapse:collapse; font-size:12px; text-align:left;">
            <thead>
              <tr style="border-bottom:1px solid var(--border); color:var(--text-dim); position:sticky; top:0; background:rgba(20,20,28,.92); backdrop-filter:blur(8px);">
                <th style="padding:8px 6px; width:28%;">模型</th>
                <th style="padding:8px 6px; text-align:right;">总 Token</th>
                <th style="padding:8px 6px; text-align:right;">请求</th>
                <th style="padding:8px 6px; text-align:right;">输入</th>
                <th style="padding:8px 6px; text-align:right;">输出</th>
                <th style="padding:8px 6px; text-align:right;">缓存命中</th>
                <th style="padding:8px 6px; text-align:right;">占比</th>
              </tr>
            </thead>
            <tbody id="modelUsageTable"></tbody>
          </table>
        </div>
        <div id="modelUsageEmpty" style="display:none; color:var(--text-dim); font-size:12px; padding:12px 4px;">暂无模型用量数据</div>
    </div>

    <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 20px; margin-top:20px;">
        <div class="card">
            <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:10px;">
                <div class="card-title" style="margin-bottom:0; font-size:11px;">今日模型端点排行榜</div>
                <div class="seg-ctrl">
                    <div class="seg-btn active" id="btnTblTodayToken" onclick="switchTblToday('tokens')">Token</div>
                    <div class="seg-btn" id="btnTblTodayCall" onclick="switchTblToday('calls')">请求数</div>
                </div>
            </div>
            <div style="max-height: 250px; overflow-y: auto;">
              <table style="width: 100%; border-collapse: collapse; font-size: 11px; text-align: left;">
                <thead><tr style="border-bottom: 1px solid var(--border); color: var(--text-dim);"><th style="padding: 6px;">模型端点</th><th style="padding: 6px; text-align:right;">数值</th></tr></thead>
                <tbody id="todayModelsTable"></tbody>
                <tbody id="todayCallsTable" style="display:none;"></tbody>
              </table>
            </div>
        </div>
        <div class="card">
            <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:10px;">
                <div class="card-title" style="margin-bottom:0; font-size:11px;">本月模型端点排行榜</div>
                <div class="seg-ctrl">
                    <div class="seg-btn active" id="btnTblMonthToken" onclick="switchTblMonth('tokens')">Token</div>
                    <div class="seg-btn" id="btnTblMonthCall" onclick="switchTblMonth('calls')">请求数</div>
                </div>
            </div>
            <div style="max-height: 250px; overflow-y: auto;">
              <table style="width: 100%; border-collapse: collapse; font-size: 11px; text-align: left;">
                <thead><tr style="border-bottom: 1px solid var(--border); color: var(--text-dim);"><th style="padding: 6px;">模型端点</th><th style="padding: 6px; text-align:right;">数值</th></tr></thead>
                <tbody id="monthModelsTable"></tbody>
                <tbody id="monthCallsTable" style="display:none;"></tbody>
              </table>
            </div>
        </div>
    </div>
</div>

<div class="modal-overlay" id="modal">
  <div class="modal">
    <h2 id="modalTitle">添加端点</h2>
    <input type="hidden" id="editName">
    <div class="form-group"><label>名称</label><input type="text" id="fName" placeholder="如 OpenAI 或 DeepSeek" oninput="this.dataset.autofilled='false'"></div>
    <div class="form-group"><label>Base URL</label><input type="text" id="fUrl" placeholder="https://api.openai.com/v1" oninput="checkFetchBtn()"></div>
    <div class="form-group">
      <label>上游 API Key</label>
      <input type="password" id="fKey" placeholder="填写模型服务商提供的 Key，不是 API Pool 客户端 Key" oninput="checkFetchBtn()">
      <div style="font-size:11px;color:var(--text-dim);margin-top:4px;">用于连接 Base URL 对应的模型服务商。</div>
    </div>
    <div class="form-group">
      <label>对外模型名</label>
      <div class="model-row">
        <input type="text" id="fModel" placeholder="gpt-4o">
        <button class="btn btn-yellow btn-sm" id="fetchModelsBtn" onclick="fetchModels()" disabled>🔍 获取</button>
      </div>
      <div id="modelBrowser" style="display:none"></div>
      <div id="batchBar" style="display:none"></div>
    </div>
    <div class="form-group">
      <label>上游模型名</label>
      <input type="text" id="fUpstreamModel" placeholder="留空则与对外模型名相同">
      <div style="font-size:11px;color:var(--text-dim);margin-top:4px;">端点级映射：实际发给该上游的模型名。客户端始终用"对外模型名"请求。</div>
    </div>
    <div class="form-row">
      <div class="form-group"><label>优先级</label><input type="number" id="fPriority" value="1" min="1"></div>
      <div class="form-group"><label>超时 (秒)</label><input type="number" id="fTimeout" value="60" min="1"></div>
    </div>
    <div class="form-row">
      <div class="form-group"><label>重试次数</label><input type="number" id="fRetries" value="0" min="0"></div>
      <div class="form-group"><label>冷却 (分钟)</label><input type="number" id="fCooldown" value="5" min="0"></div>
    </div>
      <div class="form-group">
        <label title="标识该模型是否原生支持读图。若选“不支持”，收到图片时会自动触发图片解析。">多模态 (视觉) 能力</label>
        <select id="fVision"><option value="true">👁️ 原生支持</option><option value="false">🚫 不支持 (触发自动转译)</option></select>
      </div>
    <div class="form-row">
      <div class="form-group"><label>启用</label><select id="fEnabled"><option value="true">是</option><option value="false">否</option></select></div>
      <div class="form-group"><label title="达到额度后挂起，0为不限制">每日额度 (0不限)</label><input type="number" id="fDailyLimit" value="0" min="0"></div>
    </div>
    <div class="form-row" style="grid-template-columns: 1fr 1fr 1fr;">
      <div class="form-group"><label title="每分钟最高请求次数，超限自动切换，0为不限制">并发 (0不限)</label><input type="number" id="fRpmLimit" value="0" min="0"></div>
      <div class="form-group"><label title="是否使用系统代理 (如v2ray)。本地或直连接口请选择否。">代理设置</label><select id="fProxy"><option value="true">随系统</option><option value="false">强制直连</option></select></div>
      <div class="form-group"><label title="底层协议类型">协议类型</label><select id="fProtocol"><option value="openai">OpenAI 兼容</option><option value="anthropic">Anthropic</option></select></div>
    </div>
    <div class="form-group">
      <label>后台探针</label>
      <select id="fHealthMode">
        <option value="chat">Ping (/chat/completions)</option>
        <option value="models">Models (/v1/models) 零成本</option>
        <option value="none">关闭检测</option>
      </select>
    </div>
    <div class="form-actions">
      <button class="btn btn-ghost" onclick="closeModal()">取消</button>
      <button class="btn btn-green" id="batchAddBtn" style="display:none" onclick="batchAddEndpoints()">📦 批量添加</button>
      <button class="btn btn-primary" id="singleAddBtn" onclick="saveEndpoint()">保存</button>
    </div>
  </div>
</div>


<div class="modal-overlay" id="securityModal">
  <div class="modal">
    <h2>安全设置</h2>
    <div class="form-group"><label>管理员账号</label><input type="text" id="secUsername" autocomplete="username"></div>
    <div class="form-group"><label>当前密码</label><input type="password" id="secCurrentPassword" autocomplete="current-password" placeholder="修改账号或密码时必填"></div>
    <div class="form-group"><label>新密码</label><input type="password" id="secNewPassword" autocomplete="new-password" placeholder="留空则不修改"></div>
    <div class="form-actions" style="justify-content:flex-start;margin-bottom:20px;">
      <button class="btn btn-primary" onclick="saveAdminSecurity()">保存账号密码</button>
    </div>
    <div class="form-group"><label>客户端 API Key</label><input type="text" id="secApiKey" placeholder="输入自定义 Key，或点击生成"></div>
    <div style="font-size:12px;color:var(--text-dim);margin-top:-6px;">当前：<code id="secApiKeyHint"></code></div>
    <div class="form-actions">
      <button class="btn btn-ghost" onclick="closeSecurityModal()">关闭</button>
      <button class="btn btn-yellow" onclick="setClientApiKey()">设置 Key</button>
      <button class="btn btn-green" onclick="generateClientApiKey()">生成新 Key</button>
    </div>
  </div>
</div>



<div class="modal-overlay" id="stationEditModal">
  <div class="modal">
    <h2>✏️ 编辑中转站 <code id="seKey" style="font-size:13px"></code></h2>
    <div style="font-size:11px;color:var(--text-dim);margin:-10px 0 14px;">修改将批量应用到该站的全部端点；留空的项保持各端点原值不变。显示「(多个值)」表示该站各端点当前配置不一致。</div>
    <div class="form-group"><label>站点名称</label><input type="text" id="seName" placeholder="留空不修改"></div>
    <div class="form-group"><label>Base URL</label><input type="text" id="seUrl" placeholder="留空不修改（修改后按新域名重新归组）"></div>
    <div class="form-group"><label>上游 API Key</label><input type="password" id="seKeyInput" placeholder="留空不修改"><div style="font-size:11px;color:var(--text-dim);margin-top:4px;">当前：<code id="seKeyHint">—</code></div></div>
    <div class="form-row">
      <div class="form-group"><label>超时 (秒)</label><input type="number" id="seTimeout" min="1" placeholder="不修改"></div>
      <div class="form-group"><label>重试次数</label><input type="number" id="seRetries" min="0" placeholder="不修改"></div>
    </div>
    <div class="form-row">
      <div class="form-group"><label>冷却 (分钟)</label><input type="number" id="seCooldown" min="0" placeholder="不修改"></div>
      <div class="form-group"><label>RPM 限制 (0=不限)</label><input type="number" id="seRpm" min="0" placeholder="不修改"></div>
    </div>
    <div class="form-row">
      <div class="form-group"><label>每日额度 (0=不限)</label><input type="number" id="seDaily" min="0" placeholder="不修改"></div>
      <div class="form-group"><label>使用系统代理</label><select id="seProxy"><option value="">不修改</option><option value="true">是</option><option value="false">否（直连）</option></select></div>
    </div>
    <div class="form-row">
      <div class="form-group"><label>协议</label><select id="seProtocol"><option value="">不修改</option><option value="openai">OpenAI 兼容</option><option value="anthropic">Anthropic</option></select></div>
      <div class="form-group"><label>健康检测方式</label><select id="seHealthMode"><option value="">不修改</option><option value="chat">Chat 探测</option><option value="models">Models 无感测</option><option value="none">关闭检测</option></select></div>
    </div>
    <div class="form-group">
      <label>模型列表</label>
      <div style="display:flex;gap:8px;align-items:center">
        <button class="btn btn-yellow btn-sm" onclick="seFetchModels()">🔍 获取模型</button>
        <span style="font-size:11px;color:var(--text-dim)">带着该站的 URL / Key / 配置打开添加端点窗口并自动拉取模型列表</span>
      </div>
    </div>
    <div class="form-actions">
      <button class="btn btn-ghost" onclick="closeStationEdit()">取消</button>
      <button class="btn btn-primary" onclick="saveStationEdit()">保存修改</button>
    </div>
  </div>
</div>

<div class="toast" id="toast"></div>

<script>
document.getElementById('displayUrl').textContent = window.location.protocol + '//' + window.location.host + '/v1';

const API='';
async function api(method,path,body){
  const opts={method,headers:{'Content-Type':'application/json'}};
  if(body)opts.body=JSON.stringify(body);
  const resp=await fetch(API+path,opts);
  if(resp.status===401){location.href='/login';return {ok:false,error:'未登录'};}
  return await resp.json();
}

let allModels=[],selectedModels=new Set(),latencyResults={},visionResults={};
let epFilter='all',modelPage=1,PP=50;
let clientApiKeyPlain='';
let stationFilter='';
let _stations=[];

async function refresh(){
  const[eps,chain,stations]=await Promise.all([api('GET','/api/endpoints'),api('GET','/api/chain'),api('GET','/api/stations')]);
  renderStats(eps);renderStations(stations);renderFilterBar(eps);renderEndpoints(eps);renderChain(chain);
  loadModelTokenStats();
}


let _modelAliases={};
async function loadModelAliases(){
  const r=await api('GET','/api/model-aliases');
  if(!r||r.ok===false)return;
  _modelAliases=r.model_aliases||{};
  const list=document.getElementById('aliasList');
  const entries=Object.entries(_modelAliases);
  if(!entries.length){list.innerHTML='<div class="alias-empty">暂无映射</div>';}
  else{
    list.innerHTML=entries.map(([a,t])=>`<div class="alias-chip" title="${esc(a)} → ${esc(t)}">
      <code>${esc(a)}</code><span class="alias-arrow">→</span><code>${esc(t)}</code>
      <button class="alias-del" onclick="removeModelAlias('${esc(a)}')" title="删除 ${esc(a)}">×</button>
    </div>`).join('');
  }
  const sel=document.getElementById('aliasTarget');
  const opts=['<option value="">— 选择目标模型 —</option>'];
  (r.available_models||[]).forEach(m=>{
    const tag=m.enabled&&m.in_pool?'':' (未启用/池外)';
    opts.push(`<option value="${esc(m.model)}">${esc(m.model)}${tag}</option>`);
  });
  sel.innerHTML=opts.join('');
}
async function addModelAlias(){
  const alias=document.getElementById('aliasName').value.trim();
  const target=document.getElementById('aliasTarget').value;
  if(!alias){toast('请填写别名','error');return;}
  if(!target){toast('请选择目标模型','error');return;}
  const next={..._modelAliases,[alias]:target};
  const r=await api('POST','/api/model-aliases',{model_aliases:next});
  if(r&&r.ok){toast('已添加映射：'+alias+' → '+target,'success');document.getElementById('aliasName').value='';loadModelAliases();}
  else toast(r&&r.error||'添加失败','error');
}
async function removeModelAlias(alias){
  const next={..._modelAliases};delete next[alias];
  const r=await api('POST','/api/model-aliases',{model_aliases:next});
  if(r&&r.ok){toast('已删除映射：'+alias,'success');loadModelAliases();}
  else toast(r&&r.error||'删除失败','error');
}

async function loadSecurity(){
  const r=await api('GET','/api/security');
  if(!r.ok)return;
  document.getElementById('clientApiKeyHint').textContent=r.client_api_key_hint||'未设置';
  document.getElementById('secApiKeyHint').textContent=r.client_api_key_hint||'未设置';
  document.getElementById('secUsername').value=r.username||'admin';
  if(!r.client_api_key_available) clientApiKeyPlain='';
}
async function getClientApiKey(){
  const r=await api('GET','/api/security/api-key/reveal');
  if(r.ok&&r.api_key){
    clientApiKeyPlain=r.api_key;
    document.getElementById('clientApiKeyHint').textContent=r.api_key;
    document.getElementById('secApiKeyHint').textContent=r.client_api_key_hint||r.api_key;
    toast('已显示完整 Key','success');
  }else{
    toast(r.error||'无法获取 Key','error');
    openSecurityModal();
  }
}
async function copyClientApiKey(){
  if(!clientApiKeyPlain){
    const r=await api('GET','/api/security/api-key/reveal');
    if(r.ok&&r.api_key) clientApiKeyPlain=r.api_key;
    else{toast(r.error||'无法复制 Key','error');return;}
  }
  try{
    await navigator.clipboard.writeText(clientApiKeyPlain);
    toast('API Key 已复制','success');
  }catch(e){
    const ta=document.createElement('textarea');
    ta.value=clientApiKeyPlain;ta.style.position='fixed';ta.style.opacity='0';
    document.body.appendChild(ta);ta.select();document.execCommand('copy');ta.remove();
    toast('API Key 已复制','success');
  }
}
function openSecurityModal(){
  document.getElementById('secCurrentPassword').value='';
  document.getElementById('secNewPassword').value='';
  document.getElementById('secApiKey').value='';
  loadSecurity();
  document.getElementById('securityModal').classList.add('show');
}
function closeSecurityModal(){document.getElementById('securityModal').classList.remove('show');}
async function saveAdminSecurity(){
  const r=await api('PUT','/api/security/admin',{
    username:document.getElementById('secUsername').value.trim(),
    current_password:document.getElementById('secCurrentPassword').value,
    password:document.getElementById('secNewPassword').value
  });
  if(r.ok){toast('账号密码已更新','success');document.getElementById('secCurrentPassword').value='';document.getElementById('secNewPassword').value='';loadSecurity();}
  else toast(r.error||'保存失败','error');
}
async function setClientApiKey(){
  const key=document.getElementById('secApiKey').value.trim();
  if(!key){toast('先输入 API Key','error');return;}
  const r=await api('PUT','/api/security/api-key',{api_key:key});
  if(r.ok){clientApiKeyPlain=key;toast('客户端 Key 已更新','success');document.getElementById('secApiKey').value='';loadSecurity();}
  else toast(r.error||'设置失败','error');
}
async function generateClientApiKey(){
  if(!confirm('生成新 Key 后，旧客户端 Key 会立即失效。继续吗？'))return;
  const r=await api('POST','/api/security/api-key/generate');
  if(r.ok){clientApiKeyPlain=r.api_key;document.getElementById('secApiKey').value=r.api_key;document.getElementById('clientApiKeyHint').textContent=r.api_key;toast('新 Key 已生成，请复制保存','success');loadSecurity();}
  else toast(r.error||'生成失败','error');
}
async function logout(){
  await fetch('/api/auth/logout',{method:'POST'});
  location.href='/login';
}

function renderStats(eps){
  const t=eps.length,en=eps.filter(e=>e.enabled).length,h=eps.filter(e=>e.health==='ok').length;
  const cl=eps.filter(e=>e.in_cooldown).length,bad=eps.filter(e=>e.health==='bad').length;
  const calls=eps.reduce((s,e)=>s+e.total_calls,0);
  document.getElementById('stats').innerHTML=`
    <div class="stat-item"><div class="num">${t}</div><div class="label">端点</div></div>
    <div class="stat-item"><div class="num" style="color:var(--green)">${en}</div><div class="label">启用</div></div>
    <div class="stat-item"><div class="num" style="color:var(--green)">${h}</div><div class="label">健康</div></div>
    <div class="stat-item"><div class="num" style="color:${bad?'var(--red)':'var(--text-dim)'}">${bad}</div><div class="label">异常</div></div>
    <div class="stat-item"><div class="num" style="color:${cl?'var(--yellow)':'var(--text-dim)'}">${cl}</div><div class="label">冷却</div></div>
    <div class="stat-item"><div class="num" style="color:var(--blue)">${calls}</div><div class="label">调用</div></div>`;
}

function renderStations(stations){
  _stations=stations||[];
  const el=document.getElementById('stationList');
  if(!el)return;
  if(!stations||!stations.length){el.innerHTML='<div class="empty">暂无中转站</div>';return;}
  el.innerHTML=stations.map(s=>{
    let cls='station-item';
    if(s.connect_paused)cls+=' paused';
    else if(s.health_paused)cls+=' health-off';
    if(stationFilter===s.key)cls+=' selected';
    const names=s.names.join(' / ')||s.key;
    let badges='';
    if(s.connect_paused)badges+='<span class="badge badge-disabled">⛔已停连接</span>';
    if(s.health_paused)badges+='<span class="badge badge-cooldown">🔕已停测活</span>';
    if(!s.connect_paused&&!s.health_paused&&s.bad===0&&s.healthy>0)badges+='<span class="badge badge-h-ok">✅正常</span>';
    return`<div class="${cls}" onclick="setStationFilter('${esc(s.key)}')" title="点击筛选该中转站的端点&#10;${esc(s.base_urls.join(' , '))}">
      <div class="station-main">
        <div class="station-name">${esc(names)} ${badges}</div>
        <div class="station-meta">
          <span title="域名">🌐${esc(s.key)}</span>
          <span title="端点数">📦${s.total}个</span>
          <span title="启用数" style="color:var(--green)">▶${s.enabled}</span>
          ${s.healthy?`<span title="健康" style="color:var(--green)">✅${s.healthy}</span>`:''}
          ${s.bad?`<span title="异常" style="color:var(--red)">❌${s.bad}</span>`:''}
          ${s.in_cooldown?`<span title="冷却中" style="color:var(--yellow)">⏳${s.in_cooldown}</span>`:''}
          <span title="累计成功调用">📞${s.total_calls}</span>
        </div>
      </div>
      <div class="station-actions" onclick="event.stopPropagation()">
        <button class="btn btn-sm ${s.health_paused?'btn-yellow':'btn-ghost'}" title="${s.health_paused?'恢复该站的健康检测':'暂停该站的健康检测（不再发送探活请求）'}" onclick="toggleStationHealth('${esc(s.key)}',${!s.health_paused})">${s.health_paused?'🔕测活':'🩺测活'}</button>
        <button class="btn btn-sm ${s.connect_paused?'btn-red':'btn-ghost'}" title="${s.connect_paused?'恢复该站的连接（重新加入聚合池）':'停止该站的连接（请求不再转发到该站）'}" onclick="toggleStationConnect('${esc(s.key)}',${!s.connect_paused})">${s.connect_paused?'⛔连接':'🔌连接'}</button>
        <button class="btn btn-sm btn-ghost" title="编辑该中转站配置（批量应用到该站全部端点）" onclick="openStationEdit('${esc(s.key)}')">✏️编辑</button>
        <button class="btn btn-sm btn-ghost" style="color:var(--red)" title="删除该中转站及其全部端点" onclick="deleteStation('${esc(s.key)}',${s.total})">🗑删除</button>
      </div>
    </div>`;
  }).join('');
}
function setStationFilter(key){
  stationFilter=(stationFilter===key)?'':key;
  refresh();
}
async function toggleStationHealth(key,pause){
  const r=await api('PUT',`/api/stations/${encodeURIComponent(key)}`,{health_paused:pause});
  if(r&&r.ok)toast(pause?`已暂停「${key}」测活`:`已恢复「${key}」测活`,'success');
  else toast(r.error||'操作失败','error');
  refresh();
}
async function toggleStationConnect(key,pause){
  if(pause&&!confirm(`停止「${key}」的连接？该站所有端点将不再接收请求。`))return;
  const r=await api('PUT',`/api/stations/${encodeURIComponent(key)}`,{connect_paused:pause});
  if(r&&r.ok)toast(pause?`已停止「${key}」连接`:`已恢复「${key}」连接`,'success');
  else toast(r.error||'操作失败','error');
  refresh();
}
async function deleteStation(key,total){
  if(!confirm(`删除中转站「${key}」？\n该站的 ${total} 个端点将被全部删除，且无法恢复。`))return;
  const r=await api('DELETE',`/api/stations/${encodeURIComponent(key)}`);
  if(r&&r.ok){toast(`已删除「${key}」（${r.removed} 个端点）`,'success');if(stationFilter===key)stationFilter='';}
  else toast(r.error||'删除失败','error');
  refresh();
}
let _seKey='';
function openStationEdit(key){
  const s=_stations.find(x=>x.key===key);
  if(!s){toast('中转站不存在','error');return;}
  _seKey=key;
  const cfg=s.config||{};
  document.getElementById('seKey').textContent=key;
  const multi='(多个值)';
  const setV=(id,v)=>{const el=document.getElementById(id);el.value='';el.placeholder=(v===null||v===undefined)?multi:String(v);};
  setV('seName',cfg.name);
  setV('seUrl',s.base_urls.length===1?s.base_urls[0]:null);
  document.getElementById('seKeyInput').value='';
  document.getElementById('seKeyHint').textContent=cfg.api_key_hint===null?multi:(cfg.api_key_hint||'—');
  setV('seTimeout',cfg.timeout);setV('seRetries',cfg.max_retries);
  setV('seCooldown',cfg.cooldown_minutes);setV('seRpm',cfg.rpm_limit);setV('seDaily',cfg.daily_limit);
  document.getElementById('seProxy').value='';
  document.getElementById('seProtocol').value='';
  document.getElementById('seHealthMode').value='';
  document.getElementById('stationEditModal').classList.add('show');
}
function closeStationEdit(){document.getElementById('stationEditModal').classList.remove('show');}
async function seFetchModels(){
  // 复用「添加端点」弹窗：带上该站配置模板打开并自动拉取模型列表
  const eps=await api('GET','/api/endpoints');
  const tpl=(eps||[]).find(e=>stationKeyOf(e.base_url)===_seKey);
  if(!tpl){toast('该站没有可参考的端点','error');return;}
  closeStationEdit();
  openAddModal();
  document.getElementById('fName').value=tpl.name;
  document.getElementById('fUrl').value=tpl.base_url;
  document.getElementById('fKey').value=tpl.api_key_full||'';
  document.getElementById('fTimeout').value=tpl.timeout;
  document.getElementById('fRetries').value=tpl.max_retries;
  document.getElementById('fCooldown').value=tpl.cooldown_minutes;
  document.getElementById('fDailyLimit').value=tpl.daily_limit||0;
  document.getElementById('fRpmLimit').value=tpl.rpm_limit||0;
  document.getElementById('fProxy').value=String(tpl.use_proxy!==false);
  document.getElementById('fProtocol').value=tpl.protocol||'openai';
  document.getElementById('fHealthMode').value=tpl.health_mode||'chat';
  checkFetchBtn();
  fetchModels();
}
function stationKeyOf(u){
  // 与后端 station_key 一致：按 host（含端口）小写归组
  try{const s=(u||'').includes('://')?u:'https://'+(u||'');return (new URL(s).host||'').toLowerCase()||(u||'').trim().toLowerCase();}catch(e){return (u||'').trim().toLowerCase();}
}
async function saveStationEdit(){
  const u={};
  const g=id=>document.getElementById(id).value.trim();
  if(g('seName'))u.name=g('seName');
  if(g('seUrl'))u.base_url=g('seUrl');
  if(g('seKeyInput'))u.api_key=g('seKeyInput');
  if(g('seTimeout'))u.timeout=parseInt(g('seTimeout'));
  if(g('seRetries'))u.max_retries=parseInt(g('seRetries'));
  if(g('seCooldown'))u.cooldown_minutes=parseInt(g('seCooldown'));
  if(g('seRpm'))u.rpm_limit=parseInt(g('seRpm'));
  if(g('seDaily'))u.daily_limit=parseInt(g('seDaily'));
  if(g('seProxy'))u.use_proxy=g('seProxy')==='true';
  if(g('seProtocol'))u.protocol=g('seProtocol');
  if(g('seHealthMode'))u.health_mode=g('seHealthMode');
  if(!Object.keys(u).length){toast('没有需要修改的项','error');return;}
  const r=await api('PUT',`/api/stations/${encodeURIComponent(_seKey)}`,{updates:u});
  if(r&&r.ok){
    toast('中转站配置已更新','success');
    if(u.base_url&&stationFilter===_seKey)stationFilter=r.station?r.station.key:'';
    closeStationEdit();refresh();
  }else toast(r.error||'保存失败','error');
}

function renderFilterBar(eps){
  const en=eps.filter(e=>e.enabled).length;
  document.getElementById('filterBar').innerHTML=`
    <button class="filter-btn ${epFilter==='all'?'active':''}" onclick="setFilter('all')">全部 ${eps.length}</button>
    <button class="filter-btn ${epFilter==='enabled'?'active':''}" onclick="setFilter('enabled')">启用 ${en}</button>
    <button class="filter-btn ${epFilter==='disabled'?'active':''}" onclick="setFilter('disabled')">禁用 ${eps.length-en}</button>
    ${stationFilter?`<button class="filter-btn active" style="background:var(--yellow);border-color:var(--yellow);color:#000" onclick="setStationFilter(stationFilter)" title="取消中转站筛选">🏢 ${esc(stationFilter)} ✕</button>`:''}
    <span class="filter-count" id="filterCount"></span>`;
}
function setFilter(f){epFilter=f;refresh();}

function hBadge(h,lat){
  const m={ok:['badge-h-ok','✅'],slow:['badge-h-slow','🐢'],bad:['badge-h-bad','❌'],unknown:['badge-h-unknown','❓'],testing:['badge-h-unknown','⏳']};
  const[c,l]=m[h]||m.unknown;
  return`<span class="badge ${c}">${l}${lat>=0?' '+lat+'ms':''}</span>`;
}

function isEpAbnormal(ep){return ep.in_cooldown||!ep.enabled||ep.health==='bad'||ep.is_rpm_limited||ep.station_connect_paused||ep.station_health_paused||(ep.daily_limit>0&&ep.today_used>=ep.daily_limit);}
function toggleEpBad(btn){
  const body=document.getElementById('ep-bad-body');
  const nowOpen=body.style.display==='none';
  body.style.display=nowOpen?'':'none';
  const cnt=body.querySelectorAll('.ep-item').length;
  btn.innerHTML=(nowOpen?'▲ 收起':'▼ 展开')+` ${cnt} 个异常/停用端点`;
  sessionStorage.setItem('ep-bad-open',nowOpen?'1':'0');
}
function renderEndpoints(eps){
  if(stationFilter)eps=eps.filter(e=>e.station===stationFilter);
  if(epFilter==='enabled')eps=eps.filter(e=>e.enabled);
  else if(epFilter==='disabled')eps=eps.filter(e=>!e.enabled);
  const c=document.getElementById('filterCount');if(c)c.textContent=(stationFilter?`[${stationFilter}] `:'')+`${eps.length} 个`;
  const el=document.getElementById('epList');
  if(!eps.length){el.innerHTML='<div class="empty">暂无端点</div>';return;}
  const _epCard=(ep)=>{
    let cls='ep-item';
    if(!ep.enabled)cls+=' disabled';
    if(ep.is_current)cls+=' current';
    if(ep.in_cooldown)cls+=' in-cooldown';
    if(ep.last_error&&!ep.in_cooldown)cls+=' has-error';
    if(ep.daily_limit>0&&ep.today_used>=ep.daily_limit)cls+=' in-cooldown';
    if(ep.is_rpm_limited)cls+=' in-cooldown';
    let b=`<span class="badge badge-priority">#${ep.priority}</span>${hBadge(ep.health,ep.health_latency_ms)}`;
    if(ep.protocol==='anthropic')b+=`<span class="badge" style="background:rgba(217,119,87,0.2);color:#ff9e7a;border:1px solid rgba(217,119,87,0.3)" title="Anthropic 原生协议翻译">🧠Anthropic</span>`;
    else b+=`<span class="badge badge-priority" style="background:rgba(16,163,127,0.2);color:#2ecc71" title="OpenAI 兼容协议">🟢OpenAI</span>`;
    if(ep.is_current)b+='<span class="badge badge-current">● 当前</span>';
    if(!ep.enabled)b+='<span class="badge badge-disabled">禁用</span>';
    if(ep.station_connect_paused)b+='<span class="badge badge-disabled" title="所属中转站已停止连接">⛔站点停连</span>';
    if(ep.station_health_paused)b+='<span class="badge badge-cooldown" title="所属中转站已暂停测活">🔕站点停测</span>';
      if(ep.is_vision!==false)b+=`<span class=\"badge\" style=\"background:rgba(0,122,255,.15);color:#0a84ff\" title=\"原生支持视觉能力\">👁️视觉</span>`;
    if(ep.is_rpm_limited)b+=`<span class="badge badge-cooldown" title="每分钟并发已满，限流降级中">🚧限流中</span>`;
    else if(ep.daily_limit>0&&ep.today_used>=ep.daily_limit)b+=`<span class="badge badge-cooldown" title="今日额度已满，挂起至明日">🛑额度耗尽</span>`;
    else if(ep.in_cooldown)b+=`<span class="badge badge-cooldown">⏳${fmtTime(ep.cooldown_remaining)}</span>`;
    if(!ep.use_proxy)b+=`<span class="badge badge-priority" title="绕过系统全局代理，强制直连">🌐直连</span>`;
    if(ep.health_mode==='none')b+=`<span class="badge" style="background:rgba(255,255,255,.05);color:var(--text-dim)" title="已关闭后台健康监测">🔕免扰</span>`;
    else if(ep.health_mode==='models')b+=`<span class="badge" style="background:rgba(50,215,75,.1);color:var(--green)" title="零成本 Models 探针">☘️无感测</span>`;
    if(ep.daily_limit>0)b+=`<span class="badge" style="background:rgba(255,255,255,.05);color:var(--text-dim)" title="每日消耗进度">📊${fmtNum(ep.today_used)} / ${fmtNum(ep.daily_limit)}</span>`;
    if(ep.rpm_limit>0)b+=`<span class="badge" style="background:rgba(255,255,255,.05);color:var(--text-dim)" title="每分钟最高并发请求限制">🚀${ep.rpm_limit} RPM</span>`;
    const last=ep.last_success?timeAgo(ep.last_success):'—';
    return`<div class="${cls}">
      <div class="ep-header">
        <div class="ep-name"><span style="word-break: break-all;">${esc(ep.name)}</span> ${b}</div>
        <div class="ep-actions">
          <button class="btn btn-ghost btn-sm" title="连通性测试" onclick="openTestDrawer('${ep.id}', '${esc(ep.name)} (${esc(ep.model)})')">🧪</button>
          ${ep.in_cooldown?`<button class="btn btn-yellow btn-sm" title="立刻解除冷却" onclick="clearCooldown('${ep.id}')">⏰</button>`:''}
          <button class="btn btn-ghost btn-sm" title="${ep.enabled?'禁用端点':'启用端点'}" onclick="toggleEndpoint('${ep.id}')">${ep.enabled?'⏸':'▶'}</button>
          <button class="btn btn-ghost btn-sm" title="编辑端点" onclick="editEndpoint('${ep.id}')">✏️</button>
          <button class="btn btn-ghost btn-sm" title="删除端点" onclick="deleteEndpoint('${ep.id}', '${esc(ep.name)}')" style="color:var(--red)">🗑</button>
        </div>
      </div>
      <div class="ep-meta">
        <span title="客户端请求使用的统一模型名">🤖${esc(ep.model)}</span>${ep.upstream_model&&ep.upstream_model!==ep.model?`<span title="实际发送给该上游的模型名">↗${esc(ep.upstream_model)}</span>`:''}<span title="单次请求超时时间">⏱${ep.timeout}s</span><span title="失败后最大重试次数">🔁${ep.max_retries}次</span><span title="请求失败后的冷却惩罚时间">❄️${ep.cooldown_minutes}分</span><span title="累计成功响应次数">📞${ep.total_calls}次</span><span title="最后一次成功响应时间">🕐${last}</span>
      </div>
      ${ep.last_error?`<div class="ep-error">⚠ ${esc(ep.last_error)}</div>`:''}
    </div>`;
  };
  const normal=eps.filter(ep=>!isEpAbnormal(ep));
  const bad=eps.filter(ep=>isEpAbnormal(ep));
  let html=normal.map(_epCard).join('');
  if(bad.length){
    const open=sessionStorage.getItem('ep-bad-open')==='1';
    html+=`<div style="margin:6px 0 0"><button class="btn btn-ghost" style="width:100%;justify-content:center;padding:8px 12px;border:1px dashed rgba(255,255,255,.12);border-radius:8px;color:var(--text-dim);font-size:12px;gap:6px" onclick="toggleEpBad(this)">${open?'▲ 收起':'▼ 展开'} ${bad.length} 个异常/停用端点</button><div id="ep-bad-body" ${open?'':'style="display:none"'}>${bad.map(_epCard).join('')}</div></div>`;
  }
  el.innerHTML=html;
}

function renderChain(chain){
  const el=document.getElementById('chainList');
  if(!chain.length){el.innerHTML='<div class="empty">没有启用的端点</div>';return;}
  const isChainBad=(it)=>it.in_cooldown||(it.health==='bad'&&!it.is_current);
  const normal=chain.filter(it=>!isChainBad(it));
  const bad=chain.filter(it=>isChainBad(it));
  const _chainItem=(it,isLast)=>{
    let cls='chain-item';
    if(it.in_cooldown)cls+=' cooldown';
    else if(it.is_current)cls+=' active';
    else if(it.health==='bad'||it.fail_count>0)cls+=' failed';
    const st=it.in_cooldown?'<span class="badge badge-warning">冷却中</span>':(it.is_current?'<span class="badge badge-success">服务中</span>':'');
    const h=it.health,lat=it.health_latency_ms;
    let rh='';
    if(h==='ok')rh=`<div class="chain-health" style="color:var(--green)">✓${lat>=0?' '+lat+'ms':''}</div>`;
    else if(h==='slow')rh=`<div class="chain-health" style="color:var(--yellow)">🐢${lat>=0?' '+lat+'ms':''}</div>`;
    else if(h==='bad'){rh=`<div class="chain-health" style="color:var(--red)">✗${lat>=0?' '+lat+'ms':''}</div>`;if(it.health_error)rh+=`<div class="chain-err" title="${esc(it.health_error)}">${esc(it.health_error)}</div>`;}
    else if(h==='testing')rh='<div class="chain-health" style="color:var(--text-dim)">…</div>';
    else rh='<div class="chain-health" style="color:var(--text-dim)">-</div>';
    const conn=isLast?'':'<div class="chain-connector"></div>';
    const vis=(it.is_vision!==false)?'<span class="badge" style="background:rgba(0,122,255,.15);color:#0a84ff" title="支持原生视觉">👁️视觉</span>':'';
    return`<div class="${cls}"><div class="chain-dot"></div><div class="chain-info"><div class="name">${esc(it.name)} ${st}</div><div class="model">${esc(it.model)} ${vis}</div></div><div class="chain-right">${rh}</div></div>${conn}`;
  };
  let html=normal.map((it,i)=>_chainItem(it,i===normal.length-1&&!bad.length)).join('');
  if(bad.length){
    const open=sessionStorage.getItem('chain-bad-open')==='1';
    html+=`<div class="chain-connector"></div><div style="padding:4px 0"><button class="btn btn-ghost" style="width:100%;justify-content:center;padding:6px 10px;border:1px dashed rgba(255,255,255,.1);border-radius:6px;color:var(--text-dim);font-size:11px;gap:4px" onclick="toggleChainBad(this)">${open?'▲ 收起':'▼ 展开'} ${bad.length} 个异常/冷却端点</button><div id="chain-bad-body" ${open?'':'style="display:none"'}>${bad.map((it,i)=>_chainItem(it,i===bad.length-1)).join('')}</div></div>`;
  }
  el.innerHTML=html;
}
function toggleChainBad(btn){
  const body=document.getElementById('chain-bad-body');
  const nowOpen=body.style.display==='none';
  body.style.display=nowOpen?'':'none';
  const cnt=body.querySelectorAll('.chain-item').length;
  btn.innerHTML=(nowOpen?'▲ 收起':'▼ 展开')+` ${cnt} 个异常/冷却端点`;
  sessionStorage.setItem('chain-bad-open',nowOpen?'1':'0');
}

async function runHealthCheck(){toast('正在检测...','info');const r=await api('POST','/api/health-check');if(r.ok){const o=r.results.filter(x=>x.health==='ok').length,s=r.results.filter(x=>x.health==='slow').length,b=r.results.filter(x=>x.health==='bad').length;toast(`✅${o} 🐢${s} ❌${b}`,'success');}refresh();}
async function toggleEndpoint(id){await api('POST',`/api/endpoints/${encodeURIComponent(id)}/toggle`);refresh();}
async function deleteEndpoint(id, n){if(!confirm(`删除「${n}」？`))return;await api('DELETE',`/api/endpoints/${encodeURIComponent(id)}`);toast('已删除','success');refresh();}
async function clearCooldown(id){await api('PUT',`/api/endpoints/${encodeURIComponent(id)}`,{cooldown_minutes:0});await api('POST','/api/reset');setTimeout(async()=>{await api('PUT',`/api/endpoints/${encodeURIComponent(id)}`,{cooldown_minutes:5});refresh();},200);toast('已解除冷却','success');refresh();}
let testImageBase64 = null;
function previewTestImage(input) {
  if (input.files && input.files[0]) {
    const reader = new FileReader();
    reader.onload = function(e) {
      testImageBase64 = e.target.result;
      const btn = document.getElementById('btnTestImage');
      if(btn) { btn.style.background = 'rgba(50,215,75,0.2)'; btn.title = '已附加图片'; }
      toast('图片已附加', 'success');
    };
    reader.readAsDataURL(input.files[0]);
  }
}
function clearTestImage() {
  testImageBase64 = null;
  const input = document.getElementById('testImage');
  if (input) input.value = '';
  const btn = document.getElementById('btnTestImage');
  if (btn) { btn.style.background = 'transparent'; btn.title = '上传图片测试'; }
}

function openTestDrawer(targetId, targetName) {
  document.getElementById('testTargetId').value = targetId;
  if(targetId === 'pool') {
    document.getElementById('testDrawerTitle').innerHTML = '🧪 测试端点池 (Pool)';
  } else {
    document.getElementById('testDrawerTitle').innerHTML = `🧪 测试: ${targetName}`;
  }
  document.getElementById('testResult').style.display = 'none';
  document.getElementById('testDrawer').classList.add('show');
}
function closeTestDrawer() {
  document.getElementById('testDrawer').classList.remove('show');
}
async function sendTest() {
  const targetId = document.getElementById('testTargetId').value;
  const m = document.getElementById('testMsg').value || '你好';
  toast('发送测试中...','info');
  let r;
  if(targetId === 'pool'){
    r = await api('POST','/api/test-pool',{message:m,image:testImageBase64});
  } else {
    r = await api('POST','/api/test',{id:targetId,message:m,image:testImageBase64});
  }
  const el = document.getElementById('testResult');
  el.style.display = 'block';
  if(r.ok){
    el.className='test-result success';
    el.textContent='✅ '+r.result+(r.served_by?'\n[响应: '+r.served_by+']':'');
  }else{
    el.className='test-result failure';
    el.textContent='❌ '+(r.error||r.errors?.join('\n'));
  }
  refresh();
}
async function resetPool(){await api('POST','/api/reset');toast('已重置','success');refresh();}

function checkFetchBtn(){
    const u=document.getElementById('fUrl').value.trim(),k=document.getElementById('fKey').value.trim();
    document.getElementById('fetchModelsBtn').disabled=!(u&&k);
    
    const nameEl = document.getElementById('fName');
    if (!nameEl.value || nameEl.dataset.autofilled === 'true') {
        const provider = detectProvider(u);
        if (provider) {
            nameEl.value = provider;
            nameEl.dataset.autofilled = 'true';
        } else if (nameEl.dataset.autofilled === 'true') {
            nameEl.value = '';
            nameEl.dataset.autofilled = 'false';
        }
    }
}
function detectProvider(url) {
    if(!url) return '';
    const u = url.toLowerCase();
    if(u.includes('api.openai.com')) return 'OpenAI';
    if(u.includes('openrouter.ai')) return 'OpenRouter';
    if(u.includes('api.anthropic.com')) return 'Anthropic';
    if(u.includes('api.deepseek.com')) return 'DeepSeek';
    if(u.includes('integrate.api.nvidia.com')) return 'NVIDIA';
    if(u.includes('open.bigmodel.cn')) return 'BigModel';
    if(u.includes('dashscope.aliyuncs.com')) return 'Aliyun';
    if(u.includes('api.siliconflow.cn')) return 'SiliconFlow';
    if(u.includes('api.moonshot.cn')) return 'Moonshot';
    if(u.includes('api.groq.com')) return 'Groq';
    if(u.includes('api.together.xyz')) return 'Together';
    if(u.includes('ollama')) return 'Ollama';
    if(u.includes('localhost') || u.includes('127.0.0.1')) return 'Local';
    try {
        const dom = new URL(url).hostname;
        const parts = dom.split('.');
        if(parts.length >= 2) {
            let name = parts[parts.length-2];
            return name.charAt(0).toUpperCase() + name.slice(1);
        }
    }catch(e){}
    return '';
}
async function fetchModels(){
  const u=document.getElementById('fUrl').value.trim(),k=document.getElementById('fKey').value.trim();
  const up=document.getElementById('fProxy').value==='true',pt=document.getElementById('fProtocol').value||'openai';
  if(!u||!k){toast('填写 URL 和 Key','error');return;}
  const b=document.getElementById('fetchModelsBtn');b.disabled=true;b.innerHTML='⏳';
  try{const r=await api('POST','/api/fetch-models',{base_url:u,api_key:k,use_proxy:up,protocol:pt});
    if(r.ok&&r.models?.length){allModels=r.models;selectedModels=new Set();latencyResults={};visionResults={};modelPage=1;renderModelBrowser();toast(`${r.count} 个模型`,'success');}
    else{document.getElementById('modelBrowser').innerHTML=`<div style="padding:10px;color:var(--red);font-size:12px">❌ ${esc(r.error||'无模型')}</div>`;document.getElementById('modelBrowser').style.display='block';}
  }catch(e){
    document.getElementById('modelBrowser').innerHTML=`<div style="padding:10px;color:var(--red);font-size:12px">❌ ${esc(e.message||'请求失败')}</div>`;
    document.getElementById('modelBrowser').style.display='block';
    toast('获取失败','error');
  }
  b.disabled=false;b.innerHTML='🔍 获取';
}
function isOpenRouter(){return document.getElementById('fUrl').value.includes('openrouter');}
function isFreeModel(m){if(!m.pricing)return false;return parseFloat(m.pricing.prompt||'1')===0&&parseFloat(m.pricing.completion||'1')===0;}

function renderModelBrowser(){
  const el=document.getElementById('modelBrowser');
  el.innerHTML=`<div class="model-browser">
    <div class="mb-toolbar">
      <input type="text" id="modelSearch" placeholder="搜索..." oninput="modelPage=1;filterModels()">
      <button class="btn btn-ghost btn-sm" onclick="selectAll()">全选</button>
      <button class="btn btn-ghost btn-sm" onclick="selectNone()">清空</button>
      <button class="btn btn-ghost btn-sm" onclick="testSelectedLatency()">⏱ 延迟</button>
      <button class="btn btn-ghost btn-sm" onclick="testSelectedVision()">🖼 多模态</button>
      ${isOpenRouter()?`<label><input type="checkbox" id="freeOnly" onchange="modelPage=1;filterModels()"> 🆓免费</label>`:''}
      <span class="count" id="modelCount"></span>
    </div>
    <div class="mb-head"><span></span><span>模型</span><span style="text-align:center">多模态</span><span>价格</span><span>延迟</span></div>
    <div class="mb-table" id="modelListInner"></div>
    <div class="pagination" id="modelPagination" style="display:none"></div>
  </div>`;
  el.style.display='block';
  filterModels();
}

function getFilteredModels(){
  const q=(document.getElementById('modelSearch')?.value||'').toLowerCase();
  const fo=document.getElementById('freeOnly')?.checked||false;
  let f=allModels;if(fo)f=f.filter(m=>isFreeModel(m));if(q)f=f.filter(m=>m.id.toLowerCase().includes(q));return f;
}

function filterModels(){
  const f=getFilteredModels();
  const tp=Math.max(1,Math.ceil(f.length/PP));
  if(modelPage>tp)modelPage=tp;
  const si=(modelPage-1)*PP,pg=f.slice(si,si+PP);
  const inner=document.getElementById('modelListInner');
  const c=document.getElementById('modelCount');if(c)c.textContent=`${f.length}/${allModels.length}`;
  inner.innerHTML=pg.map(m=>{
    const sel=selectedModels.has(m.id);
    const vr=visionResults[m.id];
    let mm='<span class="mm-unknown">—</span>';
    if(vr){mm=vr.supports_vision?'<span class="mm-yes">✅ 图片</span>':'<span class="mm-no">❌</span>';}
    const lat=latencyResults[m.id];
    let lh='';
    if(lat){if(lat.status==='ok')lh=`<span class="lat-ok">✓${lat.latency_ms}ms</span>`;else if(lat.status==='slow')lh=`<span class="lat-slow">🐢${lat.latency_ms}ms</span>`;else lh=`<span class="lat-bad">✗${lat.latency_ms}ms</span>`;}
    let ph='';
    if(m.pricing){if(isFreeModel(m))ph='<span class="free-tag">FREE</span>';else ph=`<span style="font-size:10px;color:var(--text-dim)">$${m.pricing.prompt||'0'}/$${m.pricing.completion||'0'}</span>`;}
    return`<div class="mb-row${sel?' selected':''}" onclick="event.target.tagName!=='INPUT'&&toggleModel('${esc(m.id)}')">
      <input type="checkbox" ${sel?'checked':''} onclick="event.stopPropagation();toggleModel('${esc(m.id)}')">
      <span class="name-cell" title="${esc(m.id)}">${esc(m.id)}</span>
      <span class="mm-cell">${mm}</span>
      <span class="price-cell">${ph}</span>
      <span class="lat-cell">${lh}</span>
    </div>`;
  }).join('');
  const pag=document.getElementById('modelPagination');
  if(tp>1){pag.style.display='flex';pag.innerHTML=`
    <button class="btn btn-ghost btn-sm" onclick="modelPage=1;filterModels()" ${modelPage===1?'disabled':''}>⏮</button>
    <button class="btn btn-ghost btn-sm" onclick="modelPage--;filterModels()" ${modelPage===1?'disabled':''}>◀</button>
    <span class="page-info">${modelPage}/${tp}</span>
    <button class="btn btn-ghost btn-sm" onclick="modelPage++;filterModels()" ${modelPage===tp?'disabled':''}>▶</button>
    <button class="btn btn-ghost btn-sm" onclick="modelPage=${tp};filterModels()" ${modelPage===tp?'disabled':''}>⏭</button>`;}
  else pag.style.display='none';
  updateBatchBar();
}

function toggleModel(id){selectedModels.has(id)?selectedModels.delete(id):selectedModels.add(id);if(selectedModels.size===1){document.getElementById('fModel').value=[...selectedModels][0];document.getElementById('fUpstreamModel').value=[...selectedModels][0];}else if(!selectedModels.size){document.getElementById('fModel').value='';document.getElementById('fUpstreamModel').value='';}filterModels();}
function selectAll(){getFilteredModels().forEach(m=>selectedModels.add(m.id));filterModels();}
function selectNone(){selectedModels.clear();document.getElementById('fModel').value='';document.getElementById('fUpstreamModel').value='';filterModels();}

function updateBatchBar(){
  const bar=document.getElementById('batchBar'),bb=document.getElementById('batchAddBtn'),sb=document.getElementById('singleAddBtn');
  if(selectedModels.size>1){bar.style.display='block';bar.innerHTML=`<div class="batch-bar"><span>已选 ${selectedModels.size} 个模型</span></div>`;bb.style.display='inline-flex';sb.style.display='none';}
  else{bar.style.display='none';bb.style.display='none';sb.style.display='inline-flex';}
}

async function testSelectedLatency(){
  const u=document.getElementById('fUrl').value.trim(),k=document.getElementById('fKey').value.trim();
  const up=document.getElementById('fProxy').value==='true',pt=document.getElementById('fProtocol').value||'openai';
  if(!u||!k){toast('填写 URL 和 Key','error');return;}
  if(!selectedModels.size){toast('勾选模型','error');return;}
  const ms=[...selectedModels];toast(`测试 ${ms.length} 个...`,'info');
  for(const mid of ms){latencyResults[mid]={status:'bad',latency_ms:0};filterModels();try{latencyResults[mid]=await api('POST','/api/test-model',{base_url:u,api_key:k,model:mid,use_proxy:up,protocol:pt});}catch(e){latencyResults[mid]={status:'bad',latency_ms:0};}filterModels();}
  toast(`✅${Object.values(latencyResults).filter(r=>r.ok).length}/${ms.length}`,'success');
}

async function testSelectedVision(){
  const u=document.getElementById('fUrl').value.trim(),k=document.getElementById('fKey').value.trim();
  const up=document.getElementById('fProxy').value==='true',pt=document.getElementById('fProtocol').value||'openai';
  if(!u||!k){toast('填写 URL 和 Key','error');return;}
  if(!selectedModels.size){toast('勾选模型','error');return;}
  const ms=[...selectedModels];toast(`检测 ${ms.length} 个多模态...`,'info');
  let vis=0;
  for(const mid of ms){visionResults[mid]={supports_vision:false};filterModels();try{const r=await api('POST','/api/test-vision',{base_url:u,api_key:k,model:mid,use_proxy:up,protocol:pt});visionResults[mid]=r;if(r.supports_vision)vis++;}catch(e){visionResults[mid]={supports_vision:false};}filterModels();}
  toast(`多模态: ${vis}/${ms.length} 支持`,'success');
}

function openAddModal(){
    document.getElementById('editName').value='';document.getElementById('modalTitle').textContent='添加端点';
    ['fName','fUrl','fKey','fModel','fUpstreamModel'].forEach(id=>document.getElementById(id).value='');
    document.getElementById('fPriority').value=1;document.getElementById('fTimeout').value=60;document.getElementById('fRetries').value=0;document.getElementById('fCooldown').value=5;document.getElementById('fEnabled').value='true';document.getElementById('fDailyLimit').value=0;document.getElementById('fRpmLimit').value=0;document.getElementById('fProxy').value='true';document.getElementById('fProtocol').value='openai';document.getElementById('fHealthMode').value='chat';document.getElementById('fVision').value='true';
    document.getElementById('modelBrowser').style.display='none';document.getElementById('batchBar').style.display='none';
    document.getElementById('fetchModelsBtn').disabled=true;document.getElementById('batchAddBtn').style.display='none';document.getElementById('singleAddBtn').style.display='inline-flex';
    allModels=[];selectedModels=new Set();latencyResults={};visionResults={};
    document.getElementById('modal').classList.add('show');
}
function editEndpoint(id){
    api('GET','/api/endpoints').then(eps=>{const ep=eps.find(e=>e.id===id);if(!ep)return;
        document.getElementById('editName').value=id;document.getElementById('modalTitle').textContent='编辑端点';
        document.getElementById('fName').value=ep.name;document.getElementById('fUrl').value=ep.base_url;document.getElementById('fKey').value=ep.api_key_full||'';document.getElementById('fModel').value=ep.model;document.getElementById('fUpstreamModel').value=ep.upstream_model||ep.model;
        document.getElementById('fPriority').value=ep.priority;document.getElementById('fTimeout').value=ep.timeout;document.getElementById('fRetries').value=ep.max_retries;document.getElementById('fCooldown').value=ep.cooldown_minutes;document.getElementById('fEnabled').value=String(ep.enabled);document.getElementById('fDailyLimit').value=ep.daily_limit||0;document.getElementById('fRpmLimit').value=ep.rpm_limit||0;document.getElementById('fProxy').value=String(ep.use_proxy!==false);document.getElementById('fProtocol').value=ep.protocol||'openai';document.getElementById('fHealthMode').value=ep.health_mode||'chat';document.getElementById('fVision').value=String(ep.is_vision!==false);
        document.getElementById('modelBrowser').style.display='none';document.getElementById('batchBar').style.display='none';document.getElementById('batchAddBtn').style.display='none';document.getElementById('singleAddBtn').style.display='inline-flex';
        allModels=[];selectedModels=new Set();latencyResults={};visionResults={};checkFetchBtn();document.getElementById('modal').classList.add('show');
    });
}
function closeModal(){document.getElementById('modal').classList.remove('show');}

async function saveEndpoint(){
    const ep_id=document.getElementById('editName').value;
    const publicModel=document.getElementById('fModel').value.trim(),upstreamModel=document.getElementById('fUpstreamModel').value.trim()||publicModel;
    const d={name:document.getElementById('fName').value.trim(),base_url:document.getElementById('fUrl').value.trim(),api_key:document.getElementById('fKey').value.trim(),model:publicModel,public_model:publicModel,upstream_model:upstreamModel,priority:parseInt(document.getElementById('fPriority').value)||1,timeout:parseInt(document.getElementById('fTimeout').value)||60,max_retries:parseInt(document.getElementById('fRetries').value)||0,cooldown_minutes:parseInt(document.getElementById('fCooldown').value)||0,enabled:document.getElementById('fEnabled').value==='true',daily_limit:parseInt(document.getElementById('fDailyLimit').value)||0,rpm_limit:parseInt(document.getElementById('fRpmLimit').value)||0,use_proxy:document.getElementById('fProxy').value==='true',protocol:document.getElementById('fProtocol').value||'openai',health_mode:document.getElementById('fHealthMode').value||'chat',is_vision:document.getElementById('fVision').value==='true'};
    if(!d.name||!d.base_url||!d.api_key){toast('填写名称/URL/Key','error');return;}
    if(!d.model){toast('选择模型','error');return;}
    if(ep_id){await api('PUT',`/api/endpoints/${encodeURIComponent(ep_id)}`,d);toast('已更新','success');}
    else{await api('POST','/api/endpoints',d);toast('已添加','success');}
    closeModal();refresh();
}

async function batchAddEndpoints(){
    const fn=document.getElementById('fName').value.trim();
    const u=document.getElementById('fUrl').value.trim(),k=document.getElementById('fKey').value.trim();
    const sp=parseInt(document.getElementById('fPriority').value)||1,to=parseInt(document.getElementById('fTimeout').value)||60,re=parseInt(document.getElementById('fRetries').value)||0,cd=parseInt(document.getElementById('fCooldown').value)||5,dl=parseInt(document.getElementById('fDailyLimit').value)||0,rl=parseInt(document.getElementById('fRpmLimit').value)||0,up=document.getElementById('fProxy').value==='true',pt=document.getElementById('fProtocol').value||'openai',hm=document.getElementById('fHealthMode').value||'chat';
    if(!u||!k){toast('填写 URL 和 Key','error');return;}
    if(!selectedModels.size){toast('选择模型','error');return;}
    const ms=[...selectedModels];toast(`添加 ${ms.length} 个...`,'info');
    const r=await api('POST','/api/endpoints/batch',{endpoints:ms.map((m,i)=>({name:fn?fn:m,model:m,priority:sp+i,is_vision:visionResults[m]?visionResults[m].supports_vision:true})),base:{base_url:u,api_key:k,timeout:to,max_retries:re,cooldown_minutes:cd,daily_limit:dl,rpm_limit:rl,use_proxy:up,protocol:pt,start_priority:sp,health_mode:hm}});
    if(r.ok){toast(`✅ ${r.added} 个`,'success');closeModal();refresh();}else toast('失败','error');
}

function esc(s){const d=document.createElement('div');d.textContent=s;return d.innerHTML;}
function timeAgo(ts){if(!ts)return'—';const s=Math.floor(Date.now()/1000-ts);if(s<60)return s+'s前';if(s<3600)return Math.floor(s/60)+'m前';if(s<86400)return Math.floor(s/3600)+'h前';return Math.floor(s/86400)+'d前';}
function fmtTime(s){if(s<=0)return'';if(s<60)return s+'s';const m=Math.floor(s/60);return(s%60)?`${m}m${s%60}s`:`${m}m`;}
function toast(msg,type){const el=document.getElementById('toast');el.textContent=msg;el.className='toast toast-'+type+' show';setTimeout(()=>el.classList.remove('show'),2500);}

function closeStatsModal(){document.getElementById('statsModal').classList.remove('show');}
function fmtNum(n) {
    if (!n) return '0';
    if (n >= 100000000) return (n / 100000000).toFixed(2).replace(/\.00$/, '') + ' 亿';
    if (n >= 10000) return (n / 10000).toFixed(2).replace(/\.00$/, '') + ' 万';
    return n.toLocaleString();
}


function drawSVGChart(containerId, data, options = {}) {
    const key = options.key || 'tokens';
    const unit = options.unit || 'Tokens';
    const container = document.getElementById(containerId);
    if (!data || data.length === 0) {
        if(container) container.innerHTML = '<div class="empty">暂无趋势数据</div>';
        return;
    }
    const maxVal = Math.max(...data.map(d => d[key])) || 1;
    const padding = 15;
    const w = container.clientWidth || 800;
    const h = 180;
    
    let pts = data.map((d, i) => {
        const x = padding + (i / Math.max(1, data.length - 1)) * (w - 2 * padding);
        const y = h - padding - (d[key] / maxVal) * (h - 2 * padding);
        return {x, y, d};
    });
    
    let pathD = pts.length ? `M ${pts[0].x},${pts[0].y}` : '';
    for (let i = 1; i < pts.length; i++) {
        const prev = pts[i-1], curr = pts[i];
        const cpX = prev.x + (curr.x - prev.x) / 2;
        pathD += ` C ${cpX},${prev.y} ${cpX},${curr.y} ${curr.x},${curr.y}`;
    }
    const polyD = pts.length ? `${pathD} L ${pts[pts.length-1].x},${h} L ${pts[0].x},${h} Z` : '';
    
    const yTicks = [maxVal, maxVal/2, 0];
    const yTickElements = yTicks.map(val => `<text x="5" y="${h - padding - (val/maxVal)*(h - 2*padding) - 4}" fill="var(--text-dim)" font-size="10" font-family="monospace">${fmtNum(val)}</text>`).join('');

    container.innerHTML = `
        <svg viewBox="0 0 ${w} ${h}" style="width:100%; height:100%; overflow:visible;">
            <defs>
                <linearGradient id="chartGrad_${containerId}" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="0%" stop-color="rgba(124, 109, 240, 0.5)"/>
                    <stop offset="100%" stop-color="rgba(124, 109, 240, 0.0)"/>
                </linearGradient>
            </defs>
            <line x1="0" y1="${padding}" x2="${w}" y2="${padding}" stroke="rgba(255,255,255,0.05)" stroke-dasharray="4 4"/>
            <line x1="0" y1="${h/2}" x2="${w}" y2="${h/2}" stroke="rgba(255,255,255,0.05)" stroke-dasharray="4 4"/>
            <line x1="0" y1="${h-padding}" x2="${w}" y2="${h-padding}" stroke="rgba(255,255,255,0.05)" stroke-dasharray="4 4"/>
            ${yTickElements}
            <path d="${polyD}" fill="url(#chartGrad_${containerId})"/>
            <path d="${pathD}" fill="none" stroke="var(--accent)" stroke-width="2.5" stroke-linecap="round"/>
            ${pts.map((p, i) => `<circle cx="${p.x}" cy="${p.y}" r="4" fill="var(--bg)" stroke="var(--accent)" stroke-width="2" class="chart-point" data-idx="${i}" style="cursor:pointer; transition:all 0.2s;"/>`).join('')}
        </svg>
        <div id="${containerId}_tt" style="position:absolute; display:none; background:rgba(20,20,25,0.95); backdrop-filter:blur(10px); border:1px solid rgba(255,255,255,0.1); border-radius:8px; padding:8px 12px; font-size:11px; box-shadow:0 8px 32px rgba(0,0,0,0.5); pointer-events:none; z-index:100; white-space:nowrap; transition: left 0.1s, top 0.1s;"></div>
    `;
    
    const tt = document.getElementById(`${containerId}_tt`);
    container.querySelectorAll('.chart-point').forEach(c => {
        c.addEventListener('mouseenter', (e) => {
            const idx = e.target.getAttribute('data-idx');
            const d = data[idx];
            c.setAttribute('r', '6');
            c.style.filter = 'drop-shadow(0 0 4px var(--accent-light))';
            tt.style.display = 'block';
            tt.innerHTML = `<div style="color:var(--text-dim);margin-bottom:4px;font-weight:600">${d.date}</div><div style="font-weight:700;color:var(--accent-light);font-size:13px;">${fmtNum(d[key])} <span style="font-size:10px;font-weight:400;color:var(--text-dim)">${unit}</span></div>`;
            
            let tx = parseFloat(c.getAttribute('cx')) + 12;
            let ty = parseFloat(c.getAttribute('cy')) - 35;
            if (tx + 120 > container.clientWidth) tx = container.clientWidth - 130;
            if (ty < 0) ty = parseFloat(c.getAttribute('cy')) + 15;
            if (ty + 50 > container.clientHeight) ty = container.clientHeight - 55;
            tt.style.left = tx + 'px';
            tt.style.top = ty + 'px';
        });
        c.addEventListener('mouseleave', () => {
            c.setAttribute('r', '4');
            c.style.filter = 'none';
            tt.style.display = 'none';
        });
    });
}

function drawCompositionChart(containerId, data) {
    const container = document.getElementById(containerId);
    if (!container || !data || !data.length) {
        if(container) container.innerHTML = '<div class="empty">暂无数据</div>';
        return;
    }
    const showCache = document.getElementById('chkCompCache')?.checked ?? true;
    const showMissed = document.getElementById('chkCompMissed')?.checked ?? true;
    const showGen = document.getElementById('chkCompGen')?.checked ?? true;

    const maxVal = Math.max(...data.map(d => {
        const c1 = showCache ? (d.cached || 0) : 0;
        const c2 = c1 + (showMissed ? Math.max(0, (d.prompt || 0) - (d.cached || 0)) : 0);
        return c2 + (showGen ? Math.max(0, (d.tokens || 0) - (d.prompt || 0)) : 0);
    })) || 1;
    const padding = 15;
    const w = container.clientWidth || 800;
    const h = 180;
    
    let pts1 = [], pts2 = [], pts3 = [];
    data.forEach((d, i) => {
        const x = padding + (i / Math.max(1, data.length - 1)) * (w - 2 * padding);
        const c1 = showCache ? (d.cached || 0) : 0;
        const c2 = c1 + (showMissed ? Math.max(0, (d.prompt || 0) - (d.cached || 0)) : 0);
        const c3 = c2 + (showGen ? Math.max(0, (d.tokens || 0) - (d.prompt || 0)) : 0);
        
        if(showCache) pts1.push({x, y: h - padding - (c1 / maxVal) * (h - 2 * padding)});
        if(showMissed) pts2.push({x, y: h - padding - (c2 / maxVal) * (h - 2 * padding)});
        if(showGen) pts3.push({x, y: h - padding - (c3 / maxVal) * (h - 2 * padding)});
    });
    
    const genPath = (pts) => {
        if (!pts.length) return '';
        let dStr = `M ${pts[0].x},${pts[0].y}`;
        for (let i = 1; i < pts.length; i++) {
            const prev = pts[i-1], curr = pts[i];
            const cpX = prev.x + (curr.x - prev.x) / 2;
            dStr += ` C ${cpX},${prev.y} ${cpX},${curr.y} ${curr.x},${curr.y}`;
        }
        return dStr;
    };
    
    const path1 = genPath(pts1), path2 = genPath(pts2), path3 = genPath(pts3);
    const poly1 = pts1.length ? `${path1} L ${pts1[pts1.length-1].x},${h} L ${pts1[0].x},${h} Z` : '';
    const poly2 = pts2.length ? `${path2} L ${pts2[pts2.length-1].x},${h} L ${pts2[0].x},${h} Z` : '';
    const poly3 = pts3.length ? `${path3} L ${pts3[pts3.length-1].x},${h} L ${pts3[0].x},${h} Z` : '';
    
    const yTicks = [maxVal, maxVal/2, 0];
    const yTickElements = yTicks.map(val => `<text x="5" y="${h - padding - (val/maxVal)*(h - 2*padding) - 4}" fill="var(--text-dim)" font-size="10" font-family="monospace">${fmtNum(val)}</text>`).join('');

    container.innerHTML = `
        <svg viewBox="0 0 ${w} ${h}" style="width:100%; height:100%; overflow:visible;">
            <defs>
                <linearGradient id="g3" x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stop-color="rgba(150, 150, 150, 0.4)"/><stop offset="100%" stop-color="rgba(150, 150, 150, 0.0)"/></linearGradient>
                <linearGradient id="g2" x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stop-color="rgba(80, 150, 255, 0.5)"/><stop offset="100%" stop-color="rgba(80, 150, 255, 0.0)"/></linearGradient>
                <linearGradient id="g1" x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stop-color="rgba(0, 200, 100, 0.5)"/><stop offset="100%" stop-color="rgba(0, 200, 100, 0.0)"/></linearGradient>
            </defs>
            <line x1="0" y1="${padding}" x2="${w}" y2="${padding}" stroke="rgba(255,255,255,0.05)" stroke-dasharray="4 4"/>
            <line x1="0" y1="${h/2}" x2="${w}" y2="${h/2}" stroke="rgba(255,255,255,0.05)" stroke-dasharray="4 4"/>
            <line x1="0" y1="${h-padding}" x2="${w}" y2="${h-padding}" stroke="rgba(255,255,255,0.05)" stroke-dasharray="4 4"/>
            ${yTickElements}
            ${showGen ? `<path d="${poly3}" fill="url(#g3)"/><path d="${path3}" fill="none" stroke="rgba(150,150,150,0.8)" stroke-width="2"/>` : ''}
            ${showMissed ? `<path d="${poly2}" fill="url(#g2)"/><path d="${path2}" fill="none" stroke="rgba(80,150,255,0.8)" stroke-width="2"/>` : ''}
            ${showCache ? `<path d="${poly1}" fill="url(#g1)"/><path d="${path1}" fill="none" stroke="rgba(0,200,100,0.8)" stroke-width="2"/>` : ''}
            
            ${(showGen?pts3:(showMissed?pts2:pts1)).map((p, i) => `<circle cx="${p.x}" cy="${p.y}" r="4" fill="var(--bg)" stroke="rgba(200,200,200,0.9)" stroke-width="2" class="chart-point-comp" data-idx="${i}" style="cursor:pointer; transition:all 0.2s;"/>`).join('')}
        </svg>
        <div id="${containerId}_tt" style="position:absolute; display:none; background:rgba(20,20,25,0.95); backdrop-filter:blur(10px); border:1px solid rgba(255,255,255,0.1); border-radius:8px; padding:10px 14px; font-size:12px; box-shadow:0 8px 32px rgba(0,0,0,0.5); pointer-events:none; z-index:100; white-space:nowrap; transition: left 0.1s, top 0.1s;"></div>
    `;
    
    const tt = document.getElementById(`${containerId}_tt`);
    container.querySelectorAll('.chart-point-comp').forEach(c => {
        c.addEventListener('mouseenter', (e) => {
            const idx = e.target.getAttribute('data-idx');
            const d = data[idx];
            c.setAttribute('r', '6');
            c.style.filter = 'drop-shadow(0 0 6px rgba(255,255,255,0.5))';
            tt.style.display = 'block';
            
            const pC = d.cached || 0;
            const pU = Math.max(0, (d.prompt || 0) - pC);
            const comp = Math.max(0, (d.tokens || 0) - pC - pU);
            
            tt.innerHTML = `
                <div style="color:var(--text-dim);margin-bottom:8px;font-weight:700;border-bottom:1px solid rgba(255,255,255,0.1);padding-bottom:6px;">${d.date}</div>
                ${showCache ? `<div style="display:flex; justify-content:space-between; width:160px; margin-bottom:4px;"><span style="color:var(--green)">命中缓存:</span> <span style="font-family:monospace">${fmtNum(pC)}</span></div>` : ''}
                ${showMissed ? `<div style="display:flex; justify-content:space-between; width:160px; margin-bottom:4px;"><span style="color:var(--blue)">未命中 Prompt:</span> <span style="font-family:monospace">${fmtNum(pU)}</span></div>` : ''}
                ${showGen ? `<div style="display:flex; justify-content:space-between; width:160px; margin-bottom:4px;"><span style="color:#aaa">生成 Output:</span> <span style="font-family:monospace">${fmtNum(comp)}</span></div>` : ''}
                <div style="display:flex; justify-content:space-between; width:160px; margin-top:6px; padding-top:6px; border-top:1px dashed rgba(255,255,255,0.1); font-weight:800; font-size:13px;"><span style="color:var(--text)">Total:</span> <span style="font-family:monospace">${fmtNum(d.tokens)}</span></div>
            `;
            
            let tx = parseFloat(c.getAttribute('cx')) + 15;
            let ty = parseFloat(c.getAttribute('cy')) - tt.clientHeight - 10;
            if (tx + 200 > container.clientWidth) tx = container.clientWidth - 210;
            if (ty < 0 || isNaN(ty)) ty = parseFloat(c.getAttribute('cy')) + 15;
            if (ty + 130 > container.clientHeight) ty = container.clientHeight - 135;
            tt.style.left = tx + 'px';
            tt.style.top = ty + 'px';
        });
        c.addEventListener('mouseleave', () => {
            c.setAttribute('r', '4');
            c.style.filter = 'none';
            tt.style.display = 'none';
        });
    });
}

function switchTab(tabId) {
    document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
    document.getElementById(tabId === 'pool' ? 'tabPool' : 'tabAnalytics').classList.add('active');
    
    document.getElementById('viewPool').style.display = tabId === 'pool' ? 'block' : 'none';
    document.getElementById('poolActions').style.display = tabId === 'pool' ? 'flex' : 'none';
    
    document.getElementById('viewAnalytics').style.display = tabId === 'analytics' ? 'block' : 'none';
    document.getElementById('analyticsActions').style.display = tabId === 'analytics' ? 'flex' : 'none';
    
    if(tabId === 'analytics') {
        loadAnalytics();
    }
}

function exportCSV() {
    window.open('/api/export-stats', '_blank');
}

let _analyticsData = null;

async function loadAnalytics(){
    const epFilter = document.getElementById('analyticsFilter').value || 'all';
    document.getElementById('tokenStatsOverview').innerHTML = '<div class="empty">加载中...</div>';
    
    const url = epFilter === 'all' ? '/api/token-stats' : '/api/token-stats?endpoint=' + encodeURIComponent(epFilter);
    const r = await api('GET', url);
    if(!r.today && r.today !== 0) {
        document.getElementById('tokenStatsOverview').innerHTML = '<div class="empty">加载失败</div>';
        return;
    }
    _analyticsData = r;
    
    const sel = document.getElementById('analyticsFilter');
    if (sel.options.length <= 1) {
        let opts = '<option value="all">全端点统计</option>';
        r.all_endpoints_list.forEach(e => { opts += `<option value="${esc(e)}">${esc(e)}</option>`; });
        sel.innerHTML = opts;
        sel.value = epFilter;
    }
    
    document.getElementById('tokenStatsOverview').innerHTML = `
        <div class="dash-stat"><div class="stat-icon">⚡</div><div class="num" style="color:var(--green)">${fmtNum(r.today)}</div><div class="label">今日消耗</div></div>
        <div class="dash-stat"><div class="stat-icon">📊</div><div class="num" style="color:var(--blue)">${fmtNum(r.last_3_days)}</div><div class="label">近 3 天</div></div>
        <div class="dash-stat"><div class="stat-icon">📅</div><div class="num" style="color:var(--yellow)">${fmtNum(r.last_7_days)}</div><div class="label">近 7 天</div></div>
        <div class="dash-stat"><div class="stat-icon">📈</div><div class="num" style="color:var(--accent-light)">${fmtNum(r.last_30_days)}</div><div class="label">近 30 天</div></div>
        <div class="dash-stat"><div class="stat-icon">🔥</div><div class="num" style="color:var(--orange)">${fmtNum(r.today_calls)}</div><div class="label">今日请求次数</div></div>
        <div class="dash-stat"><div class="stat-icon">🌍</div><div class="num" style="color:var(--orange)">${fmtNum(r.month_calls)}</div><div class="label">本月请求次数</div></div>
        <div class="dash-stat"><div class="stat-icon">💾</div><div class="num" style="color:var(--purple)">${r.today_cache_hit_rate}%</div><div class="label" style="display:flex; flex-direction:column;">今日缓存命中<span style="font-size:10px; color:var(--text-dim); margin-top:2px;">命中: ${fmtNum(r.today_cached)} / 未命: ${fmtNum(r.today_missed)}</span></div></div>
        <div class="dash-stat"><div class="stat-icon">🧠</div><div class="num" style="color:var(--purple)">${r.month_cache_hit_rate}%</div><div class="label" style="display:flex; flex-direction:column;">本月缓存命中<span style="font-size:10px; color:var(--text-dim); margin-top:2px;">命中: ${fmtNum(r.month_cached)} / 未命: ${fmtNum(r.month_missed)}</span></div></div>
    `;
    
    setTimeout(() => {
        drawSVGChart('tokenTodayChart', r.trend_today_hourly, {key: 'tokens', unit: 'Tokens'});
        drawSVGChart('missedTodayChart', r.trend_today_hourly, {key: 'missed', unit: 'Tokens'});
        drawSVGChart('callsTodayChart', r.trend_today_hourly, {key: 'calls', unit: '次'});
        drawCompositionChart('tokenTrendChart', r.trend_14d);
    }, 50);
    
    switchTblToday('tokens', true);
    switchTblMonth('tokens', true);
    renderModelUsage();
    
    // Default switchTrend to tokens is already handled in HTML. But just ensure UI state:
    switchTrend('tokens', true);
}

function renderTblData(containerId, data, key) {
    const el = document.getElementById(containerId);
    if (!el) return;
    if (!data || !data.length) {
        el.innerHTML = '<tr><td colspan="2" class="empty">暂无数据</td></tr>';
        return;
    }
    const maxVal = Math.max(...data.map(d => d[key])) || 1;
    el.innerHTML = data.map(d => `
        <tr style="border-bottom: 1px solid rgba(255,255,255,0.03);">
            <td colspan="2" style="padding: 4px 0;">
                <div class="tbl-progress-container">
                    <div class="tbl-progress-bar" style="width: ${(d[key]/maxVal*100).toFixed(1)}%;"></div>
                    <div class="tbl-content">
                        <div><div style="font-size:10px; color:var(--text-dim); margin-bottom:2px;">${esc(d.endpoint)}</div><code>${esc(d.model)}</code></div>
                        <div style="text-align:right;">
                            <div style="font-family: monospace; font-weight:600;">${fmtNum(d[key])}</div>
                            <div style="font-size:9px; color:var(--purple); margin-top:2px;">命中率 ${d.cache_hit_rate||0}%</div>
                        </div>
                    </div>
                </div>
            </td>
        </tr>
    `).join('');
}

function updateCompChart() {
    if (_analyticsData) drawCompositionChart('tokenTrendChart', _analyticsData.trend_14d);
}

function switchTrend(type, skipRender) {
    document.getElementById('btnTrendToken').classList.toggle('active', type === 'tokens');
    document.getElementById('btnTrendMissed').classList.toggle('active', type === 'missed');
    document.getElementById('btnTrendCall').classList.toggle('active', type === 'calls');
    document.getElementById('tokenTodayChart').style.display = type === 'tokens' ? 'block' : 'none';
    document.getElementById('missedTodayChart').style.display = type === 'missed' ? 'block' : 'none';
    document.getElementById('callsTodayChart').style.display = type === 'calls' ? 'block' : 'none';
}


let _modelRange = 'today';
async function loadModelTokenStats(){
  try{
    const r = await api('GET', '/api/token-stats');
    if(!r || (r.today===undefined && r.today!==0)) return;
    _analyticsData = Object.assign(_analyticsData||{}, r);
    renderModelUsage();
  }catch(e){}
}
function switchModelRange(range){
  _modelRange = range === 'month' ? 'month' : 'today';
  const t=document.getElementById('btnModelRangeToday');
  const m=document.getElementById('btnModelRangeMonth');
  if(t) t.classList.toggle('active', _modelRange==='today');
  if(m) m.classList.toggle('active', _modelRange==='month');
  renderModelUsage();
}
function renderModelUsage(){
  const rows=(_analyticsData && (_modelRange==='month' ? _analyticsData.month_models : _analyticsData.today_models)) || [];
  const html = rows.length ? rows.map(d=>{
    const share=Number(d.share||0);
    return `<tr>
      <td>
        <div class="model-usage-name">${esc(d.model||'未知模型')}</div>
        <div class="model-usage-bar-wrap"><div class="model-usage-bar" style="width:${Math.max(0,Math.min(100,share))}%"></div></div>
      </td>
      <td style="text-align:right"><div class="model-usage-num">${fmtNum(d.tokens||0)}</div></td>
      <td style="text-align:right"><div class="model-usage-num">${fmtNum(d.calls||0)}</div></td>
      <td style="text-align:right"><div class="model-usage-num">${fmtNum(d.prompt_tokens||0)}</div></td>
      <td style="text-align:right"><div class="model-usage-num">${fmtNum(d.completion_tokens||0)}</div></td>
      <td style="text-align:right">
        <div class="model-usage-num" style="color:var(--purple)">${d.cache_hit_rate||0}%</div>
        <div class="model-usage-sub">命中 ${fmtNum(d.cached_tokens||0)}</div>
      </td>
      <td style="text-align:right"><div class="model-usage-num">${share.toFixed(1)}%</div></td>
    </tr>`;
  }).join('') : '';
  [['modelUsageTable','modelUsageEmpty'],['modelUsageTableMain','modelUsageEmptyMain']].forEach(([tid,eid])=>{
    const tbody=document.getElementById(tid);
    const empty=document.getElementById(eid);
    if(!tbody) return;
    if(!rows.length){
      tbody.innerHTML='';
      if(empty) empty.style.display='block';
    } else {
      if(empty) empty.style.display='none';
      tbody.innerHTML=html;
    }
  });
  ['btnModelRangeToday','btnModelRangeTodayMain'].forEach(id=>{const el=document.getElementById(id); if(el) el.classList.toggle('active', _modelRange==='today');});
  ['btnModelRangeMonth','btnModelRangeMonthMain'].forEach(id=>{const el=document.getElementById(id); if(el) el.classList.toggle('active', _modelRange==='month');});
}

function switchTblToday(type, skipRender) {
    document.getElementById('btnTblTodayToken').classList.toggle('active', type === 'tokens');
    document.getElementById('btnTblTodayCall').classList.toggle('active', type === 'calls');
    document.getElementById('todayModelsTable').style.display = type === 'tokens' ? 'table-row-group' : 'none';
    document.getElementById('todayCallsTable').style.display = type === 'calls' ? 'table-row-group' : 'none';
    if (!skipRender && _analyticsData) {
        renderTblData(type === 'tokens' ? 'todayModelsTable' : 'todayCallsTable', _analyticsData.today_endpoints, type);
    } else if (skipRender && _analyticsData) {
        renderTblData('todayModelsTable', _analyticsData.today_endpoints, 'tokens');
        renderTblData('todayCallsTable', _analyticsData.today_endpoints, 'calls');
    }
}

function switchTblMonth(type, skipRender) {
    document.getElementById('btnTblMonthToken').classList.toggle('active', type === 'tokens');
    document.getElementById('btnTblMonthCall').classList.toggle('active', type === 'calls');
    document.getElementById('monthModelsTable').style.display = type === 'tokens' ? 'table-row-group' : 'none';
    document.getElementById('monthCallsTable').style.display = type === 'calls' ? 'table-row-group' : 'none';
    if (!skipRender && _analyticsData) {
        renderTblData(type === 'tokens' ? 'monthModelsTable' : 'monthCallsTable', _analyticsData.month_endpoints, type);
    } else if (skipRender && _analyticsData) {
        renderTblData('monthModelsTable', _analyticsData.month_endpoints, 'tokens');
        renderTblData('monthCallsTable', _analyticsData.month_endpoints, 'calls');
    }
}

let logAutoScroll = true;
const logContainer = document.getElementById('logContainer');
if (logContainer) {
    logContainer.addEventListener('scroll', () => {
        logAutoScroll = logContainer.scrollHeight - logContainer.clientHeight <= logContainer.scrollTop + 20;
    });
}
function addLogLine(entry) {
    if (!logContainer) return;
    const d = document.createElement('div');
    d.className = 'log-line';
    d.innerHTML = `<span class="log-time">[${entry.time}]</span> <span class="log-${entry.level}">[${entry.level}]</span> <span class="log-msg">${esc(entry.msg)}</span>`;
    logContainer.appendChild(d);
    if (logContainer.children.length > 300) logContainer.removeChild(logContainer.firstChild);
    if (logAutoScroll) logContainer.scrollTop = logContainer.scrollHeight;
}
let _lastLogId = 0;
async function pollLogs() {
    try {
        const logs = await api('GET', '/api/logs?since=' + _lastLogId);
        if (logs && logs.length > 0) {
            for (let entry of logs) {
                addLogLine(entry);
                _lastLogId = Math.max(_lastLogId, entry.id);
            }
        }
    } catch(err){}
    setTimeout(pollLogs, 2000);
}
pollLogs();

refresh();
loadSecurity();
loadModelAliases();
loadModelTokenStats();
setInterval(() => {
    refresh();
    loadChatLogs(chatLogsPage);
}, 3000);
let chatLogsPage = 0;
let currentChatLogs = [];
async function initChatLogs() {
  document.getElementById('clPrompt').textContent = '';
  document.getElementById('clCompletion').textContent = '';
  document.getElementById('clMeta').textContent = '';
  chatLogsPage = 0;
  await loadChatLogs(0);
}
initChatLogs();
let selectedChatLogId = null;
let _clSignature = '';
async function loadChatLogs(page) {
  if (page < 0) return;
  const limit = 50;
  const offset = page * limit;
  const res = await api('GET', `/api/chat-logs?limit=${limit}&offset=${offset}`);
  if (!res || !res.logs) return;
  chatLogsPage = page;
  currentChatLogs = res.logs || [];

  const total = res.total || 0;
  const maxPage = Math.max(0, Math.ceil(total / limit) - 1);

  document.getElementById('clPageSpan').textContent = `${page + 1} / ${maxPage + 1}`;
  document.getElementById('clPrevBtn').disabled = page <= 0;
  document.getElementById('clNextBtn').disabled = page >= maxPage;

  const listEl = document.getElementById('chatLogsList');
  if (currentChatLogs.length === 0) {
    listEl.innerHTML = '<div style="padding:20px;text-align:center;color:var(--text-dim);">暂无日志记录</div>';
    _clSignature = '';
    return;
  }

  // 数据没变化时不重绘列表，避免 3 秒轮询把用户正要点击的行刷掉
  const sig = page + ':' + currentChatLogs.map(l => l.id).join(',');
  if (sig !== _clSignature) {
    _clSignature = sig;
    listEl.innerHTML = currentChatLogs.map(log => `
      <div class="hover-bg cl-row" data-logid="${log.id}" style="padding:8px 12px; border-bottom:1px solid var(--border); display:grid; grid-template-columns: 80px 1fr 1fr 80px; gap:8px; font-size:12px; cursor:pointer; transition:background 0.2s;" onclick="viewChatLog(${log.id})">
        <div style="color:var(--text-dim);">${(log.timestamp || '').split(' ')[1] || ''}</div>
        <div style="white-space:nowrap;overflow:hidden;text-overflow:ellipsis;" title="${esc(log.endpoint_name)}">${esc(log.endpoint_name)}</div>
        <div style="white-space:nowrap;overflow:hidden;text-overflow:ellipsis;color:var(--accent-light);" title="${esc(log.model)}">${esc(log.model)}</div>
        <div style="color:var(--green);">${log.total_tokens}</div>
      </div>
    `).join('');
  }
  // 无选中时自动展示最新一条；有选中则维持高亮
  if (selectedChatLogId === null) viewChatLog(currentChatLogs[0].id);
  else updateClSelection();
}
function viewChatLog(id) {
  const log = currentChatLogs.find(l => l.id === id);
  if (!log) return;
  selectedChatLogId = id;
  document.getElementById('clPrompt').textContent = log.prompt || '';
  document.getElementById('clCompletion').textContent = log.completion || '';
  document.getElementById('clMeta').innerHTML = `<span style="color:var(--green)">${log.total_tokens} Tokens</span> <span style="margin-left:10px;color:var(--yellow)">${log.latency_ms}ms</span>`;
  updateClSelection();
}
function updateClSelection() {
  document.querySelectorAll('#chatLogsList .cl-row').forEach(el => {
    el.style.background = (String(selectedChatLogId) === el.dataset.logid) ? 'rgba(94,92,230,.18)' : '';
  });
}
async function clearChatLogs() {
  if (!confirm('确定要清空所有对话日志记录吗？此操作不可逆。')) return;
  await api('DELETE', '/api/chat-logs');
  toast('已清空', 'success');
  selectedChatLogId = null; _clSignature = '';
  document.getElementById('clPrompt').textContent = '';
  document.getElementById('clCompletion').textContent = '';
  document.getElementById('clMeta').textContent = '';
  loadChatLogs(0);
}

// Add CSS for hover
const style = document.createElement('style');
style.innerHTML = `
  .hover-bg:hover { background: rgba(255,255,255,0.05); }
`;
document.head.appendChild(style);

async function clearSysLogs() {
  if (!confirm('确定要清空系统日志吗？')) return;
  await api('DELETE', '/api/logs');
  document.getElementById('logContainer').innerHTML = '';
  toast('已清空日志', 'success');
}

async function clearTokenStats() {
  if (!confirm('确定要清空所有数据面板的 Token 统计记录吗？此操作不可逆。')) return;
  await api('DELETE', '/api/token-stats');
  toast('统计数据已清空', 'success');
  loadAnalytics();
}

</script>

<div id="testDrawer">
  <div class="drawer-header">
    <span id="testDrawerTitle">🧪 测试</span>
    <button class="btn btn-ghost btn-sm" onclick="closeTestDrawer()" style="padding: 2px 6px;">✖</button>
  </div>
  <div class="drawer-body">
    <input type="hidden" id="testTargetId" value="">
    <div id="testResult" class="test-result" style="margin-top:0; max-height:200px; display:none;"></div>
    <div class="test-input-row" style="display:flex; gap:8px;">
      <input type="text" id="testMsg" placeholder="测试消息..." value="用一句话介绍自己" style="flex:1">
      <input type="file" id="testImage" accept="image/*" style="display:none;" onchange="previewTestImage(this)">
      <button class="btn btn-ghost" onclick="document.getElementById('testImage').click()" title="上传图片测试" id="btnTestImage" style="padding:0 8px;font-size:16px;">🖼️</button>
      <button class="btn btn-primary" onclick="sendTest()">发送</button>
    </div>
  </div>
</div>
</body>

</html>"""


class Handler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        pass

    def _send_json(self, code, data, extra_headers=None):
        try:
            body = json.dumps(data, ensure_ascii=False).encode("utf-8")
            self.send_response(code)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            for k, v in (extra_headers or {}).items():
                self.send_header(k, v)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        except ConnectionError:
            pass

    def _send_html(self, html, code=200):
        try:
            body = html.encode("utf-8")
            self.send_response(code)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        except ConnectionError:
            pass

    def _session_cookie_header(self, token):
        return f"{SESSION_COOKIE}={token}; HttpOnly; SameSite=Strict; Path=/; Max-Age={SESSION_MAX_AGE}"

    def _expired_session_cookie_header(self):
        return f"{SESSION_COOKIE}=; HttpOnly; SameSite=Strict; Path=/; Max-Age=0"

    def _is_admin_authenticated(self):
        return security_manager.is_authenticated(self.headers.get("Cookie", ""))

    def _send_admin_unauthorized(self):
        self._send_json(401, {"ok": False, "error": "未登录"})

    def _send_client_unauthorized(self):
        self._send_json(401, {"error": {"message": "Invalid API key", "type": "invalid_request_error"}})

    def _read_body(self):
        try:
            length = int(self.headers.get("Content-Length", 0))
            if length:
                return json.loads(self.rfile.read(length))
            return {}
        except Exception:
            return {}

    def do_GET(self):
        cp = urlparse(self.path).path
        if cp in ("/v1", "/v1/", "/v1/models", "/models"):
            if not security_manager.verify_client_api_key(self.headers):
                self._send_client_unauthorized()
                return
            res = api_handler("GET", self.path, {})
            self._send_json(res[0], res[1])
        elif cp == "/api/auth/status":
            self._send_json(200, {"ok": True, "authenticated": self._is_admin_authenticated()})
        elif cp == "/login":
            if self._is_admin_authenticated():
                self.send_response(302)
                self.send_header("Location", "/")
                self.end_headers()
            else:
                self._send_html(LOGIN_HTML)
        elif cp == "/" or cp == "/index.html":
            if self._is_admin_authenticated():
                self._send_html(GUI_HTML)
            else:
                self._send_html(LOGIN_HTML, code=401)
        elif self.path.startswith("/api/export-stats"):
            if not self._is_admin_authenticated():
                self._send_admin_unauthorized()
                return
            csv_data = token_tracker.export_csv()
            try:
                body = csv_data.encode("utf-8-sig")
                self.send_response(200)
                self.send_header("Content-Type", "text/csv; charset=utf-8")
                self.send_header("Content-Disposition", "attachment; filename=token_stats.csv")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            except ConnectionError:
                pass
        elif cp.startswith("/api/"):
            if not self._is_admin_authenticated():
                self._send_admin_unauthorized()
                return
            res = api_handler("GET", self.path, {})
            if len(res) == 3 and res[2] is True:
                code, stream_gen = res[0], res[1]
                try:
                    self.send_response(code)
                    self.send_header("Content-Type", "text/event-stream; charset=utf-8")
                    self.send_header("Cache-Control", "no-cache")
                    self.send_header("Connection", "close")
                    self.end_headers()
                    for chunk in stream_gen:
                        self.wfile.write(chunk)
                        self.wfile.flush()
                    self.close_connection = True
                except ConnectionError:
                    pass
            else:
                self._send_json(res[0], res[1])
        else:
            self._send_json(404, {"error": "Not found"})

    def do_POST(self):
        cp = urlparse(self.path).path
        if cp == "/api/auth/login":
            body = self._read_body()
            if security_manager.verify_login(body.get("username", ""), body.get("password", "")):
                token = security_manager.create_session()
                self._send_json(200, {"ok": True}, {"Set-Cookie": self._session_cookie_header(token)})
            else:
                self._send_json(401, {"ok": False, "error": "账号或密码不正确"})
            return
        if cp == "/api/auth/logout":
            security_manager.destroy_session(self.headers.get("Cookie", ""))
            self._send_json(200, {"ok": True}, {"Set-Cookie": self._expired_session_cookie_header()})
            return
        if cp in ("/v1/chat/completions", "/chat/completions", "/v1/responses", "/responses"):
            if not security_manager.verify_client_api_key(self.headers):
                self._send_client_unauthorized()
                return
        elif cp.startswith("/api/") and not self._is_admin_authenticated():
            self._send_admin_unauthorized()
            return
        body = self._read_body()
        res = api_handler("POST", self.path, body)
        
        if len(res) == 3 and res[2] is True:
            code, stream_gen = res[0], res[1]
            try:
                self.send_response(code)
                self.send_header("Content-Type", "text/event-stream; charset=utf-8")
                self.send_header("Cache-Control", "no-cache")
                self.send_header("Connection", "close")
                self.end_headers()
                
                for chunk in stream_gen:
                    self.wfile.write(chunk)
                    self.wfile.flush()
                self.close_connection = True
            except ConnectionError:
                pass
        else:
            self._send_json(res[0], res[1])

    def do_PUT(self):
        cp = urlparse(self.path).path
        if cp.startswith("/api/") and not self._is_admin_authenticated():
            self._send_admin_unauthorized()
            return
        body = self._read_body()
        res = api_handler("PUT", self.path, body)
        self._send_json(res[0], res[1])

    def do_DELETE(self):
        cp = urlparse(self.path).path
        if cp.startswith("/api/") and not self._is_admin_authenticated():
            self._send_admin_unauthorized()
            return
        res = api_handler("DELETE", self.path, {})
        self._send_json(res[0], res[1])


def main():
    import sys
    if sys.stdout.encoding.lower() != 'utf-8':
        try: sys.stdout.reconfigure(encoding='utf-8')
        except: pass
    port_text = os.environ.get("PORT", "5200")
    try:
        port = int(port_text)
    except ValueError:
        raise SystemExit(f"Invalid PORT value: {port_text!r}")
    if not 1 <= port <= 65535:
        raise SystemExit(f"Invalid PORT value: {port}")
    server = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    print(f"\n  ⚡ API Pool 管理面板已启动")
    print(f"  🌐 管理面板访问: http://localhost:{port}")
    print(f"  🔗 客户端 Base URL: http://localhost:{port}/v1")
    print(f"  📋 已加载 {len(pool._endpoints)} 个端点")
    print(f"  🩺 健康检测: 启动时自动检测 + 每 {HEALTH_CHECK_INTERVAL}秒 复检\n")
    if security_manager.bootstrap:
        print("  🔐 首次启动安全凭据（请尽快登录后修改）")
        print(f"  👤 管理员账号: {security_manager.bootstrap['username']}")
        if security_manager.bootstrap.get("password"):
            print(f"  🔑 管理员密码: {security_manager.bootstrap['password']}")
        else:
            print("  🔑 管理员密码: 已从环境变量 API_POOL_ADMIN_PASSWORD 读取")
        if security_manager.bootstrap.get("client_api_key"):
            print(f"  🧩 客户端 API Key: {security_manager.bootstrap['client_api_key']}")
        else:
            print("  🧩 客户端 API Key: 已从环境变量 API_POOL_CLIENT_API_KEY 读取")
        print("")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n已停止")
        server.server_close()


if __name__ == "__main__":
    main()
