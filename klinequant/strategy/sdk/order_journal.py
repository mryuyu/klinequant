"""订单意图 WAL（Phase R2：先写后发 + fsync，崩溃恢复的账本级真相源）。

设计约束（对齐《SDK 阶段实施规划 v1.3》R2 + 真相源分层红线）：
  - 订单意图流水属**账本级**，必须本地 WAL 先写后发，绝不用缓存型存储；
  - ``PRAGMA synchronous=FULL``：每次 commit 落盘 fsync，最坏不丢已 begin 的意图；
  - 独立于 DuckDB batch_writer（不走缓冲），单文件 SQLite ``data/journal/{account}.db``；
  - ``tag`` 必须含 period（``{strategy}:{period}``），否则 R3 恢复会把在途挂单归错周期
    （多周期同账户下会把 1h 的挂单算进 15m 的 in_flight，串仓）；
  - 归档到 orders/fills 表属"可丢"层，与本 WAL 解耦（本模块只保证 WAL 不丢）。

崩溃恢复语义（R3 消费 :meth:`Journal.pending`）：
  - 终态：FILLED（持仓由 venue 对账恢复）/ DEAD（无需处理）；
  - 非终态（pending）：SUBMITTING（begin 后 submit 前崩溃，venue 结局未知）/
    IN_FLIGHT（挂单仍活着）/ UNKNOWN（submit 抛异常，结局未知）——均须凭
    client_order_id 向 venue 反查收敛。

线程安全：拓扑 Z 下多品种线程并发 begin/finish，单连接 + 互斥锁串行化
（``check_same_thread=False``）；量级小，不构成瓶颈。
"""
from __future__ import annotations

import logging
import sqlite3
import threading
import time
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Protocol, runtime_checkable

logger = logging.getLogger(__name__)

__all__ = [
    "JournalRow",
    "Journal",
    "SqliteJournal",
    "InMemoryJournal",
    "STATE_SUBMITTING",
    "STATE_FILLED",
    "STATE_IN_FLIGHT",
    "STATE_DEAD",
    "STATE_UNKNOWN",
    "TERMINAL_STATES",
    "PENDING_STATES",
]

# 订单意图状态机
STATE_SUBMITTING = "SUBMITTING"   # begin 已写、submit 未回（崩溃则 venue 结局未知）
STATE_FILLED = "FILLED"           # 终态：已成交（持仓由 venue 对账恢复）
STATE_IN_FLIGHT = "IN_FLIGHT"     # 非终态：挂单仍活着（R3 须恢复 in_flight + ticket）
STATE_DEAD = "DEAD"               # 终态：撤单/拒绝/过期（无需处理）
STATE_UNKNOWN = "UNKNOWN"         # 非终态：submit 抛异常，结局未知（R3 反查）

TERMINAL_STATES = frozenset({STATE_FILLED, STATE_DEAD})
PENDING_STATES = (STATE_SUBMITTING, STATE_IN_FLIGHT, STATE_UNKNOWN)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS order_journal (
    client_order_id TEXT PRIMARY KEY,
    account         TEXT NOT NULL DEFAULT '',
    tag             TEXT NOT NULL DEFAULT '',
    symbol          TEXT NOT NULL DEFAULT '',
    side            TEXT NOT NULL DEFAULT '',
    order_offset    TEXT NOT NULL DEFAULT '',
    qty             TEXT NOT NULL DEFAULT '0',
    price           TEXT,
    status          TEXT NOT NULL DEFAULT 'SUBMITTING',
    seq             INTEGER NOT NULL DEFAULT 0,
    ticket          INTEGER,
    filled_qty      TEXT NOT NULL DEFAULT '0',
    filled_price    TEXT,
    reason          TEXT NOT NULL DEFAULT '',
    created_at      REAL NOT NULL DEFAULT 0,
    updated_at      REAL NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_journal_status ON order_journal(status);
CREATE INDEX IF NOT EXISTS idx_journal_acct_tag ON order_journal(account, tag, status);
CREATE TABLE IF NOT EXISTS seq_counter (
    account  TEXT NOT NULL,
    tag      TEXT NOT NULL,
    last_seq INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (account, tag)
);
"""


def _dec(value: str | None, default: Decimal | None = None) -> Decimal | None:
    """SQLite TEXT → Decimal（None/非法值回落 default）。"""
    if value is None or value == "":
        return default
    try:
        return Decimal(value)
    except (InvalidOperation, ValueError):
        return default


@dataclass
class JournalRow:
    """一条订单意图流水记录。"""

    client_order_id: str
    account: str = ""
    tag: str = ""
    symbol: str = ""
    side: str = ""
    offset: str = ""
    qty: Decimal = Decimal("0")
    price: Decimal | None = None
    status: str = STATE_SUBMITTING
    seq: int = 0
    ticket: int | None = None
    filled_qty: Decimal = Decimal("0")
    filled_price: Decimal | None = None
    reason: str = ""
    created_at: float = 0.0
    updated_at: float = 0.0

    @property
    def is_pending(self) -> bool:
        """是否非终态（R3 须凭 client_order_id 向 venue 反查收敛）。"""
        return self.status in PENDING_STATES


@runtime_checkable
class Journal(Protocol):
    """订单意图 WAL 协议（后端可插拔：SqliteJournal / InMemoryJournal）。"""

    def next_seq(self, account: str, tag: str) -> int:
        """分配并持久化下一个 (account, tag) 序列号（Phase 3 分发 seq 复用同一序列）。"""
        ...

    def begin(
        self, *, client_order_id: str, account: str, tag: str, symbol: str,
        side: str, offset: str, qty: Decimal, price: Decimal | None = None,
        seq: int = 0,
    ) -> None:
        """submit **之前**写意图（status=SUBMITTING），先写后发 + fsync。"""
        ...

    def finish(
        self, client_order_id: str, status: str, *, ticket: int | None = None,
        filled_qty: Decimal = Decimal("0"), filled_price: Decimal | None = None,
        reason: str = "",
    ) -> None:
        """submit 返回后落终态/在途态（status ∈ FILLED/IN_FLIGHT/DEAD/UNKNOWN）。"""
        ...

    def pending(self, account: str | None = None, tag: str | None = None) -> list[JournalRow]:
        """所有非终态记录（可选按 account/tag 过滤），R3 恢复消费。"""
        ...

    def close(self) -> None:
        """关闭后端（flush + 释放连接）。"""
        ...


class SqliteJournal:
    """SQLite WAL 实现（``synchronous=FULL``，独立于 DuckDB，不走缓冲）。"""

    def __init__(self, db_path: str | Path, *, synchronous: str = "FULL"):
        self._db_path = Path(db_path)
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(str(self._db_path), check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        # 账本级持久化：每次 commit fsync；WAL 模式便于 R3 恢复期并发读
        self._conn.execute(f"PRAGMA synchronous={synchronous}")
        self._conn.execute("PRAGMA journal_mode=WAL")
        with self._lock:
            self._conn.executescript(_SCHEMA)
            self._conn.commit()
        logger.info(f"OrderJournal ready: {self._db_path} (synchronous={synchronous})")

    # ─── seq ───

    def next_seq(self, account: str, tag: str) -> int:
        with self._lock:
            cur = self._conn.execute(
                "SELECT last_seq FROM seq_counter WHERE account=? AND tag=?",
                (account, tag),
            )
            row = cur.fetchone()
            if row is None:
                seq = 1
                self._conn.execute(
                    "INSERT INTO seq_counter(account, tag, last_seq) VALUES(?,?,?)",
                    (account, tag, seq),
                )
            else:
                seq = int(row[0]) + 1
                self._conn.execute(
                    "UPDATE seq_counter SET last_seq=? WHERE account=? AND tag=?",
                    (seq, account, tag),
                )
            self._conn.commit()
            return seq

    # ─── 意图流水 ───

    def begin(
        self, *, client_order_id: str, account: str = "", tag: str = "", symbol: str = "",
        side: str = "", offset: str = "", qty: Decimal = Decimal("0"),
        price: Decimal | None = None, seq: int = 0,
    ) -> None:
        now = time.time()
        with self._lock:
            self._conn.execute(
                "INSERT OR REPLACE INTO order_journal("
                "client_order_id, account, tag, symbol, side, order_offset, qty, price,"
                " status, seq, ticket, filled_qty, filled_price, reason, created_at, updated_at"
                ") VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    client_order_id, account, tag, symbol, side, offset,
                    str(qty), str(price) if price is not None else None,
                    STATE_SUBMITTING, seq, None, "0", None, "", now, now,
                ),
            )
            self._conn.commit()

    def finish(
        self, client_order_id: str, status: str, *, ticket: int | None = None,
        filled_qty: Decimal = Decimal("0"), filled_price: Decimal | None = None,
        reason: str = "",
    ) -> None:
        now = time.time()
        with self._lock:
            cur = self._conn.execute(
                "UPDATE order_journal SET status=?, ticket=?, filled_qty=?, filled_price=?,"
                " reason=?, updated_at=? WHERE client_order_id=?",
                (
                    status, ticket, str(filled_qty),
                    str(filled_price) if filled_price is not None else None,
                    reason, now, client_order_id,
                ),
            )
            self._conn.commit()
            if cur.rowcount == 0:
                logger.warning(
                    f"journal.finish: no row for client_order_id={client_order_id} "
                    f"(begin missing?)"
                )

    def pending(
        self, account: str | None = None, tag: str | None = None
    ) -> list[JournalRow]:
        clauses = ["status IN (?,?,?)"]
        params: list[object] = list(PENDING_STATES)
        if account is not None:
            clauses.append("account=?")
            params.append(account)
        if tag is not None:
            clauses.append("tag=?")
            params.append(tag)
        sql = (
            "SELECT * FROM order_journal WHERE " + " AND ".join(clauses)
            + " ORDER BY created_at ASC, seq ASC"
        )
        with self._lock:
            rows = self._conn.execute(sql, params).fetchall()
        return [_to_row(r) for r in rows]

    def close(self) -> None:
        with self._lock:
            try:
                self._conn.commit()
                self._conn.close()
            except Exception as e:  # pragma: no cover - 关闭异常不阻断退出
                logger.warning(f"SqliteJournal close error: {e}")


def _to_row(r: sqlite3.Row) -> JournalRow:
    return JournalRow(
        client_order_id=r["client_order_id"],
        account=r["account"],
        tag=r["tag"],
        symbol=r["symbol"],
        side=r["side"],
        offset=r["order_offset"],
        qty=_dec(r["qty"], Decimal("0")) or Decimal("0"),
        price=_dec(r["price"]),
        status=r["status"],
        seq=int(r["seq"] or 0),
        ticket=(int(r["ticket"]) if r["ticket"] is not None else None),
        filled_qty=_dec(r["filled_qty"], Decimal("0")) or Decimal("0"),
        filled_price=_dec(r["filled_price"]),
        reason=r["reason"] or "",
        created_at=float(r["created_at"] or 0.0),
        updated_at=float(r["updated_at"] or 0.0),
    )


class InMemoryJournal:
    """内存 WAL（回测/单测用，无落盘）。接口与 SqliteJournal 同构。"""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._rows: dict[str, JournalRow] = {}
        self._seq: dict[tuple[str, str], int] = {}

    def next_seq(self, account: str, tag: str) -> int:
        with self._lock:
            key = (account, tag)
            self._seq[key] = self._seq.get(key, 0) + 1
            return self._seq[key]

    def begin(
        self, *, client_order_id: str, account: str = "", tag: str = "", symbol: str = "",
        side: str = "", offset: str = "", qty: Decimal = Decimal("0"),
        price: Decimal | None = None, seq: int = 0,
    ) -> None:
        now = time.time()
        with self._lock:
            self._rows[client_order_id] = JournalRow(
                client_order_id=client_order_id, account=account, tag=tag, symbol=symbol,
                side=side, offset=offset, qty=qty, price=price,
                status=STATE_SUBMITTING, seq=seq, created_at=now, updated_at=now,
            )

    def finish(
        self, client_order_id: str, status: str, *, ticket: int | None = None,
        filled_qty: Decimal = Decimal("0"), filled_price: Decimal | None = None,
        reason: str = "",
    ) -> None:
        with self._lock:
            row = self._rows.get(client_order_id)
            if row is None:
                logger.warning(
                    f"journal.finish: no row for client_order_id={client_order_id}"
                )
                return
            row.status = status
            row.ticket = ticket
            row.filled_qty = filled_qty
            row.filled_price = filled_price
            row.reason = reason
            row.updated_at = time.time()

    def pending(
        self, account: str | None = None, tag: str | None = None
    ) -> list[JournalRow]:
        with self._lock:
            out = [
                r for r in self._rows.values()
                if r.status in PENDING_STATES
                and (account is None or r.account == account)
                and (tag is None or r.tag == tag)
            ]
        return sorted(out, key=lambda r: (r.created_at, r.seq))

    def close(self) -> None:
        pass
