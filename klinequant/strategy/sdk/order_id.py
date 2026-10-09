"""结构化 client_order_id 生成器（Phase R1 身份化）。

client_order_id 是崩溃恢复时从 venue 反查订单归属的唯一线索（journal 丢失也不
失身份），故必须内嵌 (account, tag) 且稳定可解析。同时受 venue 字段约束：
  - MT5：写入 order comment，长度上限 31 字符（超限会被终端截断，破坏反查）；
  - 币安：newClientOrderId 字符集 ``^[\\.A-Za-z0-9_-]{1,36}$``（不含 ':' 等）。

策略：完整形态 ``KQ-{account}-{tag}-{seq}-{rand4}``；字符集消毒（非法字符→'_'）；
超过 ``max_len`` 时退化为短哈希形态 ``KQ-{a6}-{t6}-{seq}-{rand4}``（account/tag
各取 sha256 前 6 位），仍保证归属可反查（短哈希对同一输入稳定映射）。
"""
from __future__ import annotations

import hashlib
import re
import threading
import uuid
from collections.abc import Callable

__all__ = ["OrderIdFactory", "DEFAULT_MAX_LEN"]

# venue 安全字符集（取 MT5 comment 与币安 newClientOrderId 的交集）：字母数字 . _ -
_UNSAFE = re.compile(r"[^A-Za-z0-9._-]")

# MT5 order comment 上限 31 字符（MT5/币安两者更严者）
DEFAULT_MAX_LEN = 31


def _short_hash(value: str, n: int = 6) -> str:
    """对 account/tag 取 sha256 前 n 位十六进制（稳定短哈希）。"""
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:n]


class OrderIdFactory:
    """线程安全的结构化 client_order_id 工厂。

    Args:
        max_len: id 长度上限（默认 31，兼容 MT5 comment；超限退化短哈希形态）。
        seq_provider: 可选外部 seq 源 ``Callable[[account, tag], int]``（R2 journal
            接入后复用同一序列）；None 时用内部 per-(account, tag) 递增计数器。
    """

    def __init__(
        self,
        max_len: int = DEFAULT_MAX_LEN,
        seq_provider: Callable[[str, str], int] | None = None,
    ):
        self._max_len = max_len
        self._seq_provider = seq_provider
        self._lock = threading.Lock()
        self._counters: dict[tuple[str, str], int] = {}

    def _next_seq(self, account: str, tag: str) -> int:
        if self._seq_provider is not None:
            return self._seq_provider(account, tag)
        with self._lock:
            key = (account, tag)
            self._counters[key] = self._counters.get(key, 0) + 1
            return self._counters[key]

    def generate(self, account: str, tag: str) -> str:
        """生成一个结构化 client_order_id（含 venue 长度/字符集合规处理）。"""
        seq = self._next_seq(account, tag)
        rand4 = uuid.uuid4().hex[:4]
        full = _UNSAFE.sub("_", f"KQ-{account}-{tag}-{seq}-{rand4}")
        if len(full) <= self._max_len:
            return full
        # 超限退化：account/tag 用短哈希，仍内嵌 seq/rand 保证唯一 + 可反查归属
        short = _UNSAFE.sub(
            "_", f"KQ-{_short_hash(account)}-{_short_hash(tag)}-{seq}-{rand4}"
        )
        return short[: self._max_len]

    def make(self, req) -> str:
        """resolver 注入签名：从 OrderRequest 取 (account, tag) 生成 id。"""
        return self.generate(
            getattr(req, "account", "") or "", getattr(req, "tag", "") or ""
        )
