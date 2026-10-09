"""Phase R1 身份化单元测试：magic 派生 + 结构化 client_order_id + cancel 受 magic。

覆盖《SDK 阶段实施规划 v1.3》R1 验收点：
  - id 生成器长度/字符集合规（MT5 comment 31 / 币安 charset）
  - magic 派生稳定性（同 (account,tag) 恒定、异组合相异、int32 非零区间）
  - resolver magic 传播（显式覆盖 > 派生 > 默认）
  - resolver 注入式 client_order_id 生成器
  - Mt5Executor.cancel 按传入 magic 撤单
"""
import re
from decimal import Decimal

from core.trade_engine.executors.mt5_executor import Mt5Executor
from core.trade_engine.resolver import (
    OrderRequest,
    UnifiedResolver,
    derive_magic,
)
from protocol.types import Offset, OrderKind, OrderSide, SymbolInfo
from strategy.sdk.order_id import DEFAULT_MAX_LEN, OrderIdFactory

# 币安 newClientOrderId 字符集：^[\.A-Za-z0-9_-]{1,36}$
_BINANCE_CHARSET = re.compile(r"^[.A-Za-z0-9_-]+$")


def _spec(symbol="EURUSD") -> SymbolInfo:
    return SymbolInfo(
        symbol=symbol, market_type="FX",
        pip_size=Decimal("0.0001"), tick_size=Decimal("0.00001"),
        qty_step=Decimal("0.01"), min_qty=Decimal("0.01"), qty_max=Decimal("200"),
        can_short=True,
    )


def _open_req(**kw) -> OrderRequest:
    base = dict(
        symbol="EURUSD", tag="macd:1h", side=OrderSide.BUY, offset=Offset.OPEN,
        qty=Decimal("0.10"), kind=OrderKind.MARKET,
    )
    base.update(kw)
    return OrderRequest(**base)


# ─── derive_magic ───

def test_derive_magic_stable():
    """同 (account, tag) 恒定映射到同一 magic（跨调用稳定）"""
    assert derive_magic("fx-demo-main", "macd:1h") == derive_magic("fx-demo-main", "macd:1h")


def test_derive_magic_distinct_across_tag():
    """同账户不同 tag → 不同 magic（多周期隔离根基）"""
    assert derive_magic("fx-demo-main", "macd:1h") != derive_magic("fx-demo-main", "macd:15m")


def test_derive_magic_distinct_across_account():
    """不同账户同 tag → 不同 magic（多账户隔离）"""
    assert derive_magic("acct-a", "s:1h") != derive_magic("acct-b", "s:1h")


def test_derive_magic_int32_nonzero_range():
    """magic 落在 [1, 2^31-1]（MT5 int32、规避 0=无 magic 语义）"""
    for acct in ("a", "fx-demo-main", "crypto-demo", "x" * 50):
        for tag in ("1m", "macd:1h", "s:p:5m"):
            m = derive_magic(acct, tag)
            assert isinstance(m, int)
            assert 1 <= m <= 0x7FFFFFFF


# ─── OrderIdFactory ───

def test_order_id_charset_compliant():
    """生成的 id 只含 venue 安全字符（tag 内 ':' 被消毒）"""
    f = OrderIdFactory()
    oid = f.generate("fx-demo-main", "macd:1h")
    assert _BINANCE_CHARSET.match(oid), oid
    assert ":" not in oid


def test_order_id_within_max_len_short_inputs():
    """短 account/tag → 完整形态且不超上限"""
    f = OrderIdFactory()
    oid = f.generate("fx", "1h")
    assert len(oid) <= DEFAULT_MAX_LEN
    assert oid.startswith("KQ-fx-1h-")


def test_order_id_long_inputs_short_hash_fallback():
    """超长 account/tag → 退化短哈希形态，仍守长度上限 + 字符合规"""
    f = OrderIdFactory()
    oid = f.generate("a-very-long-account-name-exceeding-limit", "strategy-name:period-1h")
    assert len(oid) <= DEFAULT_MAX_LEN
    assert _BINANCE_CHARSET.match(oid), oid
    assert oid.startswith("KQ-")


def test_order_id_long_inputs_still_unique():
    """短哈希形态下不同 (account,tag) 仍相异（归属可反查）"""
    f = OrderIdFactory()
    a = f.generate("a-very-long-account-name-exceeding-limit", "strategy-one:1h")
    b = f.generate("a-very-long-account-name-exceeding-limit", "strategy-two:1h")
    assert a != b


def test_order_id_seq_increments_per_key():
    """同 (account,tag) seq 递增；不同 key 各自独立计数"""
    f = OrderIdFactory()
    ids = [f.generate("acct", "1h") for _ in range(3)]
    assert len(set(ids)) == 3  # 唯一
    # seq 段递增（完整形态第 4 段）
    seqs = [int(i.split("-")[3]) for i in ids]
    assert seqs == [1, 2, 3]
    # 另一 key 从 1 起
    other = f.generate("acct", "15m")
    assert int(other.split("-")[3]) == 1


def test_order_id_external_seq_provider():
    """注入外部 seq_provider 时以其为准（R2 journal 复用同一序列）"""
    f = OrderIdFactory(seq_provider=lambda a, t: 42)
    oid = f.generate("acct", "1h")
    assert "-42-" in oid


def test_order_id_make_from_request():
    """make(req) 从 OrderRequest 取 (account, tag)"""
    f = OrderIdFactory()
    oid = f.make(_open_req(account="fx-demo-main", tag="macd:1h"))
    assert _BINANCE_CHARSET.match(oid), oid
    assert len(oid) <= DEFAULT_MAX_LEN


# ─── resolver magic 传播 ───

def test_resolver_derives_magic_from_account():
    """account 有值、magic 未显式 → leg.magic = derive_magic(account, tag)"""
    r = UnifiedResolver()
    req = _open_req(account="fx-demo-main", tag="macd:1h")
    res = r.resolve(req, _spec())
    assert res.ok
    assert res.specs[0].magic == derive_magic("fx-demo-main", "macd:1h")


def test_resolver_explicit_magic_overrides():
    """显式 magic 覆盖派生"""
    r = UnifiedResolver()
    req = _open_req(account="fx-demo-main", tag="macd:1h", magic=123456)
    res = r.resolve(req, _spec())
    assert res.ok
    assert res.specs[0].magic == 123456


def test_resolver_default_magic_without_account():
    """无 account 无 magic → 保持 leg 默认 202609（零回归）"""
    r = UnifiedResolver()
    res = r.resolve(_open_req(), _spec())
    assert res.ok
    assert res.specs[0].magic == 202609


def test_resolver_close_leg_gets_magic():
    """CLOSE leg 同样获得派生 magic"""
    r = UnifiedResolver()
    req = OrderRequest(
        symbol="EURUSD", tag="macd:1h", side=OrderSide.SELL, offset=Offset.CLOSE,
        qty=Decimal("0.10"), kind=OrderKind.MARKET, account="fx-demo-main",
    )
    res = r.resolve(req, _spec(), position_volume=Decimal("0.10"),
                    available_to_close=Decimal("0.10"))
    assert res.ok
    assert res.specs[0].magic == derive_magic("fx-demo-main", "macd:1h")


def test_resolver_uses_injected_id_gen():
    """注入 order_id_gen 时 client_order_id 由生成器产出"""
    f = OrderIdFactory()
    r = UnifiedResolver(order_id_gen=f.make)
    req = _open_req(account="fx-demo-main", tag="macd:1h")
    res = r.resolve(req, _spec())
    assert res.ok
    oid = res.specs[0].client_order_id
    assert oid.startswith("KQ-")
    assert _BINANCE_CHARSET.match(oid), oid


def test_resolver_fallback_uuid_without_gen():
    """未注入生成器 → 回落随机 uuid 形态 KQ-<hex16>（零回归）"""
    r = UnifiedResolver()
    res = r.resolve(_open_req(), _spec())
    assert res.ok
    oid = res.specs[0].client_order_id
    assert oid.startswith("KQ-") and len(oid) == len("KQ-") + 16


def test_resolver_preserves_provided_client_order_id():
    """req 已带 client_order_id → 不覆盖"""
    f = OrderIdFactory()
    r = UnifiedResolver(order_id_gen=f.make)
    req = _open_req(client_order_id="KQ-manual-1")
    res = r.resolve(req, _spec())
    assert res.specs[0].client_order_id == "KQ-manual-1"


# ─── Mt5Executor.cancel 受 magic ───

class _CancelDriver:
    def __init__(self):
        self.sent = []

    def order_send(self, request):
        self.sent.append(request)
        return {"retcode": 10009, "order": request.get("order", 0)}


def test_mt5_cancel_uses_passed_magic():
    """cancel 传入 magic → 请求按该 magic 撤（多 tag 隔离）"""
    drv = _CancelDriver()
    ex = Mt5Executor(drv, magic=202609)
    assert ex.cancel(555, "EURUSD", magic=987654) is True
    assert drv.sent[-1]["magic"] == 987654


def test_mt5_cancel_falls_back_to_instance_magic():
    """cancel 未传 magic → 回落实例级 self._magic（零回归）"""
    drv = _CancelDriver()
    ex = Mt5Executor(drv, magic=202609)
    assert ex.cancel(555, "EURUSD") is True
    assert drv.sent[-1]["magic"] == 202609
