"""账户配置层（Phase 0：多账户 / 本地 / 远程，一进程一账户）

设计约束（对齐《SDK 阶段实施规划 v1.3》Phase 0 + 用户 2026-10-08 定案）：
  - 多账户走配置层：一个策略进程绑定一个账户，多账户 = 多进程实例。
  - 凭证安全：yaml 只存 ``${ENV_VAR}`` 引用，真实值在 ``klinequant/.env``，
    明文绝不落本文件 / 日志 / 记忆。
  - 插值语义：``${VAR}`` 严格模式（VAR 未设置即报错，绝不回退空串，避免静默
    用错凭证）；``${VAR:-default}`` 允许缺省（用于全可选的 MT5 连接参数）。
  - 解析优先级：CLI ``--account`` > env ``KQ_ACCOUNT`` > ``main:true`` 账户。
  - 零破坏：无 accounts.yaml / 无匹配账户 / 未显式指定且该市场无 main 时，
    ``resolve_account`` 返回 ``None``，调用方回落到既有 env 行为。

惰性插值：``resolve_account`` 先按 name/market/main（均为字面量，无需插值）
选中账户，再对**选中的那一个**做凭证插值。故运行 FX 主账户不会因加密账户的
严格占位符缺失而报错。
"""
from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

__all__ = [
    "AccountConfigError",
    "Mt5Account",
    "BinanceAccount",
    "AccountConfig",
    "load_accounts",
    "resolve_account",
]


class AccountConfigError(ValueError):
    """账户配置错误（文件损坏 / 插值缺失 / 未知账户名 / 多个 main 等）。"""


# ${VAR:-default} 允许缺省；${VAR} 严格（缺失即报错）
_ENV_DEFAULT = re.compile(r"\$\{(\w+):-([^}]*)\}")
_ENV_STRICT = re.compile(r"\$\{(\w+)\}")


def _interpolate(value: Any) -> Any:
    """递归展开 ``${VAR}`` / ``${VAR:-default}`` 占位符。

    - ``${VAR:-default}``：VAR 未设置或为空 → 用 default（default 可空）。
    - ``${VAR}``：VAR 不在 ``os.environ`` → 抛 :class:`AccountConfigError`。
    - 非字符串原样返回；dict / list 递归处理。
    """
    if isinstance(value, str):
        def _repl_default(m: re.Match[str]) -> str:
            var, default = m.group(1), m.group(2)
            env = os.environ.get(var)
            return env if env not in (None, "") else default

        out = _ENV_DEFAULT.sub(_repl_default, value)

        def _repl_strict(m: re.Match[str]) -> str:
            var = m.group(1)
            if var not in os.environ:
                raise AccountConfigError(
                    f"账户配置引用的环境变量 ${{{var}}} 未设置"
                    f"（请在 klinequant/.env 配置，或改用 ${{{var}:-默认值}} 形式）"
                )
            return os.environ[var]

        return _ENV_STRICT.sub(_repl_strict, out)
    if isinstance(value, dict):
        return {k: _interpolate(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_interpolate(v) for v in value]
    return value


@dataclass
class Mt5Account:
    """MT5（外汇）账户连接参数。

    与 :meth:`gateway.market_sources.mt5_driver.Mt5Api.init_kwargs` 同构：
    全部字段可选，缺省（空串）时连接本机已运行并登录的 MT5 终端。
    """

    terminal_path: str = ""
    login: str = ""
    password: str = ""
    server: str = ""
    host: str = ""  # 远程平台 IP（终端在远程机器时归 Phase 3 OrderAgent，本机 API 不消费）

    def init_kwargs(self) -> dict[str, Any]:
        """生成 ``Mt5Api.initialize(**kwargs)`` 参数（等价 Mt5Api.init_kwargs 逻辑）。"""
        kw: dict[str, Any] = {}
        if self.terminal_path:
            kw["path"] = self.terminal_path
        if self.login:
            kw["login"] = int(self.login)
            kw["password"] = self.password
            if self.server:
                kw["server"] = self.server
        return kw


@dataclass
class BinanceAccount:
    """币安（加密）账户连接参数（USDT-M Futures）。"""

    api_key: str = ""
    api_secret: str = ""
    testnet: bool = True
    proxy: str = ""
    rest_base: str = ""
    ws_base: str = ""


@dataclass
class AccountConfig:
    """单个账户的解析结果。"""

    name: str
    market: str  # fx | crypto
    main: bool = False
    role: str = "standalone"  # standalone | lead | follower（后两者 Phase 3 用）
    magic: int | None = None  # R1：显式覆盖派生 magic（None=按 (account,tag) 派生）
    mt5: Mt5Account | None = None
    binance: BinanceAccount | None = None
    # follower 形态预留字段（scale / symbol_map / lead_endpoint / token 等），Phase 3 消费
    extra: dict[str, Any] = field(default_factory=dict)


def _parse_account(raw: dict[str, Any], *, interpolate: bool) -> AccountConfig:
    """把一条账户 yaml dict 解析为 AccountConfig（interpolate=True 时展开凭证占位符）。"""
    if not isinstance(raw, dict):
        raise AccountConfigError(f"账户条目必须是映射，当前={type(raw).__name__}")
    data = _interpolate(raw) if interpolate else raw

    name = data.get("name")
    if not name:
        raise AccountConfigError("账户缺少 name 字段")
    market = data.get("market")
    if market not in ("fx", "crypto"):
        raise AccountConfigError(
            f"账户 {name} 的 market 必须为 fx|crypto，当前={market!r}"
        )

    mt5 = None
    m = data.get("mt5")
    if isinstance(m, dict):
        mt5 = Mt5Account(
            terminal_path=str(m.get("terminal_path", "") or ""),
            login=str(m.get("login", "") or ""),
            password=str(m.get("password", "") or ""),
            server=str(m.get("server", "") or ""),
            host=str(m.get("host", "") or ""),
        )

    bn = None
    b = data.get("binance")
    if isinstance(b, dict):
        bn = BinanceAccount(
            api_key=str(b.get("api_key", "") or ""),
            api_secret=str(b.get("api_secret", "") or ""),
            testnet=bool(b.get("testnet", True)),
            proxy=str(b.get("proxy", "") or ""),
            rest_base=str(b.get("rest_base", "") or ""),
            ws_base=str(b.get("ws_base", "") or ""),
        )

    magic = data.get("magic")
    known = {"name", "market", "main", "role", "magic", "mt5", "binance"}
    return AccountConfig(
        name=name,
        market=market,
        main=bool(data.get("main", False)),
        role=str(data.get("role", "standalone")),
        magic=int(magic) if magic is not None else None,
        mt5=mt5,
        binance=bn,
        extra={k: v for k, v in data.items() if k not in known},
    )


def _load_raw(config_path: Path | None = None) -> list[dict[str, Any]]:
    """加载 accounts.yaml 原始条目（不插值），并做结构性校验。

    name/market/main/role 均为字面量，无需插值即可用于账户选择，故此处不展开
    凭证占位符——避免某账户缺失的严格凭证影响其它账户的选择。
    """
    config_path = config_path or Path("config/accounts.yaml")
    if not config_path.exists():
        return []
    with open(config_path, encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}
    entries = raw.get("accounts") or []
    if not isinstance(entries, list):
        raise AccountConfigError("accounts.yaml 的 accounts 必须是列表")

    names: list[str] = []
    mains = 0
    for e in entries:
        if not isinstance(e, dict):
            raise AccountConfigError(f"账户条目必须是映射，当前={type(e).__name__}")
        nm = e.get("name")
        if not nm:
            raise AccountConfigError("账户缺少 name 字段")
        if e.get("market") not in ("fx", "crypto"):
            raise AccountConfigError(
                f"账户 {nm} 的 market 必须为 fx|crypto，当前={e.get('market')!r}"
            )
        names.append(nm)
        if bool(e.get("main", False)):
            mains += 1
    if len(names) != len(set(names)):
        raise AccountConfigError("accounts.yaml 存在重复的账户 name")
    if mains > 1:
        raise AccountConfigError("accounts.yaml 至多允许一个 main:true 账户")
    return entries


def load_accounts(
    config_path: Path | None = None, *, interpolate: bool = True
) -> list[AccountConfig]:
    """加载 accounts.yaml 并返回 AccountConfig 列表（无文件 → 空列表）。

    Args:
        config_path: yaml 路径，默认 ``config/accounts.yaml``。
        interpolate: 是否展开凭证占位符（默认 True；缺失严格占位符即报错）。
    """
    return [
        _parse_account(e, interpolate=interpolate) for e in _load_raw(config_path)
    ]


def resolve_account(
    name: str | None = None,
    *,
    market: str | None = None,
    config_path: Path | None = None,
) -> AccountConfig | None:
    """解析本进程要绑定的账户（选中后才对该账户做凭证插值）。

    优先级：显式 ``name``（CLI --account）> env ``KQ_ACCOUNT`` > ``main:true``。

    Args:
        name: 显式账户名（CLI --account）。
        market: 调用方市场（"fx"|"crypto"）；用于筛选 main 回落并校验显式账户市场匹配。
        config_path: yaml 路径，默认 ``config/accounts.yaml``。

    Returns:
        选中的 :class:`AccountConfig`；无 accounts.yaml / 无账户 / 未显式指定且
        该市场无 main → ``None``（调用方回落既有 env 行为，零破坏）。

    Raises:
        AccountConfigError: 显式账户名不存在、市场不匹配、或选中账户插值缺失。
    """
    entries = _load_raw(config_path)
    target = name or os.getenv("KQ_ACCOUNT") or ""

    if not entries:
        if target:
            raise AccountConfigError(
                f"未知账户名 {target!r}（accounts.yaml 无此账户或文件不存在）"
            )
        return None

    by_name = {e["name"]: e for e in entries}

    if target:
        if target not in by_name:
            raise AccountConfigError(
                f"未知账户名 {target!r}；可用账户：{', '.join(by_name)}"
            )
        chosen = by_name[target]
        if market and chosen.get("market") != market:
            raise AccountConfigError(
                f"账户 {target!r} 的 market={chosen.get('market')!r} 与所需 {market!r} 不匹配"
            )
        return _parse_account(chosen, interpolate=True)

    # 未显式指定 → 该市场的 main 账户（market=None 时取全局 main）
    for e in entries:
        if bool(e.get("main", False)) and (market is None or e.get("market") == market):
            return _parse_account(e, interpolate=True)
    return None
