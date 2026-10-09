"""账户配置层单元测试（Phase 0）

覆盖 config/accounts.py：
  - accounts.yaml 解析（name/market/main/role/mt5/binance）
  - ${VAR} 严格插值（缺失即报错）与 ${VAR:-default} 缺省插值
  - resolve_account 优先级：CLI name > env KQ_ACCOUNT > main:true
  - 未知账户名 / 市场不匹配 / 无文件 的回落与报错
  - Mt5Account.init_kwargs 与 Mt5Api.init_kwargs 同构
  - 出厂 config/accounts.yaml 在 MT5 env 缺省下可解析（零破坏）
"""
from __future__ import annotations

from pathlib import Path

import pytest

from config import accounts as acct_mod
from config.accounts import (
    AccountConfigError,
    Mt5Account,
    load_accounts,
    resolve_account,
)

SHIPPED = Path(acct_mod.__file__).parent / "accounts.yaml"


def _write(tmp_path: Path, text: str) -> Path:
    p = tmp_path / "accounts.yaml"
    p.write_text(text, encoding="utf-8")
    return p


# ═══════════════════════════════════════════════════════════
# 插值语义
# ═══════════════════════════════════════════════════════════


class TestInterpolate:
    def test_strict_present(self, monkeypatch):
        monkeypatch.setenv("KQ_T_KEY", "abc")
        assert acct_mod._interpolate("${KQ_T_KEY}") == "abc"

    def test_strict_missing_raises(self, monkeypatch):
        monkeypatch.delenv("KQ_T_MISSING", raising=False)
        with pytest.raises(AccountConfigError):
            acct_mod._interpolate("${KQ_T_MISSING}")

    def test_default_when_missing(self, monkeypatch):
        monkeypatch.delenv("KQ_T_OPT", raising=False)
        assert acct_mod._interpolate("${KQ_T_OPT:-fallback}") == "fallback"

    def test_default_when_empty(self, monkeypatch):
        monkeypatch.setenv("KQ_T_OPT", "")
        assert acct_mod._interpolate("${KQ_T_OPT:-fallback}") == "fallback"

    def test_default_empty_allowed(self, monkeypatch):
        monkeypatch.delenv("KQ_T_OPT", raising=False)
        assert acct_mod._interpolate("${KQ_T_OPT:-}") == ""

    def test_env_value_used_over_default(self, monkeypatch):
        monkeypatch.setenv("KQ_T_OPT", "real")
        assert acct_mod._interpolate("${KQ_T_OPT:-fallback}") == "real"

    def test_recursive_dict_list(self, monkeypatch):
        monkeypatch.setenv("KQ_T_KEY", "v")
        data = {"a": ["${KQ_T_KEY}", 1], "b": {"c": "${KQ_T_KEY}"}}
        out = acct_mod._interpolate(data)
        assert out == {"a": ["v", 1], "b": {"c": "v"}}


# ═══════════════════════════════════════════════════════════
# load_accounts 解析
# ═══════════════════════════════════════════════════════════


class TestLoadAccounts:
    def test_missing_file_returns_empty(self, tmp_path):
        assert load_accounts(config_path=tmp_path / "nope.yaml") == []

    def test_parse_fx_and_crypto(self, tmp_path, monkeypatch):
        monkeypatch.setenv("KQ_T_APIKEY", "k")
        monkeypatch.setenv("KQ_T_SECRET", "s")
        p = _write(tmp_path, """
accounts:
  - name: fx1
    market: fx
    main: true
    mt5:
      login: "${KQ_T_LOGIN:-}"
      password: "${KQ_T_PASS:-}"
      server: "${KQ_T_SRV:-}"
  - name: c1
    market: crypto
    binance:
      api_key: "${KQ_T_APIKEY}"
      api_secret: "${KQ_T_SECRET}"
      testnet: false
""")
        accts = load_accounts(config_path=p)
        assert [a.name for a in accts] == ["fx1", "c1"]
        fx = accts[0]
        assert fx.market == "fx" and fx.main is True and fx.role == "standalone"
        assert fx.mt5 is not None and fx.mt5.login == ""
        c = accts[1]
        assert c.binance.api_key == "k" and c.binance.api_secret == "s"
        assert c.binance.testnet is False

    def test_duplicate_name_raises(self, tmp_path):
        p = _write(tmp_path, """
accounts:
  - {name: a, market: fx}
  - {name: a, market: crypto}
""")
        with pytest.raises(AccountConfigError):
            load_accounts(config_path=p)

    def test_multiple_main_raises(self, tmp_path):
        p = _write(tmp_path, """
accounts:
  - {name: a, market: fx, main: true}
  - {name: b, market: fx, main: true}
""")
        with pytest.raises(AccountConfigError):
            load_accounts(config_path=p)

    def test_bad_market_raises(self, tmp_path):
        p = _write(tmp_path, "accounts:\n  - {name: a, market: stock}\n")
        with pytest.raises(AccountConfigError):
            load_accounts(config_path=p)

    def test_strict_interp_missing_raises_on_load(self, tmp_path, monkeypatch):
        monkeypatch.delenv("KQ_T_NOPE", raising=False)
        p = _write(tmp_path, """
accounts:
  - name: c1
    market: crypto
    binance:
      api_key: "${KQ_T_NOPE}"
""")
        with pytest.raises(AccountConfigError):
            load_accounts(config_path=p, interpolate=True)


# ═══════════════════════════════════════════════════════════
# resolve_account 优先级与回落
# ═══════════════════════════════════════════════════════════


class TestResolveAccount:
    @pytest.fixture
    def cfg(self, tmp_path):
        return _write(tmp_path, """
accounts:
  - name: fx-main
    market: fx
    main: true
    mt5:
      login: "${KQ_T_LOGIN:-}"
  - name: fx-alt
    market: fx
    mt5:
      login: "${KQ_T_LOGIN:-}"
  - name: crypto1
    market: crypto
    binance:
      api_key: "${KQ_T_KEY:-}"
""")

    def test_no_file_returns_none(self, tmp_path):
        assert resolve_account(config_path=tmp_path / "nope.yaml") is None

    def test_main_fallback_by_market(self, cfg):
        a = resolve_account(market="fx", config_path=cfg)
        assert a is not None and a.name == "fx-main"

    def test_no_main_for_market_returns_none(self, cfg):
        # crypto1 非 main，未显式指定 → None（零破坏回落 env）
        assert resolve_account(market="crypto", config_path=cfg) is None

    def test_explicit_name_wins(self, cfg):
        a = resolve_account("fx-alt", market="fx", config_path=cfg)
        assert a is not None and a.name == "fx-alt"

    def test_env_kq_account(self, cfg, monkeypatch):
        monkeypatch.setenv("KQ_ACCOUNT", "fx-alt")
        a = resolve_account(market="fx", config_path=cfg)
        assert a is not None and a.name == "fx-alt"

    def test_cli_over_env(self, cfg, monkeypatch):
        monkeypatch.setenv("KQ_ACCOUNT", "fx-alt")
        a = resolve_account("fx-main", market="fx", config_path=cfg)
        assert a is not None and a.name == "fx-main"

    def test_unknown_name_raises(self, cfg):
        with pytest.raises(AccountConfigError):
            resolve_account("ghost", market="fx", config_path=cfg)

    def test_unknown_name_no_file_raises(self, tmp_path):
        with pytest.raises(AccountConfigError):
            resolve_account("ghost", config_path=tmp_path / "nope.yaml")

    def test_market_mismatch_raises(self, cfg):
        with pytest.raises(AccountConfigError):
            resolve_account("crypto1", market="fx", config_path=cfg)

    def test_lazy_interp_other_account_not_touched(self, tmp_path, monkeypatch):
        # crypto 严格凭证缺失，但解析 fx main 不应触发 crypto 的插值报错
        monkeypatch.delenv("KQ_T_STRICT", raising=False)
        p = _write(tmp_path, """
accounts:
  - name: fxm
    market: fx
    main: true
    mt5:
      login: "${KQ_T_LOGIN:-}"
  - name: cc
    market: crypto
    binance:
      api_key: "${KQ_T_STRICT}"
""")
        a = resolve_account(market="fx", config_path=p)
        assert a is not None and a.name == "fxm"
        # 显式选 crypto 才报缺失
        with pytest.raises(AccountConfigError):
            resolve_account("cc", market="crypto", config_path=p)


# ═══════════════════════════════════════════════════════════
# Mt5Account.init_kwargs 同构
# ═══════════════════════════════════════════════════════════


class TestMt5InitKwargs:
    def test_empty_when_no_login(self):
        assert Mt5Account().init_kwargs() == {}

    def test_path_only(self):
        assert Mt5Account(terminal_path="C:/t").init_kwargs() == {"path": "C:/t"}

    def test_login_password_server(self):
        kw = Mt5Account(login="123", password="pw", server="IC-Demo").init_kwargs()
        assert kw == {"login": 123, "password": "pw", "server": "IC-Demo"}

    def test_login_without_server(self):
        kw = Mt5Account(login="123", password="pw").init_kwargs()
        assert kw == {"login": 123, "password": "pw"}


# ═══════════════════════════════════════════════════════════
# 出厂 accounts.yaml（零破坏）
# ═══════════════════════════════════════════════════════════


class TestShippedConfig:
    @pytest.fixture(autouse=True)
    def _clear_env(self, monkeypatch):
        for v in ("MT5_TERMINAL_PATH", "MT5_LOGIN", "MT5_PASSWORD", "MT5_SERVER",
                  "KQ_ACCOUNT"):
            monkeypatch.delenv(v, raising=False)

    def test_fx_main_resolves_without_mt5_env(self):
        # MT5 env 全缺省 → fx-demo-main 仍可解析，init_kwargs 为空（连本机终端）
        a = resolve_account(market="fx", config_path=SHIPPED)
        assert a is not None and a.name == "fx-demo-main"
        assert a.mt5 is not None and a.mt5.init_kwargs() == {}

    def test_crypto_has_no_main(self):
        assert resolve_account(market="crypto", config_path=SHIPPED) is None

    def test_names_present(self):
        names = [a.name for a in load_accounts(config_path=SHIPPED, interpolate=False)]
        assert "fx-demo-main" in names and "crypto-demo" in names
