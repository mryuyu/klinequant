"""安全加固模块单元测试"""
import pytest
from pathlib import Path

from gateway.security import encrypt_value, decrypt_value, SecretStore


class TestAESGCM:
    def test_encrypt_decrypt_roundtrip(self):
        """加密解密往返一致"""
        secret = "my_binance_api_key_12345"
        password = "master_pass"
        encrypted = encrypt_value(secret, password)
        assert encrypted != secret
        decrypted = decrypt_value(encrypted, password)
        assert decrypted == secret

    def test_wrong_password_fails(self):
        """错误密码解密失败"""
        encrypted = encrypt_value("hello", "correct")
        with pytest.raises(ValueError, match="Decryption failed"):
            decrypt_value(encrypted, "wrong")

    def test_tampered_data_fails(self):
        """篡改数据解密失败"""
        import base64
        encrypted = encrypt_value("test", "pass")
        blob = bytearray(base64.b64decode(encrypted))
        blob[-1] ^= 0xFF  # 翻转最后一字节
        tampered = base64.b64encode(bytes(blob)).decode()
        with pytest.raises(ValueError):
            decrypt_value(tampered, "pass")

    def test_different_encryptions_differ(self):
        """相同明文每次加密结果不同（随机 salt/nonce）"""
        e1 = encrypt_value("same", "pass")
        e2 = encrypt_value("same", "pass")
        assert e1 != e2


class TestSecretStore:
    def test_set_and_get(self, tmp_path):
        store = SecretStore(master_password="test", file_path=tmp_path / "s.enc")
        store.set("binance_api_key", "abc123")
        assert store.get("binance_api_key") == "abc123"

    def test_persistence(self, tmp_path):
        f = tmp_path / "s.enc"
        store1 = SecretStore(master_password="pw", file_path=f)
        store1.set("key1", "value1")

        # 新实例从文件加载
        store2 = SecretStore(master_password="pw", file_path=f)
        assert store2.get("key1") == "value1"

    def test_delete(self, tmp_path):
        store = SecretStore(master_password="pw", file_path=tmp_path / "s.enc")
        store.set("k", "v")
        assert store.delete("k") is True
        assert store.get("k") == ""
        assert store.delete("nonexist") is False

    def test_list_keys(self, tmp_path):
        store = SecretStore(master_password="pw", file_path=tmp_path / "s.enc")
        store.set("a", "1")
        store.set("b", "2")
        assert sorted(store.list_keys()) == ["a", "b"]

    def test_mask(self, tmp_path):
        store = SecretStore(master_password="pw", file_path=tmp_path / "s.enc")
        store.set("api_key", "ABCDEFGHIJ123456")
        masked = store.mask("api_key")
        assert "ABCD" in masked
        assert "3456" in masked
        assert "****" in masked
