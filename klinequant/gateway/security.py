"""安全加固模块

遵循需求文档 §14.1/14.2：
    - API Key AES-256-GCM 加密存储
    - TLS 1.3 配置支持

加密方案：
    - 算法：AES-256-GCM（认证加密，防篡改）
    - 密钥派生：PBKDF2-HMAC-SHA256（从主密码派生）
    - 存储格式：base64(salt + nonce + ciphertext + tag)
"""
from __future__ import annotations

import base64
import hashlib
import logging
import os
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

# 加密存储文件路径
SECRETS_FILE = Path(os.getenv("SECRETS_FILE", "config/secrets.enc"))

# PBKDF2 参数
PBKDF2_ITERATIONS = 100_000
SALT_SIZE = 16
NONCE_SIZE = 12  # GCM 推荐 96-bit nonce
KEY_SIZE = 32  # AES-256


def _derive_key(master_password: str, salt: bytes) -> bytes:
    """从主密码派生 AES-256 密钥"""
    return hashlib.pbkdf2_hmac(
        "sha256",
        master_password.encode("utf-8"),
        salt,
        PBKDF2_ITERATIONS,
        dklen=KEY_SIZE,
    )


def encrypt_value(plaintext: str, master_password: str) -> str:
    """AES-256-GCM 加密

    Returns:
        base64 编码的密文（salt + nonce + ciphertext + tag）
    """
    try:
        from cryptography.hazmat.primitives.ciphers.aead import AESGCM
    except ImportError:
        # 降级：使用 Fernet（cryptography 包必须安装）
        raise ImportError("pip install cryptography  # AES-256-GCM 加密需要")

    salt = os.urandom(SALT_SIZE)
    key = _derive_key(master_password, salt)
    nonce = os.urandom(NONCE_SIZE)

    aesgcm = AESGCM(key)
    ciphertext = aesgcm.encrypt(nonce, plaintext.encode("utf-8"), None)
    # ciphertext 已包含 16-byte tag（GCM 自动附加）

    blob = salt + nonce + ciphertext
    return base64.b64encode(blob).decode("ascii")


def decrypt_value(encrypted: str, master_password: str) -> str:
    """AES-256-GCM 解密

    Args:
        encrypted: base64 编码的密文
        master_password: 主密码

    Returns:
        解密后的明文

    Raises:
        ValueError: 解密失败（密码错误或数据被篡改）
    """
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM

    blob = base64.b64decode(encrypted)
    if len(blob) < SALT_SIZE + NONCE_SIZE + 16:
        raise ValueError("Invalid encrypted data: too short")

    salt = blob[:SALT_SIZE]
    nonce = blob[SALT_SIZE:SALT_SIZE + NONCE_SIZE]
    ciphertext = blob[SALT_SIZE + NONCE_SIZE:]

    key = _derive_key(master_password, salt)
    aesgcm = AESGCM(key)

    try:
        plaintext = aesgcm.decrypt(nonce, ciphertext, None)
    except Exception:
        raise ValueError("Decryption failed: wrong password or data tampered")

    return plaintext.decode("utf-8")


class SecretStore:
    """加密密钥存储

    管理交易所 API Key/Secret 的加密存储。
    """

    def __init__(self, master_password: Optional[str] = None, file_path: Optional[Path] = None):
        self._master = master_password or os.getenv("KLINEQUANT_MASTER_PASSWORD", "default_dev_key")
        self._file = file_path or SECRETS_FILE
        self._secrets: dict[str, str] = {}
        self._load()

    def _load(self) -> None:
        """从文件加载已加密的密钥"""
        if not self._file.exists():
            return
        try:
            import json
            data = json.loads(self._file.read_text(encoding="utf-8"))
            for k, v in data.items():
                try:
                    self._secrets[k] = decrypt_value(v, self._master)
                except ValueError:
                    logger.warning(f"Failed to decrypt secret: {k}")
        except Exception as e:
            logger.error(f"Failed to load secrets: {e}")

    def save(self) -> None:
        """加密并持久化到文件"""
        import json
        self._file.parent.mkdir(parents=True, exist_ok=True)
        data = {}
        for k, v in self._secrets.items():
            data[k] = encrypt_value(v, self._master)
        self._file.write_text(json.dumps(data, indent=2), encoding="utf-8")

    def set(self, key: str, value: str) -> None:
        """设置密钥"""
        self._secrets[key] = value
        self.save()

    def get(self, key: str, default: str = "") -> str:
        """获取密钥"""
        return self._secrets.get(key, default)

    def delete(self, key: str) -> bool:
        """删除密钥"""
        if key in self._secrets:
            del self._secrets[key]
            self.save()
            return True
        return False

    def list_keys(self) -> list[str]:
        """列出所有密钥名称（不暴露值）"""
        return list(self._secrets.keys())

    def mask(self, key: str) -> str:
        """获取脱敏显示"""
        val = self._secrets.get(key, "")
        if len(val) <= 8:
            return "****"
        return val[:4] + "****" + val[-4:]
