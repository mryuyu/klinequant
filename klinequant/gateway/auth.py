"""JWT 认证中间件

功能：
    - 签发 JWT Token（HS256）
    - 验证 Token（过期/无效）
    - FastAPI 依赖注入

遵循需求文档 §4.7 GW-002。
"""
from __future__ import annotations

import time
from typing import Optional

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
import jwt

# 配置
SECRET_KEY = "klinequant-secret-key-change-in-production"
ALGORITHM = "HS256"
TOKEN_EXPIRE_SECONDS = 86400  # 24 小时

security = HTTPBearer(auto_error=False)


def create_token(
    user_id: str,
    role: str = "admin",
    expires_in: int = TOKEN_EXPIRE_SECONDS,
) -> str:
    """签发 JWT Token

    Args:
        user_id: 用户 ID
        role: 角色（admin/operator/viewer）
        expires_in: 过期时间（秒）

    Returns:
        JWT token 字符串
    """
    now = int(time.time())
    payload = {
        "sub": user_id,
        "role": role,
        "iat": now,
        "exp": now + expires_in,
    }
    return jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)


def verify_token(token: str) -> dict:
    """验证 JWT Token

    Args:
        token: JWT token 字符串

    Returns:
        payload 字典

    Raises:
        HTTPException: Token 无效或过期
    """
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        return payload
    except jwt.ExpiredSignatureError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token expired",
            headers={"WWW-Authenticate": "Bearer"},
        )
    except jwt.InvalidTokenError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token",
            headers={"WWW-Authenticate": "Bearer"},
        )


async def get_current_user(
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(security),
) -> dict:
    """FastAPI 依赖：获取当前认证用户

    用法：
        @router.get("/api/xxx")
        async def handler(user=Depends(get_current_user)):
            ...
    """
    if credentials is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Not authenticated",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return verify_token(credentials.credentials)
