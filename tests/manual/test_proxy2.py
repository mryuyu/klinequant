"""测试代理端口 7898"""
import socket
import httpx

# 1. 端口连通性
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.settimeout(2)
result = s.connect_ex(("127.0.0.1", 7898))
s.close()
print(f"Port 7898: {'OPEN' if result == 0 else f'CLOSED(code={result})'}")

if result == 0:
    # 2. HTTP 代理测试
    try:
        with httpx.Client(proxy="http://127.0.0.1:7898", timeout=12) as c:
            r = c.get("https://api.binance.com/api/v3/ping")
            print(f"HTTP 代理: OK {r.status_code}")
    except Exception as e:
        print(f"HTTP 代理: FAIL {type(e).__name__}: {e}")

    # 3. SOCKS5 代理测试
    try:
        with httpx.Client(proxy="socks5://127.0.0.1:7898", timeout=12) as c:
            r = c.get("https://api.binance.com/api/v3/ping")
            print(f"SOCKS5 代理: OK {r.status_code}")
    except Exception as e:
        print(f"SOCKS5 代理: FAIL {type(e).__name__}: {e}")

    # 4. 对比 7897
    try:
        with httpx.Client(proxy="http://127.0.0.1:7897", timeout=12) as c:
            r = c.get("https://api.binance.com/api/v3/ping")
            print(f"7897 HTTP 代理: OK {r.status_code}")
    except Exception as e:
        print(f"7897 HTTP 代理: FAIL {type(e).__name__}")
