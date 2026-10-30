@echo off
chcp 65001 >nul
setlocal
cd /d "%~dp0"

set "VENV_PY=klinequant\.venv\Scripts\python.exe"
set "DIST=webgui\dist\index.html"

rem 代理：币安 REST/WS 需经本地代理访问；THS 为国内直连，实测带代理亦正常。
rem 如你的网络无需代理（或端口不同），按需修改/删除下面两行。
set "HTTP_PROXY=http://127.0.0.1:7897"
set "HTTPS_PROXY=http://127.0.0.1:7897"

if not exist "%VENV_PY%" (
  echo [ERROR] venv python not found: %VENV_PY%
  echo Please create .venv and install dependencies first.
  pause
  exit /b 1
)

if not exist "%DIST%" (
  echo [INFO] webgui dist not found - building Vue app, first run may take a while...
  pushd webgui
  call npm run build
  popd
  if not exist "%DIST%" (
    echo [ERROR] webgui build failed - check the npm run build output above.
    pause
    exit /b 1
  )
)

rem 启动前清理残留旧后端：占用/抢绑 :8000 的 webstack，或 start_all.ps1 的 uvicorn gateway 进程。
rem 否则新实例抢不到 :8000，前端仍由旧代码服务（改了不生效）。按命令行精确匹配，不误伤无关 python。
rem 注意：下面 powershell 内联命令须保持在顶层（勿移入 if/for 括号块，否则 ) 会提前闭合块）。
echo [CLEAN] checking for stale KlineQuant backend on :8000 ...
powershell -NoProfile -Command "$p = Get-CimInstance Win32_Process | Where-Object { $_.Name -eq 'python.exe' -and ($_.CommandLine -like '*klinequant.webstack*' -or $_.CommandLine -like '*klinequant.gateway.app*') }; if ($p) { $p | ForEach-Object { Write-Host ('  killing stale PID ' + $_.ProcessId); Stop-Process -Id $_.ProcessId -Force }; Start-Sleep -Milliseconds 600 } else { Write-Host '  none found' }"

echo [START] KlineQuant dashboard + backtest on gateway :8000
echo   index-live.html : http://127.0.0.1:8000/   (default)
echo   Vue app      : http://127.0.0.1:8000/app/
echo   API docs     : http://127.0.0.1:8000/docs
echo Press Ctrl+C to stop.
"%VENV_PY%" -m klinequant.webstack --open /
pause
