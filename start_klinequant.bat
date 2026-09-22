@echo off
chcp 65001 >nul
setlocal
cd /d "%~dp0klinequant"

set "VENV_PY=.venv\Scripts\python.exe"
set "DIST=..\frontend\dist\index.html"

if not exist "%VENV_PY%" (
  echo [ERROR] venv python not found: %VENV_PY%
  echo Please create .venv and install dependencies first.
  pause
  exit /b 1
)

if not exist "%DIST%" (
  echo [INFO] frontend dist not found - building Vue app, first run may take a while...
  pushd ..\frontend
  call npm run build
  popd
  if not exist "%DIST%" (
    echo [ERROR] frontend build failed - check the npm run build output above.
    pause
    exit /b 1
  )
)

echo [START] KlineQuant dashboard + backtest on gateway :8000
echo   lc-live.html : http://127.0.0.1:8000/
echo   Vue app      : http://127.0.0.1:8000/app/
echo   API docs     : http://127.0.0.1:8000/docs
echo Press Ctrl+C to stop.
"%VENV_PY%" -m webstack
pause
