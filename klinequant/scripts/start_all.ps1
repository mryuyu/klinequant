# KlineQuant v1.0.0-paper 一键启动脚本
# 启动：后端 API (8000) + 前端静态服务 (8080)
# 用法：.\start_all.ps1 [-NoProxy]

param(
    [switch]$NoProxy
)

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot   # klinequant 目录
$dist = Join-Path (Split-Path $root) "frontend\dist"

# 代理配置（币安 API 需要）
if (-not $NoProxy) {
    $env:HTTP_PROXY = "http://127.0.0.1:7897"
    $env:HTTPS_PROXY = "http://127.0.0.1:7897"
    Write-Host "[INFO] 代理已启用: http://127.0.0.1:7897" -ForegroundColor Cyan
}

Set-Location $root

# 1. 启动后端 (uvicorn, 8000)
Write-Host "[INFO] 启动后端 API Gateway (port 8000)..." -ForegroundColor Cyan
Start-Process -FilePath ".venv\Scripts\python.exe" `
    -ArgumentList "-m", "uvicorn", "gateway.app:app", "--host", "127.0.0.1", "--port", "8000" `
    -WorkingDirectory $root `
    -RedirectStandardOutput "logs\backend.out.log" `
    -RedirectStandardError "logs\backend.err.log" `
    -PassThru | ForEach-Object { $_.Id | Out-File logs\backend.pid -Encoding ascii }
Write-Host "  PID: $(Get-Content logs\backend.pid)" -ForegroundColor Green

# 2. 启动前端静态服务 (vite preview, 8080, 支持 SPA 路由回退)
$frontendDir = Split-Path $root | Join-Path -ChildPath "frontend"
$logDir = Join-Path $root "logs"
if (-not (Test-Path $logDir)) { New-Item -ItemType Directory -Path $logDir | Out-Null }
if (Test-Path $dist) {
    Write-Host "[INFO] 启动前端静态服务 (port 8080)..." -ForegroundColor Cyan
    $feLog = Join-Path $logDir "frontend.out.log"
    Start-Process -FilePath "cmd.exe" `
        -ArgumentList "/c", "npx vite preview --port 8080 --host 127.0.0.1 > `"$feLog`" 2>&1" `
        -WorkingDirectory $frontendDir `
        -PassThru | ForEach-Object { $_.Id | Out-File (Join-Path $logDir "frontend.pid") -Encoding ascii }
    Write-Host "  PID: $(Get-Content (Join-Path $logDir 'frontend.pid'))" -ForegroundColor Green
} else {
    Write-Host "[WARN] 未找到 frontend\dist，跳过前端静态服务（请先执行 npm run build）" -ForegroundColor Yellow
}

Start-Sleep -Seconds 2

# 3. 健康检查
Write-Host "`n[INFO] 健康检查..." -ForegroundColor Cyan
try {
    $health = Invoke-RestMethod -Uri "http://127.0.0.1:8000/api/system/health" -TimeoutSec 5
    Write-Host "  后端: healthy (version $($health.version))" -ForegroundColor Green
} catch {
    Write-Host "  后端: 未响应，请检查 logs\backend.err.log" -ForegroundColor Red
}

Write-Host @"

============================================
  KlineQuant v1.0.0-paper 已启动
  前端:  http://127.0.0.1:8080
  API:   http://127.0.0.1:8000/docs
  停止:  .\stop_all.ps1
============================================
"@ -ForegroundColor Green
