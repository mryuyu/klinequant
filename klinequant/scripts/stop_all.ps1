# KlineQuant v1.0.0-paper 一键停止脚本
# 停止 start_all.ps1 启动的后端和前端进程

$root = Split-Path -Parent $PSScriptRoot
$logDir = Join-Path $root "logs"

function Stop-ByPid($pidFile, $name) {
    if (Test-Path $pidFile) {
        $pid = (Get-Content $pidFile | Select-Object -First 1).Trim()
        if ($pid -and (Get-Process -Id $pid -ErrorAction SilentlyContinue)) {
            Stop-Process -Id $pid -Force
            Write-Host "[OK] $name (PID $pid) 已停止" -ForegroundColor Green
        } else {
            Write-Host "[SKIP] $name (PID $pid) 已不存在" -ForegroundColor Yellow
        }
        Remove-Item $pidFile -Force
    } else {
        Write-Host "[SKIP] $name 未找到 PID 文件" -ForegroundColor Yellow
    }
}

Stop-ByPid (Join-Path $logDir "backend.pid") "后端 API Gateway"
Stop-ByPid (Join-Path $logDir "frontend.pid") "前端静态服务"

# 兜底：清理残留的 uvicorn / vite preview 进程（仅限本项目端口）
Get-NetTCPConnection -LocalPort 8000, 8080 -State Listen -ErrorAction SilentlyContinue |
    ForEach-Object {
        $proc = Get-Process -Id $_.OwningProcess -ErrorAction SilentlyContinue
        if ($proc) {
            Stop-Process -Id $proc.Id -Force
            Write-Host "[OK] 清理残留进程: $($proc.ProcessName) (PID $($proc.Id))" -ForegroundColor Green
        }
    }

Write-Host "`nKlineQuant 已全部停止" -ForegroundColor Cyan
