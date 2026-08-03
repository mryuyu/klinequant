# KlineQuant 发布打包脚本
# 生成 release/klinequant-<version>-paper.zip（排除 .venv/node_modules/密钥/运行时数据）

$ErrorActionPreference = "Stop"
$workspace = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)   # f:\KlineQuantWork
$version = "1.0.0"
$tag = "paper"

$staging = Join-Path $workspace "release\staging\klinequant-$version-$tag"
$zipPath = Join-Path $workspace "release\klinequant-$version-$tag.zip"

Write-Host "[1/4] 清理旧产物..." -ForegroundColor Cyan
if (Test-Path (Join-Path $workspace "release\staging")) {
    Remove-Item (Join-Path $workspace "release\staging") -Recurse -Force
}
New-Item -ItemType Directory -Path $staging -Force | Out-Null

Write-Host "[2/4] 复制后端源码（排除 .venv/缓存/密钥/运行时数据）..." -ForegroundColor Cyan
robocopy (Join-Path $workspace "klinequant") (Join-Path $staging "klinequant") /E `
    /XD .venv __pycache__ .pytest_cache logs data .qoder .git backups `
    /XF .env *.duckdb *.duckdb.wal *.pid | Out-Null

Write-Host "[3/4] 复制前端（排除 node_modules，保留 dist 产物）..." -ForegroundColor Cyan
robocopy (Join-Path $workspace "frontend") (Join-Path $staging "frontend") /E `
    /XD node_modules test-results playwright-report .git `
    /XF *.log | Out-Null

# 根级文档
Copy-Item (Join-Path $workspace "CHANGELOG.md") $staging

Write-Host "[4/4] 压缩归档..." -ForegroundColor Cyan
if (Test-Path $zipPath) { Remove-Item $zipPath -Force }
Compress-Archive -Path (Join-Path $staging "*") -DestinationPath $zipPath -CompressionLevel Optimal

# 清理 staging
Remove-Item (Join-Path $workspace "release\staging") -Recurse -Force

$size = [math]::Round((Get-Item $zipPath).Length / 1MB, 2)
Write-Host @"

============================================
  打包完成！
  产物: $zipPath
  大小: $size MB
============================================
"@ -ForegroundColor Green
