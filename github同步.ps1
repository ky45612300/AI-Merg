# ============================================================
# GitHub 同步脚本 - 白名单模式，只同步主要文件
# 用法: powershell -ExecutionPolicy Bypass -File github同步.ps1
# 说明: 含密钥的配置文件、数据库、日志等一律不同步
# ============================================================
$ErrorActionPreference = "Stop"
Set-Location -LiteralPath $PSScriptRoot

# ---- 主要同步文件（白名单，只保留核心）----
$MainFiles = @(
    "api_pool_server.py",
    "api_config.example.json",
    "README.md",
    "LICENSE",
    ".gitignore",
    ".gitattributes",
    "github同步.ps1",
    "start_service.ps1",
    "stop_service.ps1",
    "restart_service.ps1",
    "status_service.ps1",
    "start_api_pool.bat",
    "stop_api_pool.bat",
    "restart_api_pool.bat"
)
$MainDirs = @("tests")

# ---- 不应出现在仓库里的文件/目录（从 git 移除但保留本地）----
$Untrack = @(
    ".backups", ".claude", "work", "logs", "assets",
    "api-pool.pid", "api_config.json.bak", "api_pool.db",
    "chat_logs.db-shm", "chat_logs.db-wal",
    "token_stats.db-shm", "token_stats.db-wal",
    "claude启动.bat", "一键重启.bat", "继续Claude会话.bat",
    "start_api_pool.vbs",
    "优化方案.md", "模型分组功能说明.md",
    "启动健康检测.py", "数据库维护工具.py", "配置去重分析.py"
)

Write-Host "==> [1/3] 清理误跟踪文件..." -ForegroundColor Cyan
$removed = 0
foreach ($u in $Untrack) {
    $tracked = git ls-files -- "$u"
    if ($tracked) {
        git rm --cached -r --quiet -- "$u" 2>$null
        $removed++
    }
}
if ($removed -eq 0) { Write-Host "    无需清理" } else { Write-Host "    已移除 $removed 项（本地文件保留）" }

Write-Host "==> [2/3] 暂存主要文件..." -ForegroundColor Cyan
foreach ($f in $MainFiles) {
    if (Test-Path -LiteralPath $f) { git add -- "$f" | Out-Null }
}
foreach ($d in $MainDirs) {
    if (Test-Path -LiteralPath $d) { git add -- "$d" | Out-Null }
}

# 有变更吗？（新增/修改/删除都算）
$diff = git diff --cached --name-status HEAD
if ([string]::IsNullOrWhiteSpace($diff)) {
    Write-Host "==> 没有需要同步的变更，仓库已是最新。" -ForegroundColor Yellow
    exit 0
}

$msg = "同步主要文件 $(Get-Date -Format 'yyyy-MM-dd HH:mm')"
git commit -m $msg | Out-Null
Write-Host "==> [3/3] 推送到 GitHub..." -ForegroundColor Cyan
git push origin HEAD
if ($LASTEXITCODE -ne 0) {
    Write-Host "推送失败，请检查网络或 GitHub 凭据。" -ForegroundColor Red
    exit 1
}
Write-Host "✅ 同步完成: $msg" -ForegroundColor Green
