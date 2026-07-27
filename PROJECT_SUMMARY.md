# API Pool 项目总结

更新时间：2026-07-04

## 项目定位

API Pool 是一个本地运行的轻量 API 聚合网关和管理面板。当前部署在本机：

- 项目目录：`F:\服务\api-pool`
- 管理面板：`http://127.0.0.1:5200/login`
- 客户端 Base URL：`http://127.0.0.1:5200/v1`
- 运行方式：Python 单文件服务，核心文件为 `api_pool_server.py`
- 数据存储：SQLite 和 JSON 本地文件

## 当前状态

- 服务已部署到 `F:\服务\api-pool`。
- 服务当前以无窗口后台方式运行，监听 `5200` 端口。
- 当前配置了 4 个上游端点。
- 管理面板已加入账号密码登录。
- 客户端调用 `/v1/chat/completions` 已加入固定 API Key 校验。
- 敏感配置保存在本地 `security_config.json` 和 `api_config.json`，不要提交或公开。

## 主要文件

- `api_pool_server.py`：主服务、管理面板、API 网关、认证、模型获取、流式兼容逻辑。
- `api_config.json`：上游端点配置，包含上游 API Key。
- `security_config.json`：管理员账号密码哈希、客户端 API Key 哈希。
- `token_stats.db`：Token 用量统计。
- `chat_logs.db`：对话审计日志。
- `logs/`：运行日志目录。
- `start_service.ps1` / `stop_service.ps1` / `status_service.ps1`：辅助服务脚本。
- `启动服务.bat`：双击启动入口。

## 已完成改动

0. 合并远端分支的调度增强
   - 增加端点 `in_pool`、`billing_mode` 元数据，默认保持现有端点继续参与聚合池。
   - 增加 `/api/pool/<id>` 和 `/api/switch-endpoint/<id>` 后端接口，支持池内/池外管理和手动指定当前端点。
   - 增加冷却过期探活恢复、瞬态故障探活重试、成功后健康状态回写。
   - 非流式 `/v1/chat/completions` 保留上游完整响应体，避免丢失真实 `model`、`usage`、`reasoning_content`。
   - 对 DeepSeek thinking 类多轮对话补全 `reasoning_content`，减少跨端点切换时的兼容问题。
   - 保留本地登录、客户端 API Key 校验、Windows 启动脚本和流式兼容关闭连接策略。

1. 增加管理面板登录
   - 未登录不能访问管理页面和 `/api/*` 管理接口。
   - 首次启动自动生成管理员账号、临时密码和客户端 API Key。
   - 支持在“安全设置”里修改管理员账号/密码。

2. 增加客户端 API Key 校验
   - `/v1/chat/completions` 和 `/chat/completions` 必须携带 `Authorization: Bearer <客户端 API Key>`。
   - 客户端 API Key 可手动设置或重新生成。
   - 只保存 Key 哈希，不保存明文客户端 Key。

3. 改进模型获取
   - 自动兼容常见 Base URL 写法：
     - `https://host/v1`
     - `host/v1`
     - `https://host/v1/models`
     - `https://host/v1/chat/completions`
   - 获取模型成功或失败会写入系统日志。
   - 前端错误提示更明确。
   - “API Key”文案改为“上游 API Key”，避免和 API Pool 客户端 Key 混淆。

4. 修复流式响应问题
   - 修复 `stream:true` 时客户端报 `error decoding response body` / 流读取错误的问题。
   - 当前策略：本地生成兼容 OpenAI SSE 的流式响应，末尾固定发送 `data: [DONE]`。
   - 流式响应头使用 `Connection: close`，避免客户端一直等待。

5. 缓存与运行稳定性
   - 页面和 JSON 响应增加 `Cache-Control: no-store`，减少浏览器加载旧页面的问题。
   - 增加服务启动、停止、状态脚本。

6. 顶部客户端 API Key 操作按钮
   - 在顶部“客户端接入配置”的 API Key 旁增加“获取”和“复制”按钮。
   - 按钮采用小尺寸半透明样式，与现有深色玻璃风格保持一致。
   - “获取”可显示完整客户端 API Key。
   - “复制”可复制完整客户端 API Key。
   - 如果旧配置无法读取明文 Key，会提示先生成新的 API Key。

## 已验证

- `python -m py_compile api_pool_server.py` 语法检查通过。
- 使用临时目录导入模块，验证端点池、手动切换、响应保真、`reasoning_content` 补全和兼容 SSE 生成逻辑通过。
- `/api/auth/status` 可正常响应。
- 登录页可打开。
- 未登录访问管理接口会被拦截。
- 无客户端 API Key 调用模型接口会被拦截。
- 非流式 `/v1/chat/completions` 可正常返回 OpenAI 风格 JSON。
- 流式 `/v1/chat/completions` 可正常返回 SSE，并以 `data: [DONE]` 结束。
- 当前上游模型获取可返回 5 个模型。

## 注意事项

- 本项目和后续测试项目统一使用测试管理账号：`admin`，密码：`45612300`。这是测试约定，不再生成复杂随机测试密码。
- 不要在总结、聊天记录或 README 中写入完整客户端 API Key、完整上游 API Key。
- `api_config.json` 和 `security_config.json` 已在 `.gitignore` 中排除。
- 如果服务短暂启动后退出，优先查看：
  - `logs/api-pool.err.log`
  - `logs/api-pool.out.log`
  - 5200 端口是否被占用
- Windows 下后台启动可能受当前运行环境影响；必要时可直接在项目目录前台运行：

```powershell
python -u api_pool_server.py
```

## 后续维护规则

后续每次修改功能、修 bug、调整部署方式或发现重要问题，都要同步更新本文件的“更新记录”和相关章节。

## 更新记录

### 2026-07-04

- 部署项目到 `F:\服务\api-pool`。
- 增加管理面板登录和客户端 API Key 校验。
- 修复模型获取 URL 兼容问题。
- 修复浏览器旧页面缓存问题。
- 修复 `stream:true` 流式响应读取错误。
- 新增本项目总结文件，作为后续维护记录入口。
- 在 API Key 旁增加“获取”和“复制”按钮，并优化按钮间距与视觉样式。
- 按需重置管理员登录密码，并以无窗口后台方式启动服务。
- 按用户要求，将后续所有测试项目的默认登录约定统一为 `admin / 45612300`，并验证本项目登录成功。

### 2026-07-05

- 将远端分支中较成熟的调度能力合并到本地：池内/池外标记、手动切换端点、冷却恢复探活、瞬态故障处理。
- 调整聊天响应处理，非流式接口保留上游完整响应体，兼容 DeepSeek/Hermes 对真实模型名和 `reasoning_content` 的识别。
- 保留本地安全增强：管理面板登录、客户端 API Key 校验、Windows 无窗口启动配置。
- 已通过语法检查和无真实上游依赖的本地行为验证。

### 2026-07-10

- 新增“锁定模型”功能，解决对外聚合时模型随端点轮转而随机切换的问题。
  - 根因：`/v1/chat/completions` 丢弃客户端请求里的 `model` 字段，实际用哪个模型由当前轮转到的端点决定，而端点在失败/冷却/成功后会切换到绑定其他模型的端点，导致对外模型漂移。
  - 方案：锁定单一真实模型，同名模型的多个上游之间仍自动故障转移，绝不切换到其他模型。
- 后端改动（`api_pool_server.py`）：
  - `APIPool` 新增 `locked_model` 属性；`load_locked_model()` / `save_config(..., locked_model=)` 读写 `api_config.json` 顶层 `locked_model`，重启不丢失；`_sync_to_config()` 同步保存该字段。
  - `_active_endpoints()` 在锁定时仅保留 `model == locked_model` 的端点，故障转移天然限定在同一模型内；无可用上游时按原逻辑报“没有可用的 API 端点”，不静默换模型。
  - 非流式、流式、`/v1/responses` 响应的 `model` 字段在锁定时统一强制为锁定模型；`/v1/models` 锁定时仅返回该模型，便于外部应用自动选中。
  - 新增带鉴权管理接口 `GET/POST /api/locked-model`（读取可用模型/当前锁定、设置或清除并持久化）。
- 前端改动：管理面板顶部新增 “🔒 锁定模型” 卡片，下拉选择（标注未启用/池外）、应用、解除；页面加载时 `loadLockedModel()`。
- 已验证：`py_compile` 通过；重启 5200 服务后完整流程通过——登录、拉取可用模型、锁定后 `/v1/models` 仅剩该模型、`api_config.json` 持久化、解除后恢复未锁定。
- 当前状态：功能上线并保持“未锁定”，待用户在面板自行选择目标模型。注意目前仅 `openai/gpt-oss-120b` 端点为启用状态，锁定其他模型前需先启用对应端点。

### 2026-07-11

- 新增请求级模型路由：`/v1/chat/completions` 与 `/v1/responses` 会将客户端传入的 `model` 交给调度器，只在同名、启用且池内的端点间故障转移。
- 同模型的所有候选端点均失败时，接口返回 `503 model_unavailable`；模型未配置时返回 `404 model_not_found`，不会降级到其他模型。
- 全局锁定仍优先于请求模型：锁定与请求不一致时返回 `400 model_locked`，避免静默返回错误模型。
- 已通过语法检查和隔离行为验证，覆盖同模型成功、首端点失败后同模型接管、全部同模型失败、未知模型和锁定冲突。
