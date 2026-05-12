# Go 生产级微信 AccessToken 自动刷新服务：最终需求清单

## 1. 核心业务逻辑
- **功能目标**：根据微信官方文档，定时自动获取并缓存 `access_token`。
- **接口地址**：`GET https://api.weixin.qq.com/cgi-bin/token?grant_type=client_credential&appid=APPID&secret=APPSECRET`。
- **定时策略**：
    - 采用 `time.Timer` 配合手动重置（Reset）模式，实现频率补偿。
    - 刷新周期默认为 110 分钟（略短于官方 2 小时的有效期），确保 Token 永不过期。

## 2. 外部配置与环境
- **环境变量读取**：
    - `WX_APP_ID`: 微信公众号 AppID。
    - `WX_APP_SECRET`: 微信公众号 AppSecret。
    - `PORT`: Web Server 监听端口（若未设置，默认为 `3000`）。
- **网络监听**：Web Server 默认监听地址为 `0.0.0.0`。

## 3. Web API 接口 (JSON 格式)
- **`GET /`**：返回当前内存中缓存的有效 `access_token`。
- **`GET /refresh`**：强制立即执行刷新任务。触发后需重置定时器起点，避免手动刷新后紧接着触发自动刷新。
- **`GET /status`**：返回监控指标，包含：
    - `last_run_time`: 最后一次执行任务的具体时间。
    - `last_error`: 最后一次运行的错误信息（若无则为空字符串）。
    - `total_runs`: 服务启动后的累计运行总次数（包含自动与强制刷新）。
    - `next_run_time`: 预计下一次自动刷新的时间点。

## 4. 高可用与健壮性
- **Panic Recovery**：任务协程必须包含 `defer recover()`，捕获异常并记录堆栈轨迹，防止单次任务崩溃导致整个进程（含 Web Server）退出。
- **并发安全**：使用 `sync.RWMutex` 保护 Token 及状态变量，确保多线程下读写一致性。
- **结构化日志**：使用官方 `log/slog` 库输出 JSON 日志，记录每次请求细节、错误详情及系统关键节点。

## 5. 优雅关机 (Graceful Shutdown)
- **信号处理**：监听 `SIGINT` (Ctrl+C) 和 `SIGTERM` 信号。
- **退出流程**：
    1. 接收到信号后，首先通过 `context.CancelFunc` 通知定时协程停止。
    2. 随后调用 `server.Shutdown()` 并配合超时控制（如 10 秒），优雅关闭 Web 服务，确保资源（连接、Timer）被正确释放。

## 6. 代码质量
- **资源管理**：严格防止 Timer/Ticker 泄漏，确保所有资源在服务停止时显式释放。
- **错误处理**：对微信接口返回的 `errcode` 进行业务级错误判断并记录。
