package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"runtime/debug"
	"sync"
	"syscall"
	"time"
)

// 微信接口响应结构体
type WxAccessTokenResp struct {
	AccessToken string `json:"access_token"`
	ExpiresIn   int    `json:"expires_in"`
	ErrCode     int    `json:"errcode"`
	ErrMsg      string `json:"errmsg"`
}

// 常量配置
const (
	defaultRefreshInterval = 110 * time.Minute // 略短于微信2小时有效期
	defaultPort            = "3000"
	wxTokenURL             = "https://api.weixin.qq.com/cgi-bin/token?grant_type=client_credential&appid=%s&secret=%s"
)

// AccessTokenService 核心服务结构体
type AccessTokenService struct {
	appID     string
	appSecret string

	currentToken string // 缓存的有效AccessToken
	mu           sync.RWMutex // 读写锁：保证并发安全

	// 监控状态指标
	lastRunTime time.Time
	lastError   error
	totalRuns   int64
	nextRunTime time.Time

	timer           *time.Timer       // 定时刷新器
	ctx             context.Context   // 服务上下文
	cancel          context.CancelFunc// 关闭上下文函数
	refreshInterval time.Duration     // 刷新间隔
	logger          *slog.Logger      // 结构化日志
}

// NewAccessTokenService 初始化服务
func NewAccessTokenService(appID, appSecret string) *AccessTokenService {
	ctx, cancel := context.WithCancel(context.Background())
	// 初始化JSON格式结构化日志
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	return &AccessTokenService{
		appID:           appID,
		appSecret:       appSecret,
		ctx:             ctx,
		cancel:          cancel,
		refreshInterval: defaultRefreshInterval,
		logger:          logger,
	}
}

// refreshToken 核心：执行一次Token刷新（线程安全）
func (s *AccessTokenService) refreshToken() {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 更新运行状态
	s.totalRuns++
	s.lastRunTime = time.Now()
	s.lastError = nil

	// 调用微信官方接口
	url := fmt.Sprintf(wxTokenURL, s.appID, s.appSecret)
	resp, err := http.Get(url)
	if err != nil {
		s.lastError = err
		s.logger.Error("请求微信AccessToken接口失败", "error", err)
		return
	}
	defer resp.Body.Close()

	// 解析响应数据
	var wxResp WxAccessTokenResp
	if err := json.NewDecoder(resp.Body).Decode(&wxResp); err != nil {
		s.lastError = err
		s.logger.Error("解析微信响应失败", "error", err)
		return
	}

	// 业务级错误处理（微信返回errcode≠0）
	if wxResp.ErrCode != 0 {
		s.lastError = fmt.Errorf("wx_error: errcode=%d, errmsg=%s", wxResp.ErrCode, wxResp.ErrMsg)
		s.logger.Error("微信接口返回业务错误", "errcode", wxResp.ErrCode, "errmsg", wxResp.ErrMsg)
		return
	}

	// 更新缓存Token
	s.currentToken = wxResp.AccessToken
	s.logger.Info("AccessToken刷新成功", "expires_in", wxResp.ExpiresIn)
}

// Start 启动服务：首次立即刷新 + 启动定时协程
func (s *AccessTokenService) Start() {
	s.logger.Info(
		"微信AccessToken服务启动",
		"app_id", s.appID,
		"refresh_interval", s.refreshInterval.Minutes(),
	)

	// 服务启动后立即刷新一次
	s.refreshToken()

	// 初始化定时器
	s.timer = time.NewTimer(s.refreshInterval)
	s.mu.Lock()
	s.nextRunTime = time.Now().Add(s.refreshInterval)
	s.mu.Unlock()

	// 启动定时刷新协程
	go func() {
		// Panic 捕获：防止单次任务崩溃导致整个服务退出
		defer func() {
			if r := recover(); r != nil {
				stack := string(debug.Stack())
				s.logger.Error("定时协程Panic", "panic", r, "stack", stack)
				// 自动重启协程，保证高可用
				go s.Start()
			}
		}()

		for {
			select {
			case <-s.ctx.Done():
				// 收到停止信号，释放定时器资源
				s.timer.Stop()
				s.logger.Info("定时协程已停止")
				return
			case <-s.timer.C:
				// 执行自动刷新
				s.refreshToken()
				// 重置定时器（频率补偿）
				s.timer.Reset(s.refreshInterval)
				// 更新下次运行时间
				s.mu.Lock()
				s.nextRunTime = time.Now().Add(s.refreshInterval)
				s.mu.Unlock()
				s.logger.Info("自动刷新完成", "next_run_time", s.nextRunTime.Format(time.RFC3339))
			}
		}
	}()
}

// -------------------------- Web API 接口 --------------------------
// GetTokenHandler 获取当前缓存的AccessToken
func (s *AccessTokenService) GetTokenHandler(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	token := s.currentToken
	s.mu.RUnlock()

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{"access_token": token})
	s.logger.Info("查询AccessToken", "remote_addr", r.RemoteAddr)
}

// ForceRefreshHandler 强制立即刷新Token
func (s *AccessTokenService) ForceRefreshHandler(w http.ResponseWriter, r *http.Request) {
	// 执行刷新
	s.refreshToken()

	// 重置定时器，避免手动刷新后立即自动刷新
	s.mu.Lock()
	s.timer.Reset(s.refreshInterval)
	s.nextRunTime = time.Now().Add(s.refreshInterval)
	nextRun := s.nextRunTime
	s.mu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"msg":           "强制刷新成功",
		"next_run_time": nextRun.Format(time.RFC3339),
	})
	s.logger.Info("强制刷新AccessToken", "remote_addr", r.RemoteAddr)
}

// StatusHandler 获取服务监控状态
func (s *AccessTokenService) StatusHandler(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	lastErr := ""
	if s.lastError != nil {
		lastErr = s.lastError.Error()
	}

	status := map[string]interface{}{
		"last_run_time": s.lastRunTime.Format(time.RFC3339),
		"last_error":    lastErr,
		"total_runs":    s.totalRuns,
		"next_run_time": s.nextRunTime.Format(time.RFC3339),
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(status)
	s.logger.Info("查询服务状态", "remote_addr", r.RemoteAddr)
}

// -------------------------- 优雅关机 --------------------------
// GracefulShutdown 监听系统信号，优雅关闭服务
func (s *AccessTokenService) GracefulShutdown(server *http.Server) {
	sigChan := make(chan os.Signal, 1)
	// 监听Ctrl+C(SIGINT)和容器停止(SIGTERM)信号
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// 阻塞等待信号
	sig := <-sigChan
	s.logger.Info("接收到退出信号，开始优雅关机", "signal", sig.String())

	// 步骤1：取消上下文，停止定时协程
	s.cancel()

	// 步骤2：停止定时器，防止资源泄漏
	s.timer.Stop()

	// 步骤3：优雅关闭HTTP服务（10秒超时）
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		s.logger.Error("HTTP服务关闭失败", "error", err)
	} else {
		s.logger.Info("HTTP服务已优雅关闭")
	}
}

// -------------------------- 主函数 --------------------------
func main() {
	// 读取环境变量配置
	appID := os.Getenv("WX_APP_ID")
	appSecret := os.Getenv("WX_APP_SECRET")
	port := os.Getenv("PORT")
	if port == "" {
		port = defaultPort
	}

	// 校验必填配置
	if appID == "" || appSecret == "" {
		slog.Error("请配置环境变量 WX_APP_ID 和 WX_APP_SECRET")
		os.Exit(1)
	}

	// 初始化并启动核心服务
	svc := NewAccessTokenService(appID, appSecret)
	svc.Start()

	// 注册路由
	mux := http.NewServeMux()
	mux.HandleFunc("GET /", svc.GetTokenHandler)
	mux.HandleFunc("GET /refresh", svc.ForceRefreshHandler)
	mux.HandleFunc("GET /status", svc.StatusHandler)

	// 初始化HTTP服务
	server := &http.Server{
		Addr:    "0.0.0.0:" + port,
		Handler: mux,
	}

	// 启动优雅关机协程
	go svc.GracefulShutdown(server)

	// 启动HTTP服务
	svc.logger.Info("HTTP服务启动成功", "listen_addr", server.Addr)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		svc.logger.Error("HTTP服务启动失败", "error", err)
		os.Exit(1)
	}

	svc.logger.Info("服务已完全退出")
}
