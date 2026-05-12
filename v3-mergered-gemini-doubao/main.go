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

// --- 配置与结构体 ---

const (
	defaultRefreshInterval = 110 * time.Minute
	retryInterval          = 10 * time.Second // 失败后重试间隔
	defaultPort            = "3000"
	wxTokenURL             = "https://api.weixin.qq.com/cgi-bin/token?grant_type=client_credential&appid=%s&secret=%s"
)

type WxAccessTokenResp struct {
	AccessToken string `json:"access_token"`
	ExpiresIn   int    `json:"expires_in"`
	ErrCode     int    `json:"errcode"`
	ErrMsg      string `json:"errmsg"`
}

type AccessTokenService struct {
	appID     string
	appSecret string

	// 缓存数据（读写锁保护）
	mu           sync.RWMutex
	currentToken string
	lastRunTime  time.Time
	lastError    string
	totalRuns    int64
	nextRunTime  time.Time

	timer  *time.Timer
	ctx    context.Context
	cancel context.CancelFunc
	logger *slog.Logger
}

func NewAccessTokenService(appID, appSecret string) *AccessTokenService {
	ctx, cancel := context.WithCancel(context.Background())
	return &AccessTokenService{
		appID:     appID,
		appSecret: appSecret,
		ctx:       ctx,
		cancel:    cancel,
		logger:    slog.New(slog.NewJSONHandler(os.Stdout, nil)),
	}
}

// --- 核心业务逻辑 ---

// refreshToken 执行实际的网络请求，注意：此函数内部不加锁，确保不阻塞其他读取请求
func (s *AccessTokenService) doRefresh() (string, error) {
	client := &http.Client{Timeout: 15 * time.Second}
	url := fmt.Sprintf(wxTokenURL, s.appID, s.appSecret)

	resp, err := client.Get(url)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	var wxResp WxAccessTokenResp
	if err := json.NewDecoder(resp.Body).Decode(&wxResp); err != nil {
		return "", err
	}

	if wxResp.ErrCode != 0 {
		return "", fmt.Errorf("wx_err_%d: %s", wxResp.ErrCode, wxResp.ErrMsg)
	}

	return wxResp.AccessToken, nil
}

// Execute 统一的刷新入口
func (s *AccessTokenService) Execute() {
	token, err := s.doRefresh()

	s.mu.Lock()
	defer s.mu.Unlock()

	s.totalRuns++
	s.lastRunTime = time.Now()

	if err != nil {
		s.lastError = err.Error()
		s.logger.Error("Token刷新失败", "error", err)
		// 失败后立即安排一次短时间的重试
		s.safeResetTimer(retryInterval)
		return
	}

	s.currentToken = token
	s.lastError = ""
	s.logger.Info("Token刷新成功")
	// 成功后安排正常的长周期刷新
	s.safeResetTimer(defaultRefreshInterval)
}

// safeResetTimer 安全重置定时器，防止 Channel 阻塞或竞态
func (s *AccessTokenService) safeResetTimer(d time.Duration) {
	if s.timer == nil {
		s.timer = time.NewTimer(d)
	} else {
		if !s.timer.Stop() {
			select {
			case <-s.timer.C:
			default:
			}
		}
		s.timer.Reset(d)
	}
	s.nextRunTime = time.Now().Add(d)
}

func (s *AccessTokenService) Start() {
	// 初始执行
	s.Execute()

	go func() {
		defer func() {
			if r := recover(); r != nil {
				s.logger.Error("定时协程崩溃重启", "panic", r, "stack", string(debug.Stack()))
				time.Sleep(time.Second)
				go s.Start()
			}
		}()

		for {
			select {
			case <-s.ctx.Done():
				s.logger.Info("定时刷新协程退出")
				return
			case <-s.timer.C:
				s.Execute()
			}
		}
	}()
}

// --- HTTP 处理函数 ---

func (s *AccessTokenService) GetToken(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	json.NewEncoder(w).Encode(map[string]string{"access_token": s.currentToken})
}

func (s *AccessTokenService) ForceRefresh(w http.ResponseWriter, r *http.Request) {
	s.Execute() // 这里会触发加锁并重置定时器
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"message": "force refresh triggered"})
}

func (s *AccessTokenService) Status(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	json.NewEncoder(w).Encode(map[string]any{
		"last_run":     s.lastRunTime.Format(time.RFC3339),
		"next_run":     s.nextRunTime.Format(time.RFC3339),
		"total_runs":   s.totalRuns,
		"last_error":   s.lastError,
		"token_cached": s.currentToken != "",
	})
}

// --- 主入口 ---

func main() {
	appID, appSecret := os.Getenv("WX_APP_ID"), os.Getenv("WX_APP_SECRET")
	if appID == "" || appSecret == "" {
		slog.Error("缺少环境变量 WX_APP_ID 或 WX_APP_SECRET")
		os.Exit(1)
	}

	svc := NewAccessTokenService(appID, appSecret)
	svc.Start()

	mux := http.NewServeMux()
	mux.HandleFunc("GET /", svc.GetToken)
	mux.HandleFunc("GET /refresh", svc.ForceRefresh)
	mux.HandleFunc("GET /status", svc.Status)

	server := &http.Server{
		Addr:    ":" + defaultPort,
		Handler: mux,
	}

	// 优雅关机
	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
		<-sig
		slog.Info("正在关闭服务...")
		svc.cancel()

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := server.Shutdown(ctx); err != nil {
			slog.Error("服务关闭异常", "error", err)
		}
	}()

	slog.Info("服务启动", "port", defaultPort)
	if err := server.ListenAndServe(); err != http.ErrServerClosed {
		slog.Error("服务启动失败", "error", err)
	}
}
