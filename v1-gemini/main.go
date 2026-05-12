package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"runtime/debug"
	"sync"
	"syscall"
	"time"
)

// --- 配置管理 ---

type Config struct {
	AppID     string
	AppSecret string
	Port      string
}

func loadConfig() Config {
	port := os.Getenv("PORT")
	if port == "" {
		port = "3000" // 默认端口
	}
	return Config{
		AppID:     os.Getenv("WX_APP_ID"),
		AppSecret: os.Getenv("WX_APP_SECRET"),
		Port:      port,
	}
}

// --- 数据结构 ---

type WxTokenResponse struct {
	AccessToken string `json:"access_token"`
	ExpiresIn   int    `json:"expires_in"`
	ErrCode     int    `json:"errcode"`
	ErrMsg      string `json:"errmsg"`
}

type TokenManager struct {
	mu           sync.RWMutex
	accessToken  string
	lastRunTime  time.Time
	lastError    string
	totalRuns    int
	nextRunTime  time.Time
	refreshChan  chan struct{}
	config       Config
	interval     time.Duration
}

// --- 业务逻辑 ---

func (tm *TokenManager) fetchToken() error {
	if tm.config.AppID == "" || tm.config.AppSecret == "" {
		return errors.New("missing APP_ID or APP_SECRET environment variables")
	}

	tm.mu.Lock()
	tm.totalRuns++
	tm.mu.Unlock()

	url := fmt.Sprintf("https://api.weixin.qq.com/cgi-bin/token?grant_type=client_credential&appid=%s&secret=%s", 
		tm.config.AppID, tm.config.AppSecret)
	
	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	var res WxTokenResponse
	if err := json.Unmarshal(body, &res); err != nil {
		return err
	}

	if res.ErrCode != 0 {
		return fmt.Errorf("wx_error(code:%d, msg:%s)", res.ErrCode, res.ErrMsg)
	}

	tm.mu.Lock()
	defer tm.mu.Unlock()
	tm.accessToken = res.AccessToken
	tm.lastRunTime = time.Now()
	tm.lastError = ""
	tm.nextRunTime = time.Now().Add(tm.interval)

	return nil
}

func (tm *TokenManager) safeExecute() {
	defer func() {
		if r := recover(); r != nil {
			slog.Error("Task Panic Recovered", "error", r, "stack", string(debug.Stack()))
			tm.mu.Lock()
			tm.lastError = fmt.Sprintf("Panic: %v", r)
			tm.mu.Unlock()
		}
	}()

	if err := tm.fetchToken(); err != nil {
		slog.Warn("Fetch Token Failed", "error", err)
		tm.mu.Lock()
		tm.lastError = err.Error()
		tm.mu.Unlock()
	} else {
		slog.Info("Fetch Token Success", "time", time.Now().Format(time.RFC3339))
	}
}

func (tm *TokenManager) StartWorker(ctx context.Context) {
	tm.safeExecute()

	timer := time.NewTimer(tm.interval)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			tm.safeExecute()
			timer.Reset(tm.interval)
		case <-tm.refreshChan:
			slog.Info("Forced Refresh Triggered via API")
			tm.safeExecute()
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(tm.interval)
		}
	}
}

// --- 主程序 ---

func main() {
	// 初始化结构化日志
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	cfg := loadConfig()
	tm := &TokenManager{
		config:      cfg,
		interval:    110 * time.Minute, // 提前 10 分钟刷新
		refreshChan: make(chan struct{}, 1),
	}

	ctx, cancel := context.WithCancel(context.Background())
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	mux := http.NewServeMux()
	
	// API: 获取当前 Token
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		tm.mu.RLock()
		token := tm.accessToken
		tm.mu.RUnlock()
		
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"access_token": token})
	})

	// API: 强制刷新
	mux.HandleFunc("/refresh", func(w http.ResponseWriter, r *http.Request) {
		select {
		case tm.refreshChan <- struct{}{}:
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(`{"status":"refresh_request_queued"}`))
		default:
			w.WriteHeader(http.StatusTooManyRequests)
			w.Write([]byte(`{"status":"error","message":"refresh already in progress"}`))
		}
	})

	// API: 状态监控
	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		tm.mu.RLock()
		defer tm.mu.RUnlock()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"last_run_time": tm.lastRunTime.Format(time.RFC3339),
			"last_error":    tm.lastError,
			"total_runs":    tm.totalRuns,
			"next_run_time": tm.nextRunTime.Format(time.RFC3339),
			"app_id_set":    tm.config.AppID != "",
		})
	})

	addr := "0.0.0.0:" + cfg.Port
	srv := &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	// 启动 Worker
	go tm.StartWorker(ctx)

	// 启动 Server
	go func() {
		slog.Info("Web Server Started", "listen", addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("HTTP Server Error", "error", err)
			os.Exit(1)
		}
	}()

	// 优雅关机
	<-sigChan
	slog.Info("Shutting down gracefully...")
	
	cancel()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		slog.Error("HTTP Shutdown Error", "error", err)
	}

	slog.Info("Service exited")
}
