package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"pvfine/services"
)

const defaultHTTPPort = 19245

type cliConfig struct {
	transport string
	port      int
	pvfPath   string
	configDir string
	cacheDir  string
}

func parseFlags(args []string) (cliConfig, error) {
	var cfg cliConfig
	fs := flag.NewFlagSet("pvfine-mcp", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	fs.StringVar(&cfg.transport, "transport", "stdio", "MCP 传输模式: stdio 或 http")
	fs.IntVar(&cfg.port, "port", defaultHTTPPort, "HTTP 模式监听端口 (127.0.0.1，支持 0 自动分配)")
	fs.StringVar(&cfg.pvfPath, "pvf", "", "启动时预打开的 PVF 归档路径")
	fs.StringVar(&cfg.configDir, "config-dir", "", "自定义配置目录（默认使用系统用户配置目录/pvfine）")
	fs.StringVar(&cfg.cacheDir, "cache-dir", "", "自定义缓存目录（默认使用系统用户缓存目录/pvfine）")

	if err := fs.Parse(args); err != nil {
		return cfg, err
	}
	if fs.NArg() > 0 {
		return cfg, fmt.Errorf("未识别的额外参数: %s", strings.Join(fs.Args(), " "))
	}

	cfg.transport = strings.ToLower(strings.TrimSpace(cfg.transport))
	if cfg.transport != "stdio" && cfg.transport != "http" {
		return cfg, fmt.Errorf("不支持的 transport %q，仅支持 stdio 或 http", cfg.transport)
	}
	if cfg.port < 0 || cfg.port > 65535 {
		return cfg, fmt.Errorf("端口 %d 超出有效范围 (0-65535)", cfg.port)
	}
	return cfg, nil
}

func run(ctx context.Context, cfg cliConfig) error {
	host, err := services.NewStandaloneMCPHost(ctx, services.StandaloneMCPOptions{
		ConfigDir: cfg.configDir,
		CacheDir:  cfg.cacheDir,
	})
	if err != nil {
		return fmt.Errorf("初始化 MCP 服务失败: %w", err)
	}
	defer func() {
		if shutdownErr := host.Shutdown(); shutdownErr != nil {
			log.Printf("[WARN] MCP 退出清理失败: %v", shutdownErr)
		}
	}()

	if cfg.pvfPath != "" {
		if _, err := host.Open(cfg.pvfPath); err != nil {
			return fmt.Errorf("打开 PVF 归档失败 (%s): %w", cfg.pvfPath, err)
		}
	}

	switch cfg.transport {
	case "stdio":
		if err := host.ServeStdio(ctx); err != nil && ctx.Err() == nil {
			return fmt.Errorf("MCP stdio 服务异常退出: %w", err)
		}
		return nil
	case "http":
		addr := fmt.Sprintf("127.0.0.1:%d", cfg.port)
		endpoint, err := host.ServeHTTP(addr)
		if err != nil {
			return err
		}
		fmt.Fprintf(os.Stderr, "pvfine-mcp listening on %s\n", endpoint)
		<-ctx.Done()
		return nil
	default:
		return fmt.Errorf("不支持的 transport: %s", cfg.transport)
	}
}

func main() {
	// Always route diagnostic logs to stderr so stdout remains strictly reserved
	// for MCP stdio JSON-RPC frames.
	log.SetOutput(services.CaptureLogs(os.Stderr))

	cfg, err := parseFlags(os.Args[1:])
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			os.Exit(0)
		}
		fmt.Fprintf(os.Stderr, "pvfine-mcp: %v\n", err)
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, cfg); err != nil {
		fmt.Fprintf(os.Stderr, "pvfine-mcp: %v\n", err)
		os.Exit(1)
	}
}
