package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
	"unicode/utf16"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"pvfine/internal/mcpserver"
	"pvfine/internal/pvf"
	scriptengine "pvfine/internal/script"
	"pvfine/services"
)

func TestParseFlagsValidation(t *testing.T) {
	cfg, err := parseFlags(nil)
	if err != nil {
		t.Fatalf("default flags failed: %v", err)
	}
	if cfg.transport != "stdio" || cfg.port != defaultHTTPPort {
		t.Fatalf("unexpected defaults: %+v", cfg)
	}

	cfg, err = parseFlags([]string{"--transport", "HTTP", "--port", "0", "--pvf", "/tmp/test.pvf"})
	if err != nil {
		t.Fatalf("valid http flags failed: %v", err)
	}
	if cfg.transport != "http" || cfg.port != 0 || cfg.pvfPath != "/tmp/test.pvf" {
		t.Fatalf("unexpected parsed config: %+v", cfg)
	}

	for _, bad := range [][]string{
		{"--transport", "grpc"},
		{"--port", "70000"},
		{"extra-arg"},
	} {
		if _, err := parseFlags(bad); err == nil {
			t.Fatalf("expected error for invalid args %v", bad)
		}
	}
}

func writeSamplePVF(t *testing.T) string {
	t.Helper()
	archive := pvf.New()
	text := ">test_key\t测试值\r\n"
	units := utf16.Encode([]rune(text))
	raw := make([]byte, len(units)*2)
	for i, u := range units {
		binary.LittleEndian.PutUint16(raw[i*2:], u)
	}
	archive.AddFile("etc/sample.str", raw, pvf.TypeUnicode)
	archive.AddFile("etc/plain.txt", []byte("hello pvf"), 0)

	pvfPath := filepath.Join(t.TempDir(), "sample.pvf")
	f, err := os.Create(pvfPath)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if err := archive.SaveTo(f); err != nil {
		t.Fatalf("SaveTo failed: %v", err)
	}
	return pvfPath
}

func newIsolatedHost(t *testing.T) (*services.StandaloneMCPHost, string) {
	t.Helper()
	tmp := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(tmp, "xdg-config"))
	t.Setenv("XDG_CACHE_HOME", filepath.Join(tmp, "xdg-cache"))
	t.Setenv("APPDATA", filepath.Join(tmp, "appdata"))
	t.Setenv("LOCALAPPDATA", filepath.Join(tmp, "localappdata"))

	cfgDir := filepath.Join(tmp, "cfg")
	cacheDir := filepath.Join(tmp, "cache")
	host, err := services.NewStandaloneMCPHost(context.Background(), services.StandaloneMCPOptions{
		ConfigDir: cfgDir,
		CacheDir:  cacheDir,
	})
	if err != nil {
		t.Fatalf("NewStandaloneMCPHost failed: %v", err)
	}
	t.Cleanup(func() {
		if err := host.Shutdown(); err != nil {
			t.Errorf("host.Shutdown failed: %v", err)
		}
	})
	return host, cfgDir
}

func callToolJSON(t *testing.T, session *mcp.ClientSession, name string, args map[string]any) map[string]any {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	res, err := session.CallTool(ctx, &mcp.CallToolParams{Name: name, Arguments: args})
	if err != nil {
		t.Fatalf("CallTool(%s) RPC failed: %v", name, err)
	}
	if res.IsError {
		var msg string
		if len(res.Content) > 0 {
			if tc, ok := res.Content[0].(*mcp.TextContent); ok {
				msg = tc.Text
			}
		}
		t.Fatalf("CallTool(%s) returned error: %s", name, msg)
	}
	if len(res.Content) == 0 {
		t.Fatalf("CallTool(%s) returned empty content", name)
	}
	tc, ok := res.Content[0].(*mcp.TextContent)
	if !ok {
		t.Fatalf("CallTool(%s) unexpected content type %T", name, res.Content[0])
	}
	var out map[string]any
	if err := json.Unmarshal([]byte(tc.Text), &out); err != nil {
		t.Fatalf("CallTool(%s) invalid JSON %q: %v", name, tc.Text, err)
	}
	return out
}

func TestHeadlessHTTPServerAndEmbeddedTypes(t *testing.T) {
	pvfPath := writeSamplePVF(t)
	host, cfgDir := newIsolatedHost(t)

	endpoint, err := host.ServeHTTP("127.0.0.1:0")
	if err != nil {
		t.Fatalf("ServeHTTP failed: %v", err)
	}
	if !strings.HasPrefix(endpoint, "http://127.0.0.1:") || !strings.HasSuffix(endpoint, "/mcp") {
		t.Fatalf("unexpected endpoint: %q", endpoint)
	}

	// Ensure standalone HTTP startup never writes desktop's mcp.json config file.
	if _, err := os.Stat(filepath.Join(cfgDir, "mcp.json")); !os.IsNotExist(err) {
		t.Fatalf("ServeHTTP unexpectedly created mcp.json: %v", err)
	}

	ctx := context.Background()
	client := mcp.NewClient(&mcp.Implementation{Name: "test-http", Version: "1"}, nil)
	session, err := client.Connect(ctx, &mcp.StreamableClientTransport{Endpoint: endpoint}, nil)
	if err != nil {
		t.Fatalf("HTTP connect failed: %v", err)
	}
	defer session.Close()

	tools, err := session.ListTools(ctx, nil)
	if err != nil || len(tools.Tools) == 0 {
		t.Fatalf("ListTools failed: err=%v, tools=%v", err, tools)
	}

	// Verify embedded script-api.d.ts is returned in headless binary builds.
	help := callToolJSON(t, session, "pvf_script_help", map[string]any{"topic": "types"})
	doc, _ := help["declarations"].(string)
	if doc != scriptengine.APIDeclaration {
		t.Fatalf("pvf_script_help(types) declarations mismatch: len=%d want=%d", len(doc), len(scriptengine.APIDeclaration))
	}

	wsBefore := callToolJSON(t, session, "pvf_workspace_info", map[string]any{})
	if open, _ := wsBefore["open"].(bool); open {
		t.Fatalf("expected initially closed workspace, got %+v", wsBefore)
	}

	// Open archive via submit()-backed MCP tool (must NOT return UI_NOT_READY).
	openTask := callToolJSON(t, session, "pvf_archive_open", map[string]any{
		"path":                   pvfPath,
		"expectedWorkspaceEpoch": wsBefore["workspaceEpoch"],
		"requestId":              "req-open-1",
	})
	taskID, _ := openTask["taskId"].(string)
	if taskID == "" {
		t.Fatalf("expected taskId from pvf_archive_open, got %+v", openTask)
	}

	deadline := time.Now().Add(5 * time.Second)
	for {
		status := callToolJSON(t, session, "pvf_task_status", map[string]any{"taskId": taskID})
		state, _ := status["status"].(string)
		if state == "completed" {
			break
		}
		if state == "failed" {
			t.Fatalf("pvf_archive_open task failed: %+v", status)
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for pvf_archive_open: %+v", status)
		}
		time.Sleep(20 * time.Millisecond)
	}

	ws := callToolJSON(t, session, "pvf_workspace_info", map[string]any{})
	if open, _ := ws["open"].(bool); !open {
		t.Fatalf("expected workspace open=true, got %+v", ws)
	}

	fileRes := callToolJSON(t, session, "pvf_read_file", map[string]any{
		"workspaceId":      ws["workspaceId"],
		"expectedRevision": ws["revision"],
		"path":             "etc/sample.str",
	})
	encodedFile, _ := json.Marshal(fileRes)
	if !strings.Contains(string(encodedFile), "测试值") {
		t.Fatalf("unexpected file read response: %s", encodedFile)
	}
}

func TestHeadlessStdioPreloadedArchiveAndScriptDryRun(t *testing.T) {
	pvfPath := writeSamplePVF(t)
	host, _ := newIsolatedHost(t)
	if _, err := host.Open(pvfPath); err != nil {
		t.Fatalf("host.Open failed: %v", err)
	}

	c2sReader, c2sWriter := io.Pipe()
	s2cReader, s2cWriter := io.Pipe()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	serverDone := make(chan error, 1)
	go func() {
		serverDone <- mcpserverRunStdioForTest(ctx, host, c2sReader, s2cWriter)
	}()

	client := mcp.NewClient(&mcp.Implementation{Name: "test-stdio", Version: "1"}, nil)
	session, err := client.Connect(ctx, &mcp.IOTransport{Reader: s2cReader, Writer: c2sWriter}, nil)
	if err != nil {
		t.Fatalf("stdio connect failed: %v", err)
	}

	ws := callToolJSON(t, session, "pvf_workspace_info", map[string]any{})
	if open, _ := ws["open"].(bool); !open {
		t.Fatalf("expected preloaded workspace open=true, got %+v", ws)
	}

	runTask := callToolJSON(t, session, "pvf_script_run", map[string]any{
		"workspaceId":      ws["workspaceId"],
		"expectedRevision": ws["revision"],
		"source":           "for (const p of pvf.glob('etc/**/*')) { pvf.log('info', p.path); }",
		"requestId":        "req-script-1",
	})
	taskID, _ := runTask["taskId"].(string)
	if taskID == "" {
		t.Fatalf("expected taskId from pvf_script_run, got %+v", runTask)
	}

	deadline := time.Now().Add(5 * time.Second)
	for {
		status := callToolJSON(t, session, "pvf_script_status", map[string]any{"taskId": taskID})
		state, _ := status["status"].(string)
		if state == "completed" {
			encoded, _ := json.Marshal(status)
			if !strings.Contains(string(encoded), "etc/sample.str") {
				t.Fatalf("expected script logs to mention etc/sample.str, got: %s", encoded)
			}
			break
		}
		if state == "failed" {
			t.Fatalf("pvf_script_run failed: %+v", status)
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for pvf_script_run: %+v", status)
		}
		time.Sleep(20 * time.Millisecond)
	}

	_ = session.Close()
	cancel()
	<-serverDone
}

func TestRunRejectsMissingPVF(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(tmp, "xdg-config"))
	t.Setenv("XDG_CACHE_HOME", filepath.Join(tmp, "xdg-cache"))
	err := run(context.Background(), cliConfig{
		transport: "http",
		port:      0,
		pvfPath:   filepath.Join(tmp, "does-not-exist.pvf"),
		configDir: filepath.Join(tmp, "cfg"),
		cacheDir:  filepath.Join(tmp, "cache"),
	})
	if err == nil {
		t.Fatal("expected error for missing -pvf file")
	}
}

func mcpserverRunStdioForTest(ctx context.Context, host *services.StandaloneMCPHost, r io.ReadCloser, w io.WriteCloser) error {
	return mcpserver.NewServer(host.Tools()).Run(ctx, &mcp.IOTransport{Reader: r, Writer: w})
}
