package services

import (
	"context"
	"path/filepath"

	"pvfine/internal/mcpserver"
	scriptengine "pvfine/internal/script"
)

// StandaloneMCPOptions configures a headless MCP host without a GUI runtime.
type StandaloneMCPOptions struct {
	ConfigDir string
	CacheDir  string
}

// StandaloneMCPHost owns the service graph required to run pvfine MCP tools
// without Wails. Callers must call Shutdown on exit to flush and release state.
type StandaloneMCPHost struct {
	Core     *core
	Settings *SettingsService
	Archive  *ArchiveService
	Editor   *EditorService
	Script   *ScriptService
	Export   *ExportService
	Cache    *CacheService
	MCP      *MCPService
}

// NewStandaloneMCPHost constructs and starts background cache maintenance for
// headless MCP execution.
func NewStandaloneMCPHost(ctx context.Context, opts StandaloneMCPOptions) (*StandaloneMCPHost, error) {
	settings := NewSettingsService()
	if opts.ConfigDir != "" {
		settings = newSettingsService(filepath.Join(opts.ConfigDir, "settings.json"))
	}

	c := NewCore(settings)
	_ = NewAutosaveService(c, settings)

	var cache *CacheService
	if opts.CacheDir != "" {
		cache = newCacheServiceWithRoot(c, settings, opts.CacheDir)
	} else {
		cache = NewCacheService(c, settings)
	}

	fileSets := NewFileSetService()
	if opts.ConfigDir != "" {
		fileSets = newFileSetService(filepath.Join(opts.ConfigDir, "filesets.json"))
	}

	archive := NewArchiveService(c)
	editor := NewEditorService(c, settings)

	var script *ScriptService
	if opts.ConfigDir != "" {
		script = newScriptService(c, scriptengine.NewGojaRuntime(), filepath.Join(opts.ConfigDir, "scripts"), fileSets)
	} else {
		script = NewScriptService(c, fileSets)
	}

	export := NewExportService(c)
	_ = NewArchiveDiffService(c)

	var mcpSvc *MCPService
	if opts.ConfigDir != "" {
		mcpSvc = newMCPService(c, archive, editor, script, export, filepath.Join(opts.ConfigDir, "mcp.json"))
	} else {
		mcpSvc = NewMCPService(c, archive, editor, script, export, scriptengine.APIDeclaration)
	}

	if err := cache.StartMaintenance(ctx); err != nil {
		return nil, err
	}

	return &StandaloneMCPHost{
		Core:     c,
		Settings: settings,
		Archive:  archive,
		Editor:   editor,
		Script:   script,
		Export:   export,
		Cache:    cache,
		MCP:      mcpSvc,
	}, nil
}

// Open opens a PVF archive so MCP tools can inspect or mutate it immediately.
func (h *StandaloneMCPHost) Open(path string) (ArchiveInfo, error) {
	return h.Archive.Open(path)
}

// Tools returns all registered MCP tool definitions.
func (h *StandaloneMCPHost) Tools() []mcpserver.Tool {
	return h.MCP.Tools()
}

// ServeStdio serves MCP tools over stdin/stdout until EOF or ctx cancellation.
func (h *StandaloneMCPHost) ServeStdio(ctx context.Context) error {
	return mcpserver.ServeStdio(ctx, h.MCP.Tools())
}

// ServeHTTP starts the loopback Streamable HTTP server without modifying mcp.json.
func (h *StandaloneMCPHost) ServeHTTP(addr string) (string, error) {
	return h.MCP.ServeHTTPAddr(addr)
}

// Shutdown cleanly stops MCP tasks/servers, closes any open archive, and stops
// background cache maintenance.
func (h *StandaloneMCPHost) Shutdown() error {
	if h == nil {
		return nil
	}
	var err error
	if h.MCP != nil {
		err = h.MCP.ServiceShutdown()
	}
	if h.Archive != nil {
		h.Archive.Close()
	}
	if h.Cache != nil {
		if shutdownErr := h.Cache.ServiceShutdown(); err == nil {
			err = shutdownErr
		}
	}
	return err
}
