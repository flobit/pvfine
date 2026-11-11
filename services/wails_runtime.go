//go:build !mcp_only

package services

import (
	"context"

	"github.com/wailsapp/wails/v3/pkg/application"
)

func hasLiveWailsRuntime() bool {
	return application.Get() != nil
}

func wailsWindowExists(name string) bool {
	app := application.Get()
	if app == nil {
		return true
	}
	_, exists := app.Window.GetByName(name)
	return exists
}

func emitWailsEvent(name string, data ...any) {
	if app := application.Get(); app != nil {
		app.Event.Emit(name, data...)
	}
}

func subscribeMCPProgressEvents(s *MCPService) {
	app := application.Get()
	if app == nil {
		return
	}
	for _, name := range []string{"script:progress", "archive:save-progress"} {
		app.Event.On(name, func(event *application.CustomEvent) {
			s.updateRunningTaskProgress(event.Data)
		})
	}
}

// ObserveLogEvents makes startup history available after the frontend subscribes.
func ObserveLogEvents(app *application.App) {
	app.Event.On("app:logs-request", func(*application.CustomEvent) {
		_ = app.Event.Emit("app:logs", applicationLogs.snapshot())
	})
	// application.New logs before its event processor is initialized. Only
	// enable live forwarding after New returns; startup logs stay in history.
	applicationLogs.setEmitter(func(entry appLogEntry) {
		_ = app.Event.Emit("app:log", entry)
	})
}

func (s *MCPService) ServiceStartup(_ context.Context, _ application.ServiceOptions) error {
	return s.Startup()
}

func (s *CacheService) ServiceStartup(ctx context.Context, _ application.ServiceOptions) error {
	return s.StartMaintenance(ctx)
}
