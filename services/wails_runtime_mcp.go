//go:build mcp_only

package services

func hasLiveWailsRuntime() bool { return false }

func wailsWindowExists(string) bool { return true }

func emitWailsEvent(string, ...any) {}

func subscribeMCPProgressEvents(*MCPService) {}
