package script

import (
	"os"
	"path/filepath"
	"testing"
)

func TestAPIDeclarationMatchesFrontend(t *testing.T) {
	frontendPath := filepath.Join("..", "..", "frontend", "src", "script-api.d.ts")
	raw, err := os.ReadFile(frontendPath)
	if err != nil {
		t.Fatalf("read frontend script-api.d.ts: %v", err)
	}
	if APIDeclaration == "" {
		t.Fatal("APIDeclaration is empty")
	}
	if APIDeclaration != string(raw) {
		t.Fatalf("internal/script/script-api.d.ts is out of sync with %s; copy frontend/src/script-api.d.ts to internal/script/script-api.d.ts", frontendPath)
	}
}
