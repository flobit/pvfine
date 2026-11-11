package script

import _ "embed"

// APIDeclaration embeds script-api.d.ts for script help discovery across desktop and MCP binaries.
//
//go:embed script-api.d.ts
var APIDeclaration string
