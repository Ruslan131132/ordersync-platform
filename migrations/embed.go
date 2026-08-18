// Package migrations embeds the SQL schema so binaries do not depend on the
// working directory at runtime.
package migrations

import "embed"

//go:embed *.sql
var FS embed.FS
