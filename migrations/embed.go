// Package migrations holds the SQL schema, applied in file-name order.
package migrations

import "embed"

//go:embed *.sql
var FS embed.FS
