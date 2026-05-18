package store

import (
	"database/sql"
	_ "embed"
	"fmt"
)

// schemaSQL is the full DDL applied on Open(). It is written with
// CREATE TABLE/INDEX IF NOT EXISTS so applying it repeatedly is idempotent.
//
//go:embed schema.sql
var schemaSQL string

// migrate applies the embedded schema. It is safe to call on every Open():
// every statement is IF NOT EXISTS, so an already-initialised database is
// left unchanged.
func migrate(db *sql.DB) error {
	if _, err := db.Exec(schemaSQL); err != nil {
		return fmt.Errorf("apply schema: %w", err)
	}
	return nil
}
