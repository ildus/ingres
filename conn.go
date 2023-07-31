package main

import (
    "database/sql"
	"database/sql/driver"
)

// Compile time validation that our types implement the expected interfaces
var (
	_ driver.Driver = Driver{}
)

// Driver is the Ingres database driver.
type Driver struct{}

// Open opens a new connection to the database. name is a connection string.
// Most users should only use it through database/sql package from the standard
// library.
func (d Driver) Open(name string) (driver.Conn, error) {
	return Open(name)
}

func init() {
    d := &Driver{}
	sql.Register("ingres", d)
	sql.Register("vector", d)
}
