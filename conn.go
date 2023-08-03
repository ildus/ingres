package main

import (
    "database/sql"
	"database/sql/driver"
	"github.com/ildus/ingo/ingres"
    "log"
)

// Compile time validation that our types implement the expected interfaces
var (
	_ driver.Driver = Driver{}
    env *ingres.OpenAPIEnv
)

// Driver is the Ingres database driver.
type Driver struct{}

func init() {
    var err error
	env, err = ingres.InitOpenAPI()
    if err != nil {
        log.Fatalf("could not initialize OpenAPI: %v", err)
    }

    d := &Driver{}
	sql.Register("ingres", d)
	sql.Register("vector", d)
}

// Open opens a new connection to the database. name is a connection string.
// Most users should only use it through database/sql package from the standard
// library.
func (d Driver) Open(name string) (driver.Conn, error) {
    var params ingres.ConnParams
    params.DbName = name
	return env.Connect(params)
}
