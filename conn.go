package ingres

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"log"
)

// Compile time validation that our types implement the expected interfaces
var (
	_   driver.Driver = Driver{}
	env *OpenAPIEnv
)

// Driver is the Ingres database driver.
type Driver struct{}

func init() {
	var err error
	env, err = InitOpenAPI()
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
	var params ConnParams
	params.DbName = name
	return env.Connect(params)
}

func (c *OpenAPIConn) QueryContext(ctx context.Context, query string,
	args []driver.NamedValue) (*rows, error) {

	return runQuery(c.handle, nil, query, SELECT)
}

func (c *OpenAPIConn) ExecContext(ctx context.Context, query string,
	args []driver.NamedValue) (*rows, error) {

	rows, err := runQuery(c.handle, c.AutoCommitTransation.handle, query, EXEC)
	if err != nil {
		return nil, err
	}

	rows.fetchInfo()
	err = rows.Close()
	if err != nil {
		return nil, err
	}

	return rows, nil
}

func (c *OpenAPIConn) Prepare(query string) (*stmt, error) {
}
