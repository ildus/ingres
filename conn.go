package ingres

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"log"
)

// Compile time validation that our types implement the expected interfaces
var (
	_   driver.Driver = Driver{}
	env *OpenAPIEnv
)

// Driver is the Ingres database driver.
type Driver struct{}

type IngresError struct {
	State     string
	ErrorCode int
	Err       error
}

func newIngresError(state string, code int, err error) *IngresError {
	return &IngresError{
		State:     state,
		ErrorCode: code,
		Err:       err,
	}
}

func (e *IngresError) Error() string {
	return fmt.Sprintf("%v", e.Err)
}

func (e *IngresError) Unwrap() error {
	return e.Err
}

func init() {
	var err error
	env, err = InitOpenAPI()
	if err != nil {
		log.Fatalf("could not initialize OpenAPI: %v", err)
	}

	d := &Driver{}
	sql.Register("ingres", d)
}

func (d Driver) Open(name string) (driver.Conn, error) {
	var params ConnParams
	params.DbName = name
    conn, err := env.Connect(params)
    if err != nil {
        return nil, err
    }
    err = conn.AutoCommit()
    if err != nil {
        conn.Close()
        return nil, err
    }

    return conn, nil
}

func makeStmt(c *OpenAPIConn, query string, queryType QueryType) *stmt {
	return &stmt{
		conn:      c,
		query:     query,
		queryType: queryType,
	}
}

func (c *OpenAPIConn) Query(query string, args []driver.Value) (driver.Rows, error) {

	s := makeStmt(c, query, SELECT)
	return s.Query(args)
}

func (c *OpenAPIConn) Exec(query string, args []driver.Value) (driver.Result, error) {

	s := makeStmt(c, query, EXEC)
	return s.Exec(args)
}

func (c *OpenAPIConn) Prepare(query string) (driver.Stmt, error) {
	return makeStmt(c, query, SELECT), nil
}

func (c *OpenAPIConn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

func (c *OpenAPIConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	return nil, nil
}

func (c *OpenAPIConn) Close() error {
	return disconnect(c)
}

func (s *stmt) Exec(args []driver.Value) (driver.Result, error) {
	s.queryType = EXEC
	rows, err := s.runQuery(s.conn.handle, s.conn.AutoCommitTransation.handle)
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

func (s *stmt) Query(args []driver.Value) (driver.Rows, error) {
	s.queryType = SELECT
	return s.runQuery(s.conn.handle, s.conn.AutoCommitTransation.handle)
}

func (s *stmt) Close() error {
	return nil
}

func (s *stmt) NumInput() int {
	return -1
}
