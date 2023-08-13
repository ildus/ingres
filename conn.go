package ingres

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"log"
	"net/url"
	"strings"
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

	if strings.Contains(name, "?") {
		parts := strings.Split(name, "?")
		if len(parts) != 2 {
			return nil, errors.New("DSN is invalid")
		}

		values, err := url.ParseQuery(parts[1])

		if err != nil {
			return nil, errors.New("parameters parse error")
		}

		if values.Has("username") && !values.Has("password") {
			return nil, errors.New("password has not been specified")
		}

		params.UserName = values.Get("username")
		params.Password = values.Get("password")

		name = parts[0]
	}

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
		args:      nil,
		conn:      c,
		query:     query,
		queryType: queryType,
	}
}

func (c *OpenAPIConn) Query(query string, args []driver.Value) (driver.Rows, error) {
	s := makeStmt(c, query, QUERY)
	return s.Query(args)
}

func (c *OpenAPIConn) Exec(query string, args []driver.Value) (driver.Result, error) {
	s := makeStmt(c, query, EXEC)
	return s.Exec(args)
}

func (c *OpenAPIConn) Prepare(query string) (driver.Stmt, error) {
	return makeStmt(c, query, QUERY), nil
}

func (c *OpenAPIConn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

func (c *OpenAPIConn) Close() error {
	return disconnect(c)
}

func (s *stmt) Exec(args []driver.Value) (driver.Result, error) {
	s.queryType = EXEC
	if len(args) > 0 {
		s.args = args
	}

	if s.conn.currentTransaction == nil {
		return nil, errors.New("transaction required")
	}
	rows, err := s.runQuery(s.conn.currentTransaction.handle)
	if err != nil {
		return nil, err
	}

	err = rows.fetchInfo()
	if err != nil {
		return nil, err
	}

	err = rows.Close()
	if err != nil {
		return nil, err
	}

	return rows, nil
}

func (s *stmt) Query(args []driver.Value) (driver.Rows, error) {
	s.queryType = QUERY
	s.args = args

	if s.conn.currentTransaction == nil {
		return nil, errors.New("transaction required")
	}

	return s.runQuery(s.conn.currentTransaction.handle)
}

func (s *stmt) NumInput() int {
	return -1
}

func (t *OpenAPITransaction) Commit() error {
	err := commitTransaction(t.handle)
	if err == nil {
		t.conn.currentTransaction = nil
		t.conn.AutoCommit()
	}
	return err
}

func (t *OpenAPITransaction) Rollback() error {
	err := rollbackTransaction(t.handle)
	if err == nil {
		t.conn.currentTransaction = nil
		t.conn.AutoCommit()
	}
	return err
}
