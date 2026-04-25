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
	_   driver.DriverContext = Driver{}
	_   driver.Connector = (*ingresConnector)(nil)
	_   driver.ConnBeginTx = (*OpenAPIConn)(nil)
	_   driver.ExecerContext = (*OpenAPIConn)(nil)
	_   driver.QueryerContext = (*OpenAPIConn)(nil)
	_   driver.ConnPrepareContext = (*OpenAPIConn)(nil)
	_   driver.StmtExecContext = (*stmt)(nil)
	_   driver.StmtQueryContext = (*stmt)(nil)
	env *OpenAPIEnv
)

// Driver is the Ingres database driver.
type Driver struct{}

type ingresConnector struct {
	driver Driver
	name   string
}

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

	if verbose {
		env.EnableTrace()
	}

	d := &Driver{}
	sql.Register("ingres", d)
}

func (d Driver) Open(name string) (driver.Conn, error) {
	connector, err := d.OpenConnector(name)
	if err != nil {
		return nil, err
	}
	return connector.Connect(context.Background())
}

func (d Driver) OpenConnector(name string) (driver.Connector, error) {
	if _, err := parseConnParams(name); err != nil {
		return nil, err
	}
	return &ingresConnector{driver: d, name: name}, nil
}

func (c *ingresConnector) Connect(ctx context.Context) (driver.Conn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	params, err := parseConnParams(c.name)
	if err != nil {
		return nil, err
	}

	conn, err := env.ConnectContext(ctx, params)
	if err != nil {
		return nil, err
	}
	err = conn.AutoCommitContext(ctx)
	if err != nil {
		conn.Close()
		return nil, err
	}

	return conn, nil
}

func (c *ingresConnector) Driver() driver.Driver {
	return c.driver
}

func parseConnParams(name string) (ConnParams, error) {
	var params ConnParams

	if strings.Contains(name, "?") {
		parts := strings.Split(name, "?")
		if len(parts) != 2 {
			return ConnParams{}, errors.New("DSN is invalid")
		}

		values, err := url.ParseQuery(parts[1])

		if err != nil {
			return ConnParams{}, errors.New("parameters parse error")
		}

		if values.Has("username") && !values.Has("password") {
			return ConnParams{}, errors.New("password has not been specified")
		}

		params.UserName = values.Get("username")
		params.Password = values.Get("password")

		name = parts[0]
	}

	params.DbName = name
	return params, nil
}

func makeStmt(c *OpenAPIConn, query string, queryType QueryType) *stmt {
	return &stmt{
		args:      nil,
		conn:      c,
		query:     strings.TrimRight(query, "; "),
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

func (c *OpenAPIConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return c.Prepare(query)
}

func (c *OpenAPIConn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

func (c *OpenAPIConn) Close() error {
	return disconnect(c)
}

func isBadConnError(err error) bool {
	if err == nil {
		return false
	}

	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "current state") ||
		strings.Contains(msg, "active transactions") ||
		strings.Contains(msg, "active queries") ||
		strings.Contains(msg, "active statements") ||
		strings.Contains(msg, "incomplete query") ||
		strings.Contains(msg, "invalid sequence")
}

func namedValuesToValues(args []driver.NamedValue) ([]driver.Value, error) {
	vals := make([]driver.Value, len(args))
	for i, arg := range args {
		if arg.Name != "" {
			return nil, fmt.Errorf("named parameters are not supported")
		}
		vals[i] = arg.Value
	}
	return vals, nil
}

func (c *OpenAPIConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	vals, err := namedValuesToValues(args)
	if err != nil {
		return nil, err
	}

	s := makeStmt(c, query, EXEC)
	return s.execCtx(ctx, vals)
}

func (c *OpenAPIConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	vals, err := namedValuesToValues(args)
	if err != nil {
		return nil, err
	}

	s := makeStmt(c, query, QUERY)
	return s.queryCtx(ctx, vals)
}

func (s *stmt) Exec(args []driver.Value) (driver.Result, error) {
	return s.execCtx(context.Background(), args)
}

func (s *stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	vals, err := namedValuesToValues(args)
	if err != nil {
		return nil, err
	}
	return s.execCtx(ctx, vals)
}

func (s *stmt) execCtx(ctx context.Context, args []driver.Value) (driver.Result, error) {
	var rows *rows
	var err error

	if err = ctx.Err(); err != nil {
		return nil, err
	}

	s.queryType = EXEC
	if len(args) > 0 {
		s.args = args
	}

	if s.conn.currentTransaction == nil {
		err = s.conn.AutoCommit()
		if err != nil {
			if isBadConnError(err) {
				return nil, driver.ErrBadConn
			}
			return nil, err
		}
	}
	autocommitMode := s.conn.currentTransaction != nil && s.conn.currentTransaction.autocommit
	rows, err = s.runQuery(ctx, s.conn.currentTransaction.handle)
	if err != nil {
		if autocommitMode && isBadConnError(err) {
			return nil, driver.ErrBadConn
		}
		return nil, err
	}

	err = rows.fetchInfoContext(ctx)
	if err != nil {
		_ = rows.CloseContext(context.Background())
		if autocommitMode && isBadConnError(err) {
			return nil, driver.ErrBadConn
		}
		return nil, err
	}

	err = rows.CloseContext(ctx)
	if err != nil {
		if autocommitMode && isBadConnError(err) {
			return nil, driver.ErrBadConn
		}
		return nil, err
	}

	return rows, nil
}

func (s *stmt) Query(args []driver.Value) (driver.Rows, error) {
	return s.queryCtx(context.Background(), args)
}

func (s *stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	vals, err := namedValuesToValues(args)
	if err != nil {
		return nil, err
	}
	return s.queryCtx(ctx, vals)
}

func (s *stmt) queryCtx(ctx context.Context, args []driver.Value) (driver.Rows, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	s.queryType = QUERY
	s.args = args

	if s.conn.currentTransaction == nil {
		err := s.conn.AutoCommit()
		if err != nil {
			if isBadConnError(err) {
				return nil, driver.ErrBadConn
			}
			return nil, err
		}
	}
	autocommitMode := s.conn.currentTransaction != nil && s.conn.currentTransaction.autocommit

	rows, err := s.runQuery(ctx, s.conn.currentTransaction.handle)
	if err != nil && autocommitMode && isBadConnError(err) {
		return nil, driver.ErrBadConn
	}

	return rows, err
}

func (s *stmt) NumInput() int {
	return -1
}

func (t *OpenAPITransaction) Commit() error {
	if t.handle == nil {
		t.conn.currentTransaction = nil
		return nil
	}

	err := commitTransaction(t.handle)
	if err == nil {
		t.conn.currentTransaction = nil
	}
	return err
}

func (t *OpenAPITransaction) Rollback() error {
	if t.handle == nil {
		t.conn.currentTransaction = nil
		return nil
	}

	err := rollbackTransaction(t.handle)
	if err == nil {
		t.conn.currentTransaction = nil
	}
	return err
}
