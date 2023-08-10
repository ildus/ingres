package ingres

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"io"
	"testing"
)

func TestInitOpenAPI(t *testing.T) {
	verbose = false
	env, err := InitOpenAPI()
	require.Equal(t, err, nil)
	ReleaseOpenAPI(env)
}

func TestConnect(t *testing.T) {
	env, err := InitOpenAPI()
	require.NoError(t, err)
	defer ReleaseOpenAPI(env)

	conn, err := env.Connect(ConnParams{DbName: "mydb"})
	require.NoError(t, err)
	defer conn.Close()

	err = conn.AutoCommit()
	require.NoError(t, err)

	err = conn.DisableAutoCommit()
	require.NoError(t, err)
}

func TestManyRows(t *testing.T) {
	conn, deinit := testconn(t)
	defer deinit()

	tx, err := conn.Begin()
	require.NoError(t, err)
	defer tx.Rollback()

	rows, err := conn.Query("select reltid, relid from iirelation limit 5", nil)
	require.Equal(t, err, nil)
	assert.Equal(t, rows.Columns()[0], "reltid")
	assert.Equal(t, rows.Columns()[1], "relid")
	defer rows.Close()

	count := 0
	for {
		var dest = make([]driver.Value, len(rows.Columns()))
		if rows.Next(dest) == io.EOF {
			break
		}

		count += 1
	}
	assert.Equal(t, 5, count)
}

func TestHandleError(t *testing.T) {
	conn, deinit := testconn(t)
	defer deinit()

	tx, err := conn.Begin()
	require.NoError(t, err)
	defer tx.Rollback()

	// should be error
	rows, err := conn.Query("select reltid, from iirelation", nil)
	assert.NotEqual(t, nil, err)
	assert.Contains(t, err.Error(), "Syntax error")
	assert.Nil(t, rows)
}

func testconn(t *testing.T) (*OpenAPIConn, func()) {
	env, err := InitOpenAPI()
	require.NoError(t, err)

	conn, err := env.Connect(ConnParams{DbName: "mydb"})
	require.NoError(t, err)
	if err != nil {
		ReleaseOpenAPI(env)
		t.Fail()
	}

	return conn, func() {
		conn.Close()
		ReleaseOpenAPI(env)
	}
}

func TestExec(t *testing.T) {
	conn, deinit := testconn(t)
	defer deinit()

	err := conn.AutoCommit()
	require.NoError(t, err)
	defer conn.DisableAutoCommit()

	_, err = conn.Exec("create table if not exists test_table(a int)", nil)
	require.NoError(t, err)

	_, err = conn.Exec("insert into test_table values (1), (2)", nil)
	require.NoError(t, err)

	rows, err := conn.Query("select count(*) from test_table", nil)
	require.NoError(t, err)
	defer rows.Close()

	dest := make([]driver.Value, len(rows.Columns()))
	rows.Next(dest)
	assert.Equal(t, int32(2), dest[0].(int32))

	_, err = conn.Exec("drop table test_table", nil)
	require.NoError(t, err)
}

func TestFetch(t *testing.T) {
	conn, deinit := testconn(t)
	defer deinit()

	tx, err := conn.Begin()
	require.Nil(t, err)

	_, err = conn.Exec("create table if not exists test_table(a int)", nil)
	require.Nil(t, err)

	_, err = conn.Exec("insert into test_table values (1), (2)", nil)
	require.Nil(t, err)

	_, err = conn.Exec("select * from test_table", nil)
	require.Nil(t, err)

	_, err = conn.Exec("drop table test_table", nil)
	require.Nil(t, err)

	err = tx.Commit()
	require.Nil(t, err)

}

func TestDecode(t *testing.T) {
	conn, deinit := testconn(t)
	defer deinit()

	tx, err := conn.Begin()
	require.NoError(t, err)
	defer tx.Rollback()

	rows, err := conn.Query(`select
            int1(10), int2(11),int4(12), int8(13),
            float4(1.1), float8(10.1),
            true, false,
            c('a'), char('b'), varchar('c'), text('d'),
            byte('aa'), varbyte('bb'),
            nchar('aaa'), nvarchar('bbb')
        from iirelation limit 5`, nil)

	require.Equal(t, err, nil)
	defer rows.Close()

	dest := make([]driver.Value, len(rows.Columns()))
	rows.Next(dest)
	assert.Equal(t, dest[0].(int8), int8(10))
	assert.Equal(t, dest[1].(int16), int16(11))
	assert.Equal(t, dest[2].(int32), int32(12))
	assert.Equal(t, dest[3].(int64), int64(13))
	assert.Equal(t, dest[4].(float32), float32(1.1))
	assert.Equal(t, dest[5].(float64), float64(10.1))
	assert.True(t, dest[6].(bool))
	assert.False(t, dest[7].(bool))
	assert.Equal(t, "a", dest[8].(string))
	assert.Equal(t, "b", dest[9].(string))
	assert.Equal(t, "c", dest[10].(string))
	assert.Equal(t, "d", dest[11].(string))
	assert.Equal(t, []byte("aa"), dest[12].([]byte))
	assert.Equal(t, []byte("bb"), dest[13].([]byte))
	assert.Equal(t, "aaa", dest[14].(string))
	assert.Equal(t, "bbb", dest[15].(string))
}

func TestLongVarchar(t *testing.T) {
	conn, deinit := testconn(t)
	defer deinit()

	tx, err := conn.Begin()
	require.NoError(t, err)
	defer tx.Rollback()

	_, err = conn.Exec("drop table if exists test_long", nil)
	require.NoError(t, err)

	_, err = conn.Exec("create table test_long(a int, b long varchar, c int)", nil)
	require.NoError(t, err)

	_, err = conn.Exec("insert into test_long values (1, repeat('a', 10000), 2)", nil)
	require.NoError(t, err)

	_, err = conn.Exec("insert into test_long values (3, repeat('b', 12345), 4)", nil)
	require.NoError(t, err)

	_, err = conn.Exec("insert into test_long values (3, 'a', 4)", nil)
	require.NoError(t, err)

	rows, err := conn.Query(`select * from test_long`, nil)
	require.NoError(t, err)
	defer rows.Close()

	dest := make([]driver.Value, len(rows.Columns()))

	// 1st line
	err = rows.Next(dest)
	require.NoError(t, err)

	require.Equal(t, dest[0].(int32), int32(1))

	res := dest[1].(string)
	assert.Equal(t, 10000, len(res))
	for i := 0; i < len(res); i++ {
		assert.Equal(t, 'a', rune(res[i]), fmt.Sprintf(`at location %d expected 'a'`, i))
	}
	require.Equal(t, dest[2].(int32), int32(2))

	// 2nd line
	err = rows.Next(dest)
	require.NoError(t, err)
	require.Equal(t, dest[0].(int32), int32(3))

	res = dest[1].(string)
	assert.Equal(t, 12345, len(res))
	for i := 0; i < len(res); i++ {
		assert.Equal(t, 'b', rune(res[i]), fmt.Sprintf(`at location %d expected 'b'`, i))
	}
	require.Equal(t, dest[2].(int32), int32(4))

	// 3rd line
	err = rows.Next(dest)
	require.NoError(t, err)
	require.Equal(t, dest[0].(int32), int32(3))

	res = dest[1].(string)
	assert.Equal(t, 1, len(res))
	assert.Equal(t, "a", res)
	require.Equal(t, dest[2].(int32), int32(4))
}

func TestLongNVarchar(t *testing.T) {
	conn, deinit := testconn(t)
	defer deinit()

	tx, err := conn.Begin()
	require.NoError(t, err)
	defer tx.Rollback()

	_, err = conn.Exec("drop table if exists test_long", nil)
	require.NoError(t, err)

	_, err = conn.Exec("create table test_long(a int, b long nvarchar, c int)", nil)
	require.NoError(t, err)

	_, err = conn.Exec("insert into test_long values (1, repeat('a', 10000), 2)", nil)
	require.NoError(t, err)

	_, err = conn.Exec("insert into test_long values (3, repeat('b', 12345), 4)", nil)

	require.NoError(t, err)

	rows, err := conn.Query(`select * from test_long`, nil)
	require.NoError(t, err)
	defer rows.Close()

	dest := make([]driver.Value, len(rows.Columns()))
	err = rows.Next(dest)
	require.NoError(t, err)

	require.Equal(t, dest[0].(int32), int32(1))

	res := dest[1].(string)
	assert.Equal(t, 10000, len(res))
	for i := 0; i < len(res); i++ {
		assert.Equal(t, 'a', rune(res[i]), fmt.Sprintf(`at location %d expected 'a'`, i))
	}
	require.Equal(t, dest[2].(int32), int32(2))

	err = rows.Next(dest)
	require.NoError(t, err)
	require.Equal(t, dest[0].(int32), int32(3))

	res = dest[1].(string)
	assert.Equal(t, 12345, len(res))
	for i := 0; i < len(res); i++ {
		assert.Equal(t, 'b', rune(res[i]), fmt.Sprintf(`at location %d expected 'b'`, i))
	}
	require.Equal(t, dest[2].(int32), int32(4))
}

func TestLongBytes(t *testing.T) {
	conn, deinit := testconn(t)
	defer deinit()

	tx, err := conn.Begin()
	require.NoError(t, err)
	defer tx.Rollback()

	_, err = conn.Exec("drop table if exists test_long", nil)
	require.NoError(t, err)

	_, err = conn.Exec("create table test_long(a int, b long byte, c int)", nil)
	require.NoError(t, err)

	_, err = conn.Exec("insert into test_long values (1, repeat('a', 10000), 2)", nil)
	require.NoError(t, err)

	_, err = conn.Exec("insert into test_long values (3, repeat('b', 12345), 4)", nil)

	require.NoError(t, err)

	rows, err := conn.Query(`select * from test_long`, nil)
	require.NoError(t, err)
	defer rows.Close()

	dest := make([]driver.Value, len(rows.Columns()))
	err = rows.Next(dest)
	require.NoError(t, err)

	require.Equal(t, dest[0].(int32), int32(1))

	res := dest[1].([]byte)
	assert.Equal(t, 10000, len(res))
	for i := 0; i < len(res); i++ {
		assert.Equal(t, 'a', rune(res[i]), fmt.Sprintf(`at location %d expected 'a'`, i))
	}
	require.Equal(t, dest[2].(int32), int32(2))

	err = rows.Next(dest)
	require.NoError(t, err)
	require.Equal(t, dest[0].(int32), int32(3))

	res = dest[1].([]byte)
	assert.Equal(t, 12345, len(res))
	for i := 0; i < len(res); i++ {
		assert.Equal(t, 'b', rune(res[i]), fmt.Sprintf(`at location %d expected 'b'`, i))
	}
	require.Equal(t, dest[2].(int32), int32(4))
}

func TestDates(t *testing.T) {
	conn, deinit := testconn(t)
	defer deinit()

	tx, err := conn.Begin()
	require.NoError(t, err)
	defer tx.Rollback()

	_, err = conn.Exec("drop table if exists test_dates", nil)
	require.NoError(t, err)

	_, err = conn.Exec(`create table test_dates(
        d1 ingresdate,
        d2 date,
        t1 time with time zone,
        t2 time without time zone,
        t3 time with local time zone,
        ts1 timestamp with time zone,
        ts2 timestamp without time zone,
        ts3 timestamp with local time zone,
        i1 interval year to month,
        i2 interval day to second
    )`, nil)
	require.NoError(t, err)

	_, err = conn.Exec(`insert into test_dates values (
        '2021-10-10',
        '2022-10-10',
        '12:30:55-05:00',
        '12:30:55',
        '12:30:56',
        '2006-12-15 9:30:55-08:00',
        '2007-12-15 12:30:55',
        '2008-12-15 12:30:55',
        '55-4',
        '-18 12:02:23.12345'
    )`, nil)
	require.NoError(t, err)

	rows, err := conn.Query(`select * from test_dates`, nil)
	require.NoError(t, err)
	defer rows.Close()

	dest := make([]driver.Value, len(rows.Columns()))
	err = rows.Next(dest)
	require.NoError(t, err)

	assert.Equal(t, dest[0].(string), "10-oct-2021")
	assert.Equal(t, dest[1].(string), "2022-10-10")
	assert.Equal(t, dest[2].(string), "12:30:55-05:00")
	assert.Equal(t, dest[3].(string), "12:30:55")
	assert.Equal(t, dest[4].(string), "12:30:56")
	assert.Equal(t, dest[5].(string), "2006-12-15 09:30:55-08:00")
	assert.Equal(t, dest[6].(string), "2007-12-15 12:30:55")
	assert.Equal(t, dest[7].(string), "2008-12-15 12:30:55")
	assert.Equal(t, dest[8].(string), "55-04")
	assert.Equal(t, dest[9].(string), "-18 12:02:23")
}

func TestNull(t *testing.T) {
	conn, err := sql.Open("ingres", "mydb")
	require.NoError(t, err)
	defer conn.Close()

	_, err = conn.Exec("drop table if exists test_null")
	require.NoError(t, err)

	_, err = conn.Exec(`create table test_null(
        d1 int null,
        d2 varchar(10) null,
        d3 int not null
    )`)
	require.NoError(t, err)

	_, err = conn.Exec(`insert into test_null values
        (null, 'asdf', 1),
        (1, null, 2),
        (null, null, 3),
        (2, 'hkll', 4)
    `)
	require.NoError(t, err)

	rows, err := conn.Query(`select * from test_null`)
	require.NoError(t, err)
	defer rows.Close()

	var intval sql.NullInt32
	var strval sql.NullString
	var int2val int

	rows.Next()
	err = rows.Scan(&intval, &strval, &int2val)
	require.NoError(t, err)
	assert.False(t, intval.Valid)
	assert.True(t, strval.Valid)
	assert.Equal(t, 1, int2val)

	rows.Next()
	err = rows.Scan(&intval, &strval, &int2val)
	require.NoError(t, err)
	assert.True(t, intval.Valid)
	assert.False(t, strval.Valid)
	assert.Equal(t, 2, int2val)

	rows.Next()
	err = rows.Scan(&intval, &strval, &int2val)
	require.NoError(t, err)
	assert.False(t, intval.Valid)
	assert.False(t, strval.Valid)
	assert.Equal(t, 3, int2val)

	rows.Next()
	err = rows.Scan(&intval, &strval, &int2val)
	require.NoError(t, err)
	assert.True(t, intval.Valid)
	assert.True(t, strval.Valid)
	assert.Equal(t, 4, int2val)

	require.False(t, rows.Next())
	require.NoError(t, rows.Err())
}

func TestArgs(t *testing.T) {
	conn, err := sql.Open("ingres", "mydb")
	require.NoError(t, err)
	defer conn.Close()

	_, err = conn.Exec("drop table if exists test_args")
	require.NoError(t, err)

	_, err = conn.Exec(`create table test_args(
        d1 int null,
        d2 varchar(10) null,
        d3 int not null,
        d4 float4,
        d5 float8
    )`)
	require.NoError(t, err)

	res, err := conn.Exec(`insert into test_args values ( ~V , ~V , ~V , ~V , ~V )`,
        1, "aaa", 2, 0.1, 0.2)
	require.NoError(t, err)

	stmt, err := conn.Prepare(`insert into test_args values ( ~V , ~V , ~V , ~V , ~V )`)
	require.NoError(t, err)

    res, err = stmt.Exec(3, "bbb", 4, 1.1, 2.2)
	require.NoError(t, err)

    res, err = stmt.Exec(3, "ccc", 4, 3.3, 4.4)
	require.NoError(t, err)

	cnt, err := res.RowsAffected()
	require.NoError(t, err)
	assert.Equal(t, int64(1), cnt)

    rows, err := conn.Query("select * from test_args")
    defer rows.Close()

	var intval sql.NullInt32
	var strval sql.NullString
	var int2val int
	var f1 float32
	var f2 float64

	rows.Next()
	err = rows.Scan(&intval, &strval, &int2val, &f1, &f2)
	require.NoError(t, err)
	assert.Equal(t, int32(1), intval.Int32)
	assert.Equal(t, "aaa", strval.String)
	assert.Equal(t, 2, int2val)
	assert.Equal(t, float32(0.1), f1)
	assert.Equal(t, 0.2, f2)

	rows.Next()
	err = rows.Scan(&intval, &strval, &int2val, &f1, &f2)
	require.NoError(t, err)
	assert.Equal(t, int32(3), intval.Int32)
	assert.Equal(t, "bbb", strval.String)
	assert.Equal(t, 4, int2val)

	rows.Next()
	err = rows.Scan(&intval, &strval, &int2val, &f1, &f2)
	require.NoError(t, err)
	assert.Equal(t, int32(3), intval.Int32)
	assert.Equal(t, "ccc", strval.String)
	assert.Equal(t, 4, int2val)

	require.False(t, rows.Next())
	require.NoError(t, rows.Err())

	_, err = conn.Exec("drop table if exists test_args")
	require.NoError(t, err)
}
