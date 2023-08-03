package ingres

import (
	"testing"
    "github.com/stretchr/testify/require"
    "log"
    "io"

	"database/sql/driver"
)

func TestInitOpenAPI(t *testing.T) {
	env, err := InitOpenAPI()
    require.Equal(t, err, nil)
	ReleaseOpenAPI(env)
}

func TestConnect(t *testing.T) {
	env, err := InitOpenAPI()
    require.Equal(t, err, nil)
	defer ReleaseOpenAPI(env)

	conn, err := env.Connect(ConnParams{DbName: "mydb"})
    require.Equal(t, err, nil)
	defer conn.Disconnect()

    err = conn.AutoCommit()
    require.Equal(t, err, nil)

    err = conn.DisableAutoCommit()
    require.Equal(t, err, nil)
}

func TestQuery(t *testing.T) {
	env, err := InitOpenAPI()
    require.Equal(t, err, nil)
	defer ReleaseOpenAPI(env)

	conn, err := env.Connect(ConnParams{DbName: "mydb"})
    require.Equal(t, err, nil)
	defer conn.Disconnect()

    rows, err := conn.Fetch("select reltid, relid from iirelation limit 5")
    require.Equal(t, err, nil)
    require.Equal(t, rows.colNames[0], "reltid")
    require.Equal(t, rows.colNames[1], "relid")
    defer rows.Close()

    for {
        var dest = make([]driver.Value, len(rows.colNames))
        if rows.Next(dest) == io.EOF {
            break
        }

        log.Println(dest)
    }
}
