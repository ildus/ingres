package ingres

import (
	"testing"
    "github.com/stretchr/testify/require"
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

    rows, err := conn.Fetch("select table_name from iitables")
    require.Equal(t, err, nil)
    require.Equal(t, rows.colNames[0], "table_name")
    defer rows.Close()
}
