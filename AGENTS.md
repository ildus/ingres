# Agents

Go `database/sql` driver for Ingres/Vector via OpenAPI (`iiapi`,
cgo). Module `github.com/ildus/ingres`, Go 1.21+ (`runtime.Pinner`).

Build needs `II_SYSTEM`, then `make iiapi.pc` and
`PKG_CONFIG_PATH=. go build`. Tests need a live server
(`INGRES_TEST_DB`, default `mydb`). If `pkg-config` cannot see
`iiapi`, say the change was not compiled.

`~V` is the parameter marker. Leave it in the query and bind with
`IIapi_putParms`. Do not splice argument text into SQL.

OpenAPI calls return before they finish. Call, then `waitContext`,
and keep buffers valid until that wait returns. `IIapi_getColumns`
reuses column buffers until `rows` is closed.

Go pointers stored in `malloc`'d `IIAPI_DATAVALUE` arrays must be
pinned (`pinBytes` / `runtime.Pinner`) for that whole window. Do not
copy argument or column bytes into C memory to satisfy the cgo rule.
Unpin before freeing the C array. A `Pinner` collected while it still
holds pins panics, so every `Close` path must unpin.

In a column block, `i` is the slot in the block and `j = start + i`
is the column. Buffer and length both come from column `j`.

Allocate C strings with `cString` and `C.free` them after the OpenAPI
call completes. Reject NUL bytes there. Server error text is data
(`errors.New`), not a format string.

Column decodes must fail on a short buffer or a length prefix past
the buffer, not panic. Copy `[]byte` results out of driver memory.
`LastInsertId` is unsupported.
