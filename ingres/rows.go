package ingres

import (
	"reflect"
    "errors"

	"database/sql/driver"
)

type noRows struct{}

var (
    emptyRows noRows

	errNoRowsAffected  = errors.New("no RowsAffected available after the empty statement")
	errNoLastInsertID  = errors.New("no LastInsertId available after the empty statement")
)

var _ driver.Result = noRows{}

func (noRows) LastInsertId() (int64, error) {
	return 0, errNoLastInsertID
}

func (noRows) RowsAffected() (int64, error) {
	return 0, errNoRowsAffected
}

// ColumnTypeScanType returns the value type that can be used to scan types into.
func (rs *rows) ColumnTypeScanType(index int) reflect.Type {
	return rs.colTyps[index].getType()
}

// ColumnTypeDatabaseTypeName return the database system type name.
func (rs *rows) ColumnTypeDatabaseTypeName(index int) string {
	return rs.colTyps[index].getTypeName()
}

// ColumnTypeLength returns the length of the column type if the column is a
// variable length type. If the column is not a variable length type ok
// should return false.
func (rs *rows) ColumnTypeLength(index int) (length int64, ok bool) {
	return rs.colTyps[index].Length()
}

// ColumnTypePrecisionScale should return the precision and scale for decimal
// types. If not applicable, ok should be false.
func (rs *rows) ColumnTypePrecisionScale(index int) (precision, scale int64, ok bool) {
    desc := rs.colTyps[index]
    if desc.isDecimal() {
	    return int64(rs.colTyps[index].precision), int64(rs.colTyps[index].scale), true
    }
    return 0, 0, false
}

func (rs *rows) Columns() []string {
	return rs.colNames
}

func (rs *rows) Result() driver.Result {
	if rs.result == nil {
		return emptyRows
	}

	return rs.result
}
