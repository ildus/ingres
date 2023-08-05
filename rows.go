package ingres

import (
	"reflect"
)

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

func (rs *rows) ColumnTypeNullable(index int) (nullable bool, ok bool) {
	return rs.colTyps[index].nullable, true
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

func (rs rows) LastInsertId() (int64, error) {
    return rs.lastInsertId, nil
}

func (rs rows) RowsAffected() (int64, error) {
	return rs.rowsAffected, nil
}
