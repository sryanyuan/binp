package tableinfo

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/juju/errors"
	"github.com/sirupsen/logrus"
)

// GetTableInfo get table info from database
func GetTableInfo(db *sql.DB, schema string, table string) (*TableInfo, error) {
	ti := &TableInfo{}
	// Get table column info
	if err := ti.GetTableColumnInfo(db, schema, table); nil != err {
		return nil, errors.Trace(err)
	}

	return ti, nil
}

// GetTableColumnInfo get columns info from database
func (ti *TableInfo) GetTableColumnInfo(db *sql.DB, schema string, table string) error {
	rs, err := retryQuery(db, fmt.Sprintf("SHOW COLUMNS FROM %s.%s", schema, table))
	if nil != err {
		return errors.Trace(err)
	}
	defer rs.Close()

	columns, err := rs.Columns()
	if nil != err {
		return errors.Trace(err)
	}
	columnCnt := len(columns)
	datas := make([]sql.RawBytes, columnCnt)
	args := make([]interface{}, columnCnt)
	for i := 0; i < columnCnt; i++ {
		args[i] = &datas[i]
	}

	columnIndex := 0
	for rs.Next() {
		err = rs.Scan(args...)
		if nil != err {
			return errors.Trace(err)
		}

		if nil == ti.Columns {
			ti.Columns = make([]*ColumnInfo, 0, 32)
		}
		var column ColumnInfo
		column.Index = columnIndex
		column.Name = string(datas[0])
		column.Type = string(datas[1])
		if strings.EqualFold(string(datas[2]), "NO") {
			column.Nullable = false
		}
		if nil != datas[4] {
			column.HasDefault = true
			column.Default = string(datas[4])
		}

		if strings.Contains(column.Type, "unsigned") {
			column.Unsigned = true
		}
		ti.Columns = append(ti.Columns, &column)
		columnIndex++
	}

	return nil
}

// GetTablePrimaryKeys get all primary keys
func (ti *TableInfo) GetTablePrimaryKeys(db *sql.DB, schema string, table string) error {
	rs, err := retryQuery(db, fmt.Sprintf("SHOW INDEX FROM %s.%s", schema, table))
	if nil != err {
		return errors.Trace(err)
	}
	defer rs.Close()

	columns, err := rs.Columns()
	if nil != err {
		return errors.Trace(err)
	}
	columnCnt := len(columns)
	datas := make([]sql.RawBytes, columnCnt)
	args := make([]interface{}, columnCnt)
	for i := 0; i < columnCnt; i++ {
		args[i] = &datas[i]
	}

	keyName := ""
	columnIndex := 0

	for rs.Next() {
		err = rs.Scan(args...)
		if nil != err {
			return errors.Trace(err)
		}
		// Unique key ?
		if "0" == string(datas[1]) {
			if "" == keyName {
				keyName = string(datas[2])
			} else {
				if keyName != string(datas[2]) {
					break
				}
			}

			ti.Columns[columnIndex].IsPrimary = true
		}

		columnIndex++
	}

	return nil
}

func retryQuery(db *sql.DB, stmt string) (*sql.Rows, error) {
	var rows *sql.Rows
	var err error
	maxRetryTimes := 1000

	for i := 0; i < maxRetryTimes; i++ {
		rows, err = db.Query(stmt)
		if nil != err {
			logrus.Errorf("DB query error = %v", err)
			continue
		}
		return rows, err
	}

	return nil, errors.Errorf("Failed to query , stmt = %s, error = %v", stmt, err)
}
