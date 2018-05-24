package main

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/juju/errors"
	"github.com/sirupsen/logrus"
	"github.com/sryanyuan/binp/mconn"
)

func dbFromDBConfig(dc *mconn.DBConfig) (*sql.DB, error) {
	dtype := strings.ToLower(dc.Type)
	switch dtype {
	case "mysql":
		{
			db, err := sql.Open(dtype, fmt.Sprintf("%s:%s@tcp(%s:%d)/",
				dc.Username, dc.Password, dc.Host, dc.Port))
			if nil != err {
				return nil, errors.Trace(err)
			}
			return db, nil
		}
	default:
		{
			return nil, errors.Errorf("Unknown to type %s", dtype)
		}
	}
}

func executorFromDBConfig(dcs []mconn.DBConfig) (IJobExecutor, error) {
	if nil == dcs || 0 == len(dcs) {
		return nil, errors.New("Invalid executor input argument number")
	}
	// Every executor type must be same
	dtype := ""
	for i := range dcs {
		dc := &dcs[i]
		if "" == dtype {
			dtype = strings.ToLower(dc.Type)
		} else {
			if dtype != dc.Type {
				return nil, errors.New("Executor type must be same")
			}
		}
	}
	// Only mysql executor support multi db
	if dtype != "mysql" && len(dcs) > 1 {
		return nil, errors.New("Only mysql executor support multi destination")
	}

	if dtype == "mysql" {
		var me mysqlExecutor

		dbs := make([]*sql.DB, 0, len(dcs))
		for i := range dcs {
			dc := &dcs[i]
			db, err := dbFromDBConfig(dc)
			if nil != err {
				return nil, errors.Trace(err)
			}
			dbs = append(dbs, db)
		}

		if err := me.Attach(dbs); nil != err {
			return nil, errors.Trace(err)
		}

		return &me, nil
	}

	return nil, errors.Errorf("Unknown executor type %s", dtype)
}

func getTableKey(schema string, table string) string {
	return fmt.Sprintf("%s.%s", schema, table)
}

func getTableInfo(db *sql.DB, schema string, table string) (*TableInfo, error) {
	ti := &TableInfo{}
	// Get table column info
	if err := getTableColumnInfo(db, schema, table, ti); nil != err {
		return nil, errors.Trace(err)
	}

	return ti, nil
}

func getTableColumnInfo(db *sql.DB, schema string, table string, ti *TableInfo) error {
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

func getTableIndexInfo(db *sql.DB, schema string, table string, ti *TableInfo) error {
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

		if "0" == string(datas[1]) {
			if "" == keyName {
				keyName = string(datas[4])
			} else {
				if keyName != string(datas[4]) {
					break
				}
			}

			ti.Columns[columnIndex].HasIndex = true
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
