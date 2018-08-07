package utils

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/juju/errors"
	"github.com/sryanyuan/binp/mconn"
)

func CreateDB(dc *mconn.DBConfig) (*sql.DB, error) {
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
