package observer

import (
	"database/sql"
	"fmt"

	"github.com/juju/errors"
	"github.com/sryanyuan/binp/binlog"
)

// MySQLDest is the config show how to synchronize binlog data to destinate MySQL
type MySQLDest struct {
	Host     string
	Port     uint16
	Username string
	Password string
	Charset  string
	Text     bool
}

func (d *MySQLDest) dsn() string {
	interpolateParams := "false"
	if d.Text {
		interpolateParams = "true"
	}
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/?charset=%s&interpolateParams=%s",
		d.Username, d.Password, d.Host, d.Port, d.Charset, interpolateParams)
}

// MySQLObserver observe binlog events and translate to SQL statement, and execute to destinate database
type MySQLObserver struct {
	BaseObserver
	toi int
	tos []*sql.DB
}

// NewMySQLObserver create an observer observe binlog event and synchronize to MySQL
func NewMySQLObserver(dsts []MySQLDest) (*MySQLObserver, error) {
	tos := make([]*sql.DB, 0, len(dsts))
	for i := range dsts {
		dst := &dsts[i]
		db, err := sql.Open("mysql", dst.dsn())
		if nil != err {
			return nil, errors.Trace(err)
		}
		tos = append(tos, db)
	}
	return &MySQLObserver{tos: tos}, nil
}

func (o *MySQLObserver) OnEvent(evt *binlog.Event) error {
	return nil
}
