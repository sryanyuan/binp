package main

import (
	"github.com/juju/errors"
	"github.com/sirupsen/logrus"
	"github.com/sryanyuan/binp/slave"
)

func main() {
	var err error
	var config AppConfig
	config.Log.Level = "debug"

	if err = initLog(&config.Log); nil != err {
		logrus.Errorf("init log error = %v", err)
		return
	}

	slv := slave.NewSlave(&slave.ReplicationConfig{
		Host:     "10.100.47.4",
		Port:     3310,
		Username: "root",
		Password: "password",
	})

	if err = slv.Start(slave.Position{}); nil != err {
		logrus.Error(errors.Details(err))
	}
}
