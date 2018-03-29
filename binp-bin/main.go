package main

import (
	"github.com/Sirupsen/logrus"
	"github.com/sryanyuan/binp/slave"
)

func main() {
	var err error
	var config AppConfig

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

	slv.Start(slave.Position{})
}
