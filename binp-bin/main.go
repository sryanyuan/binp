package main

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/juju/errors"
	"github.com/sirupsen/logrus"
	"github.com/sryanyuan/binp/mconn"
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

	slv := slave.NewSlave(&mconn.ReplicationConfig{
		Host:     "10.100.47.4",
		Port:     3310,
		Username: "root",
		Password: "password",
	})
	if err = slv.Start(mconn.Position{}); nil != err {
		logrus.Error(errors.Details(err))
		return
	}

	handler := NewEventHandler(slv)
	eh := make(chan struct{})
	sh := make(chan os.Signal, 1)
	signal.Notify(sh,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)
	go func() {
		s := <-sh
		logrus.Infof("Got signal %v", s)
		handler.Close()
		close(eh)
	}()

	err = handler.handleEvent()
	if nil != err {
		logrus.Errorf("Handle binlog event error: %v", errors.ErrorStack(err))
	} else {
		<-eh
	}
}
