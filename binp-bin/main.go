package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/sryanyuan/binp/dbg"
	"github.com/sryanyuan/binp/rule"

	"github.com/juju/errors"
	"github.com/sirupsen/logrus"
	"github.com/sryanyuan/binp/slave"

	_ "net/http/pprof"
)

var (
	flagConfigPath string
)

func main() {
	// Debug pprof
	if os.Getenv("HTTP_PPROF") != "" {
		go http.ListenAndServe(os.Getenv("HTTP_PPROF"), nil)
	}
	var err error
	var config AppConfig

	// Get config
	flag.StringVar(&flagConfigPath, "config", "", "config file path")
	flag.Parse()

	if "" == flagConfigPath {
		logrus.Infof("missing config file")
		flag.PrintDefaults()
		return
	}

	err = config.fromFile(flagConfigPath)
	if nil != err {
		logrus.Infof("parse config file %v error = %v", flagConfigPath, err)
		return
	}

	// Set log config
	if err = initLog(&config.Log); nil != err {
		logrus.Errorf("init log error = %v", err)
		return
	}

	// Set sync rule config
	sds, err := config.SRule.ToSyncDescs()
	if nil != err {
		logrus.Errorf("init sync rule desc error = %v", err)
		return
	}
	sr, err := rule.NewDefaultSyncRuleWithRules(sds)
	if nil != err {
		logrus.Errorf("init sync rule error = %v", err)
		return
	}

	slv := slave.NewSlave(config.DataSources, &config.Replication, sr)
	handler := NewEventHandler(slv, &config)
	if err = handler.Prepare(); nil != err {
		logrus.Errorf(errors.Details(err))
		return
	}

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

	if 0 != dbg.Get().PprofPort {
		logrus.Infof("Open pprof port at %d", dbg.Get().PprofPort)
		go func() {
			logrus.Error(http.ListenAndServe(fmt.Sprintf(":%d", dbg.Get().PprofPort), nil))
		}()
	}

	err = handler.handleEvent()
	if nil != err {
		logrus.Errorf("Handle binlog event error: %v", errors.ErrorStack(err))
	} else {
		<-eh
	}
}
