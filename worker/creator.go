package worker

import (
	"strings"

	"github.com/juju/errors"
)

type executorCreatorFn func(string, *DestinationConfig) (IJobExecutor, error)

var (
	executorCreators map[string]executorCreatorFn
)

func registerExecutor(name string, fn executorCreatorFn) {
	if nil == executorCreators {
		executorCreators = make(map[string]executorCreatorFn)
	}
	executorCreators[strings.ToLower(name)] = fn
}

func createExecutorByName(name string, cfg *DestinationConfig) (IJobExecutor, error) {
	fn, ok := executorCreators[strings.ToLower(name)]
	if !ok || nil == fn {
		return nil, errors.Errorf("Can't create executor named as %s", name)
	}
	return fn(name, cfg)
}
