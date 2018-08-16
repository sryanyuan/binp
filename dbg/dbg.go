package dbg

import (
	"os"
	"strconv"
)

// Dbg holds debug information
type Dbg struct {
	Debug     bool
	PprofPort int
}

var (
	globalDbg Dbg
)

func init() {
	ev := os.Getenv("DEBUG")
	if "" != ev {
		globalDbg.Debug = true
	}
	ev = os.Getenv("PPORT")
	if "" != ev {
		port, err := strconv.Atoi(ev)
		if nil == err {
			globalDbg.PprofPort = port
		}
	}
}

// Get gets the debug information
func Get() *Dbg {
	return &globalDbg
}
