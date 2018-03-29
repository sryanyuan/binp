package main

import (
	"bytes"
	"fmt"
	"path"
	"runtime"
	"sort"
	"strings"

	"github.com/Sirupsen/logrus"
)

const (
	defaultLogLevel      = logrus.InfoLevel
	defaultLogTimeFormat = "2006/01/02 15:04:05.000"
	defaultLogFormat     = "text"
)

type LogConfig struct {
	// Level options: [debug, info, warning, error, fatal]
	Level string
	// EnableColors enable color output
	EnableColors bool
}

func initLog(cfg *LogConfig) error {
	logrus.SetLevel(stringToLevel(cfg.Level))
	logrus.AddHook(&funcLineInjectorHook{})
	logrus.SetFormatter(&textFormatter{
		EnableColors: cfg.EnableColors,
	})

	return nil
}

func stringToLevel(lv string) logrus.Level {
	switch strings.ToLower(lv) {
	case "debug":
		{
			return logrus.DebugLevel
		}
	case "info":
		{
			return logrus.InfoLevel
		}
	case "warn", "warning":
		{
			return logrus.WarnLevel
		}
	case "error":
		{
			return logrus.ErrorLevel
		}
	case "fatal":
		{
			return logrus.FatalLevel
		}
	default:
		{
			return defaultLogLevel
		}
	}
}

// fileline injector
// isSKippedPackageName tests wether path name is on log library calling stack.
func isSkippedPackageName(name string) bool {
	return strings.Contains(name, "github.com/sirupsen/logrus")
}

type funcLineInjectorHook struct {
}

// Fire implements logrus.Hook interface
// https://github.com/sirupsen/logrus/issues/63
func (h *funcLineInjectorHook) Fire(entry *logrus.Entry) error {
	pc := make([]uintptr, 3)
	cnt := runtime.Callers(7, pc)

	for i := 0; i < cnt; i++ {
		fu := runtime.FuncForPC(pc[i] - 1)
		name := fu.Name()
		if !isSkippedPackageName(name) {
			file, line := fu.FileLine(pc[i] - 1)
			entry.Data["file"] = path.Base(file)
			entry.Data["line"] = line
			break
		}
	}
	return nil
}

// Levels implements logrus.Hook interface.
func (h *funcLineInjectorHook) Levels() []logrus.Level {
	return logrus.AllLevels
}

type textFormatter struct {
	DisableTimestamp bool
	EnableColors     bool
	EnableEntryOrder bool
}

// Format implements logrus.Formatter
func (f *textFormatter) Format(entry *logrus.Entry) ([]byte, error) {
	var b *bytes.Buffer
	if entry.Buffer != nil {
		b = entry.Buffer
	} else {
		b = &bytes.Buffer{}
	}

	if f.EnableColors {
		colorStr := logTypeToColor(entry.Level)
		fmt.Fprintf(b, "\033%sm ", colorStr)
	}

	if !f.DisableTimestamp {
		fmt.Fprintf(b, "%s ", entry.Time.Format(defaultLogTimeFormat))
	}
	if file, ok := entry.Data["file"]; ok {
		fmt.Fprintf(b, "%s:%v:", file, entry.Data["line"])
	}
	fmt.Fprintf(b, " [%s] %s", entry.Level.String(), entry.Message)

	if f.EnableEntryOrder {
		keys := make([]string, 0, len(entry.Data))
		for k := range entry.Data {
			if k != "file" && k != "line" {
				keys = append(keys, k)
			}
		}
		sort.Strings(keys)
		for _, k := range keys {
			fmt.Fprintf(b, " %v=%v", k, entry.Data[k])
		}
	} else {
		for k, v := range entry.Data {
			if k != "file" && k != "line" {
				fmt.Fprintf(b, " %v=%v", k, v)
			}
		}
	}

	b.WriteByte('\n')

	if f.EnableColors {
		b.WriteString("\033[0m")
	}
	return b.Bytes(), nil
}

// logTypeToColor converts the Level to a color string.
func logTypeToColor(level logrus.Level) string {
	switch level {
	case logrus.DebugLevel:
		return "[0;37"
	case logrus.InfoLevel:
		return "[0;36"
	case logrus.WarnLevel:
		return "[0;33"
	case logrus.ErrorLevel:
		return "[0;31"
	case logrus.FatalLevel:
		return "[0;31"
	case logrus.PanicLevel:
		return "[0;31"
	}

	return "[0;37"
}
