// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package log

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"

	"github.com/pingcap/errors"
	pclog "github.com/pingcap/log"
	lightningLog "github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tiflow/dm/pkg/helper"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	defaultLogLevel   = "info"
	defaultLogMaxDays = 7
	defaultLogMaxSize = 512 // MB
)

// Config serializes log related config in toml/json.
type Config struct {
	// Log level.
	Level string `toml:"level" json:"level"`
	// the format of the log, "text" or "json"
	Format string `toml:"format" json:"format"`
	// Log filename, leave empty to disable file log.
	File string `toml:"file" json:"file"`
	// Max size for a single file, in MB.
	FileMaxSize int `toml:"max-size" json:"max-size"`
	// Max log keep days, default is never deleting.
	FileMaxDays int `toml:"max-days" json:"max-days"`
	// Maximum number of old log files to retain.
	FileMaxBackups int `toml:"max-backups" json:"max-backups"`
}

// Adjust adjusts config.
func (cfg *Config) Adjust() {
	if len(cfg.Level) == 0 {
		cfg.Level = defaultLogLevel
	}
	if cfg.Level == "warning" {
		cfg.Level = "warn"
	}
	if cfg.FileMaxSize == 0 {
		cfg.FileMaxSize = defaultLogMaxSize
	}
	if cfg.FileMaxDays == 0 {
		cfg.FileMaxDays = defaultLogMaxDays
	}
}

// Logger is a simple wrapper around *zap.Logger which provides some extra
// methods to simplify DM's log usage.
type Logger struct {
	*zap.Logger
}

// WithFields return new Logger with specified fields.
func (l Logger) WithFields(fields ...zap.Field) Logger {
	return Logger{l.With(fields...)}
}

// ErrorFilterContextCanceled wraps Logger.Error() and will filter error log when error is context.Canceled.
func (l Logger) ErrorFilterContextCanceled(msg string, fields ...zap.Field) {
	for _, field := range fields {
		switch field.Type {
		case zapcore.StringType:
			if field.Key == "error" && strings.Contains(field.String, context.Canceled.Error()) {
				return
			}
		case zapcore.ErrorType:
			err, ok := field.Interface.(error)
			if ok && errors.Cause(err) == context.Canceled {
				return
			}
		}
	}
	l.Logger.WithOptions(zap.AddCallerSkip(1)).Error(msg, fields...)
}

// logger for DM.
var (
	appLogger = Logger{zap.NewNop()}
	appLevel  zap.AtomicLevel
	appProps  *pclog.ZapProperties
)

// InitLogger initializes DM's and also the TiDB library's loggers.
func InitLogger(cfg *Config) error {
	inDev := strings.ToLower(cfg.Level) == "debug"
	// init DM logger
	logger, props, err := pclog.InitLogger(&pclog.Config{
		Level:  cfg.Level,
		Format: cfg.Format,
		File: pclog.FileLogConfig{
			Filename:   cfg.File,
			MaxSize:    cfg.FileMaxSize,
			MaxDays:    cfg.FileMaxDays,
			MaxBackups: cfg.FileMaxBackups,
		},
		Development: inDev,
	})
	if err != nil {
		return terror.ErrInitLoggerFail.Delegate(err)
	}

	// Do not log stack traces at all, as we'll get the stack trace from the
	// error itself.
	logger = logger.WithOptions(zap.AddStacktrace(zap.DPanicLevel))
	pclog.ReplaceGlobals(logger, props)

	lightningLogger := logger.With(zap.String("component", "lightning"))
	lightningLog.SetAppLogger(lightningLogger)

	appLogger = Logger{logger}
	appLevel = props.Level
	appProps = props
	// init and set tidb slow query logger to stdout if log level is debug
	if inDev {
		slowQueryLogger := zap.NewExample()
		slowQueryLogger = slowQueryLogger.With(zap.String("component", "slow query logger"))
		logutil.SlowQueryLogger = slowQueryLogger
	} else {
		logutil.SlowQueryLogger = zap.NewNop()
	}
	return nil
}

// With creates a child logger from the global logger and adds structured
// context to it.
func With(fields ...zap.Field) Logger {
	return Logger{appLogger.With(fields...)}
}

// SetLevel modifies the log level of the global logger. Returns the previous
// level.
func SetLevel(level zapcore.Level) zapcore.Level {
	oldLevel := appLevel.Level()
	appLevel.SetLevel(level)
	return oldLevel
}

// ShortError contructs a field which only records the error message without the
// verbose text (i.e. excludes the stack trace).
//
// In DM, all errors are almost always propagated back to `main()` where
// the error stack is written. Including the stack in the middle thus usually
// just repeats known information. You should almost always use `ShortError`
// instead of `zap.Error`, unless the error is no longer propagated upwards.
func ShortError(err error) zap.Field {
	if err == nil {
		return zap.Skip()
	}
	return zap.String("error", err.Error())
}

// L returns the current logger for DM.
func L() Logger {
	return appLogger
}

// Props returns the current logger's props.
func Props() *pclog.ZapProperties {
	return appProps
}

// WrapStringerField returns a wrap stringer field.
func WrapStringerField(message string, object fmt.Stringer) zap.Field {
	if helper.IsNil(object) {
		return zap.String(message, "NULL")
	}

	return zap.Stringer(message, object)
}

// WithCtx adds fields from ctx to the logger.
func WithCtx(ctx context.Context) Logger {
	return Logger{appLogger.With(getZapFieldsFromCtx(ctx)...)}
}

var enabledRedactLog atomic.Bool

func init() {
	SetRedactLog(false)
}

// IsRedactLogEnabled indicates whether the log desensitization is enabled.
func IsRedactLogEnabled() bool {
	return enabledRedactLog.Load()
}

// SetRedactLog sets enabledRedactLog.
func SetRedactLog(enabled bool) {
	enabledRedactLog.Store(enabled)
}

// RedactString receives string argument and return omitted information if redact log enabled.
func RedactString(arg string) string {
	if IsRedactLogEnabled() {
		return "?"
	}
	return arg
}

// ZapRedactString receives string argument and return omitted information in zap.Field if redact log enabled.
func ZapRedactString(key, arg string) zap.Field {
	return zap.String(key, RedactString(arg))
}
