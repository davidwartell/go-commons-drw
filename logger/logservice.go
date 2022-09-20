/*
 * Copyright (c) 2022 by David Wartell. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

// Package logger Provides structured & deprecated unstructured logging facade to facilitate logging to different
// providers and multiple loggers.
package logger

import (
	"context"
	"fmt"
	"github.com/mattn/go-colorable"
	"github.com/natefinch/lumberjack"
	backupLogger "github.com/sirupsen/logrus"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const (
	consoleKey = "console"
	fileKey    = "file"
)

// A Level is a logging priority. Higher levels are more important.
type Level int8

//goland:noinspection GoUnusedConst
const (
	// DebugLevel logs are typically voluminous, and are usually disabled in
	// production.
	DebugLevel Level = iota - 1
	// InfoLevel is the default logging priority.
	InfoLevel
	// WarnLevel logs are more important than Info, but don't need individual
	// human review.
	WarnLevel
	// ErrorLevel logs are high-priority. If an application is running smoothly,
	// it shouldn't generate any error-level logs.
	ErrorLevel
	// DPanicLevel logs are particularly important errors. In development the
	// logger panics after writing the message.
	DPanicLevel
	// PanicLevel logs a message, then panics.
	PanicLevel
	// FatalLevel logs a message, then calls os.Exit(1).
	FatalLevel

	//_minLevel = DebugLevel
	//_maxLevel = FatalLevel

	DefaultAppShortName = "hello-world"
	taskName            = "Logging Service"
)

type LogInstance struct {
	logger  *zap.Logger
	level   zap.AtomicLevel
	enabled *atomic.Bool
}

type Singleton struct {
	sync.RWMutex
	started bool
	loggers map[string]*LogInstance
	options *Options
}

type LoggingOption func(o *Options)

type Options struct {
	productNameShort string
}

var instance *Singleton
var once sync.Once

// WithProductNameShort sets the name of your product used in log file names.
// example: logger.Instance().StartTask(logger.WithProductNameShort("your-product-name-here"))
//
//goland:noinspection GoUnusedExportedFunction
func WithProductNameShort(productNameShort string) LoggingOption {
	return func(o *Options) {
		o.productNameShort = productNameShort
	}
}

func (s *Singleton) StartTask(opts ...LoggingOption) {
	var err error
	s.Lock()
	if s.started {
		// if already started do nothing
		s.Unlock()
		return
	}

	// apply options
	for _, opt := range opts {
		opt(s.options)
	}

	//
	// console logger
	//
	s.loggers[consoleKey] = &LogInstance{
		enabled: atomic.NewBool(false),
	}
	s.loggers[consoleKey].level = zap.NewAtomicLevelAt(zapcore.Level(InfoLevel))

	consoleEncoderConfig := zap.NewDevelopmentEncoderConfig()
	//consoleEncoderConfig.FunctionKey = "function"		// uncomment this to enable calling function like: github.com/foo/bar/foo/slogger.(*Singleton).ErrorUnstruct
	consoleEncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
	consoleEncoderConfig.EncodeTime = zapcore.TimeEncoderOfLayout("2006-01-02 15:04:05.000000000 UTCZ07:00")

	s.loggers[consoleKey].logger = zap.New(
		zapcore.NewCore(
			zapcore.NewConsoleEncoder(consoleEncoderConfig),
			zapcore.AddSync(colorable.NewColorableStdout()),
			s.loggers[consoleKey].level,
		),
		zap.AddCallerSkip(1),
		zap.Development(),
		zap.AddCaller(),
		zap.AddStacktrace(zapcore.WarnLevel),
	)

	//
	// file logger
	//
	s.loggers[fileKey] = &LogInstance{
		enabled: atomic.NewBool(false),
	}
	s.loggers[fileKey].level = zap.NewAtomicLevelAt(zapcore.Level(ErrorLevel))

	var exPath string
	exPath, err = os.Executable()
	if err != nil {
		backupLogger.Errorf("error creating log file failed to get current working directory: %v", err)
	}
	workingDir := filepath.Dir(exPath)
	logFilePath := filepath.Join(workingDir, s.options.productNameShort+".log")
	lumberjackSink := zapcore.AddSync(&lumberjack.Logger{
		Filename:   logFilePath,
		MaxSize:    10, // megabytes
		MaxBackups: 5,
		MaxAge:     28, // days
	})

	fileEncoderConfig := zap.NewProductionEncoderConfig()
	fileEncoderConfig.EncodeTime = func(t time.Time, enc zapcore.PrimitiveArrayEncoder) {
		enc.AppendString(t.UTC().Format(time.RFC3339Nano))
	}

	fileLoggerCore := zapcore.NewCore(
		zapcore.NewJSONEncoder(fileEncoderConfig),
		lumberjackSink,
		instance.loggers[fileKey].level,
	)
	fileLoggerCore = zapcore.NewSamplerWithOptions(fileLoggerCore, time.Second, 10, 5)
	instance.loggers[fileKey].logger = zap.New(fileLoggerCore, zap.AddStacktrace(zap.ErrorLevel), zap.AddCaller(), zap.AddCallerSkip(1))

	s.started = true
	s.Unlock()
	s.Info(getTaskLogPrefix(taskName, "started"))
}

func (s *Singleton) Sync() {
	s.RLock()
	for _, logInstance := range s.loggers {
		_ = logInstance.logger.Sync()
	}
	s.RUnlock()
}

func (s *Singleton) StopTask() {
	s.Lock()
	if !s.started {
		// if not running do nothing
		s.Unlock()
		return
	}
	s.started = false
	s.Unlock()
	s.Sync()
	s.Info(getTaskLogPrefix(taskName, "stopped"))
}

func Instance() *Singleton {
	once.Do(func() {
		instance = &Singleton{
			loggers: make(map[string]*LogInstance),
			options: &Options{
				productNameShort: DefaultAppShortName,
			},
		}
	})
	return instance
}

func (s *Singleton) AddLogger(key string, w io.Writer, newLevel Level) {
	s.Lock()
	defer s.Unlock()

	// if a logger already exists at this key do nothing
	_, exists := instance.loggers[key]
	if exists {
		return
	}

	instance.loggers[key] = &LogInstance{
		enabled: atomic.NewBool(true),
	}
	instance.loggers[key].level = zap.NewAtomicLevelAt(zapcore.Level(newLevel))

	encoderConfig := zap.NewProductionEncoderConfig()
	encoderConfig.EncodeTime = func(t time.Time, enc zapcore.PrimitiveArrayEncoder) {
		enc.AppendString(t.UTC().Format(time.RFC3339Nano))
	}

	newloggerCore := zapcore.NewCore(
		zapcore.NewJSONEncoder(encoderConfig),
		zapcore.AddSync(w),
		instance.loggers[key].level,
	)
	newloggerCore = zapcore.NewSamplerWithOptions(newloggerCore, time.Second, 10, 5)
	instance.loggers[key].logger = zap.New(newloggerCore, zap.AddStacktrace(zap.ErrorLevel), zap.AddCaller(), zap.AddCallerSkip(1))
}

func (s *Singleton) SetLoggerEnabled(key string, enabled bool) {
	s.RLock()
	defer s.RUnlock()
	if s.loggers[key] != nil {
		s.loggers[key].enabled.Store(enabled)
	}
}

func (s *Singleton) SetConsoleLogging(enabled bool) {
	s.SetLoggerEnabled(consoleKey, enabled)
}

func (s *Singleton) SetFileLogging(enabled bool) {
	s.SetLoggerEnabled(fileKey, enabled)
}

func (s *Singleton) SetFileLogLevel(newLevel Level) {
	s.Lock()
	defer s.Unlock()
	if s.loggers[fileKey] != nil {
		s.loggers[fileKey].level.SetLevel(zapcore.Level(newLevel))
	}
}

func (s *Singleton) SetConsoleLogLevel(newLevel Level) {
	s.Lock()
	defer s.Unlock()
	if s.loggers[consoleKey] != nil {
		s.loggers[consoleKey].level.SetLevel(zapcore.Level(newLevel))
	}
}

func (s *Singleton) SetLogLevel(key string, newLevel Level) {
	s.Lock()
	defer s.Unlock()
	if s.loggers[key] != nil {
		s.loggers[key].level.SetLevel(zapcore.Level(newLevel))
	}
}

func (s *Singleton) Trace(msg string, fields ...Field) {
	s.RLock()
	defer s.RUnlock()
	for _, logInstance := range s.loggers {
		if logInstance.enabled.Load() {
			logInstance.logger.Debug(msg, fieldsToZapFields(fields...)...)
		}
	}
}

func (s *Singleton) Debug(msg string, fields ...Field) {
	s.RLock()
	defer s.RUnlock()
	for _, logInstance := range s.loggers {
		if logInstance.enabled.Load() {
			logInstance.logger.Debug(msg, fieldsToZapFields(fields...)...)
		}
	}
}

func (s *Singleton) Info(msg string, fields ...Field) {
	s.RLock()
	defer s.RUnlock()
	for _, logInstance := range s.loggers {
		if logInstance.enabled.Load() {
			logInstance.logger.Info(msg, fieldsToZapFields(fields...)...)
		}
	}
}

func (s *Singleton) InfoIgnoreCancel(ctx context.Context, msg string, fields ...Field) {
	if ctx.Err() != nil {
		return
	}
	s.Info(msg, fields...)
}

func (s *Singleton) Warn(msg string, fields ...Field) {
	s.RLock()
	defer s.RUnlock()
	for _, logInstance := range s.loggers {
		if logInstance.enabled.Load() {
			logInstance.logger.Warn(msg, fieldsToZapFields(fields...)...)
		}
	}
}

func (s *Singleton) WarnIgnoreCancel(ctx context.Context, msg string, fields ...Field) {
	if ctx.Err() != nil {
		return
	}
	s.Warn(msg, fields...)
}

func (s *Singleton) Error(msg string, fields ...Field) {
	s.RLock()
	defer s.RUnlock()
	for _, logInstance := range s.loggers {
		if logInstance.enabled.Load() {
			logInstance.logger.Error(msg, fieldsToZapFields(fields...)...)
		}
	}
}

func (s *Singleton) ErrorIgnoreCancel(ctx context.Context, msg string, fields ...Field) {
	if ctx.Err() != nil {
		return
	}
	s.Error(msg, fields...)
}

func (s *Singleton) Panic(msg string, fields ...Field) {
	s.RLock()
	defer s.RUnlock()
	var foundLogger bool
	for _, logInstance := range s.loggers {
		if logInstance.enabled.Load() {
			foundLogger = true
			logInstance.logger.Panic(msg, fieldsToZapFields(fields...)...)
		}
	}
	if !foundLogger {
		panic(msg)
	}
}

func (s *Singleton) DPanic(msg string, fields ...Field) {
	s.RLock()
	defer s.RUnlock()
	var foundLogger bool
	for _, logInstance := range s.loggers {
		if logInstance.enabled.Load() {
			foundLogger = true
			logInstance.logger.DPanic(msg, fieldsToZapFields(fields...)...)
		}
	}
	if !foundLogger {
		panic(msg)
	}
}

func (s *Singleton) Fatal(msg string, fields ...Field) {
	s.RLock()
	defer s.RUnlock()
	var foundLogger bool
	for _, logInstance := range s.loggers {
		if logInstance.enabled.Load() {
			foundLogger = true
			logInstance.logger.Fatal(msg, fieldsToZapFields(fields...)...)
		}
	}
	if !foundLogger {
		fmt.Println(msg)
		os.Exit(1)
	}
}

// Deprecated: use structured logging instead.
func (s *Singleton) TraceUnstruct(args ...interface{}) {
	s.RLock()
	defer s.RUnlock()
	for _, logInstance := range s.loggers {
		if logInstance.enabled.Load() {
			logInstance.logger.Sugar().Debug(args...)
		}
	}
}

// Deprecated: use structured logging instead.
func (s *Singleton) DebugUnstruct(args ...interface{}) {
	s.RLock()
	defer s.RUnlock()
	for _, logInstance := range s.loggers {
		if logInstance.enabled.Load() {
			logInstance.logger.Sugar().Debug(args...)
		}
	}
}

// Deprecated: use structured logging instead.
func (s *Singleton) InfoUnstruct(args ...interface{}) {
	s.RLock()
	defer s.RUnlock()
	for _, logInstance := range s.loggers {
		if logInstance.enabled.Load() {
			logInstance.logger.Sugar().Info(args...)
		}
	}
}

// Deprecated: use structured logging instead.
func (s *Singleton) WarnUnstruct(args ...interface{}) {
	s.RLock()
	defer s.RUnlock()
	for _, logInstance := range s.loggers {
		if logInstance.enabled.Load() {
			logInstance.logger.Sugar().Warn(args...)
		}
	}
}

// Deprecated: use structured logging instead.
func (s *Singleton) WarnIgnoreCancelUnstruct(ctx context.Context, args ...interface{}) {
	if ctx.Err() != nil {
		return
	}
	s.WarnUnstruct(args)
}

// Deprecated: use structured logging instead.
func (s *Singleton) ErrorUnstruct(args ...interface{}) {
	s.RLock()
	defer s.RUnlock()
	for _, logInstance := range s.loggers {
		if logInstance.enabled.Load() {
			logInstance.logger.Sugar().Error(args...)
		}
	}
}

// Deprecated: use structured logging instead.
func (s *Singleton) ErrorIgnoreCancelUnstruct(ctx context.Context, args ...interface{}) {
	if ctx.Err() != nil {
		return
	}
	s.ErrorUnstruct(args)
}

// Deprecated: use structured logging instead.
func (s *Singleton) PanicUnstruct(args ...interface{}) {
	s.RLock()
	defer s.RUnlock()
	var foundLogger bool
	for _, logInstance := range s.loggers {
		if logInstance.enabled.Load() {
			foundLogger = true
			logInstance.logger.Sugar().Panic(args...)
		}
	}
	if !foundLogger {
		if len(args) >= 1 {
			if str, ok := args[0].(string); ok {
				panic(str)
			}
		}
	}
}

// Deprecated: use structured logging instead.
func (s *Singleton) DPanicUnstruct(args ...interface{}) {
	s.RLock()
	defer s.RUnlock()
	var foundLogger bool
	for _, logInstance := range s.loggers {
		if logInstance.enabled.Load() {
			foundLogger = true
			logInstance.logger.Sugar().DPanic(args...)
		}
	}
	if !foundLogger {
		if len(args) >= 1 {
			if str, ok := args[0].(string); ok {
				panic(str)
			}
		}
	}
}

// Deprecated: use structured logging instead.
func (s *Singleton) FatalUnstruct(args ...interface{}) {
	s.RLock()
	defer s.RUnlock()
	var foundLogger bool
	for _, logInstance := range s.loggers {
		if logInstance.enabled.Load() {
			foundLogger = true
			logInstance.logger.Sugar().Fatal(args...)
		}
	}
	if !foundLogger {
		if len(args) >= 1 {
			if str, ok := args[0].(string); ok {
				fmt.Println(str)
			}
		}
		os.Exit(1)
	}
}

// Deprecated: use structured logging instead.
func (s *Singleton) TracefUnstruct(format string, args ...interface{}) {
	s.RLock()
	defer s.RUnlock()
	for _, logInstance := range s.loggers {
		if logInstance.enabled.Load() {
			logInstance.logger.Sugar().Debugf(format, args...)
		}
	}
}

// Deprecated: use structured logging instead.
func (s *Singleton) DebugfUnstruct(format string, args ...interface{}) {
	s.RLock()
	defer s.RUnlock()
	for _, logInstance := range s.loggers {
		if logInstance.enabled.Load() {
			logInstance.logger.Sugar().Debugf(format, args...)
		}
	}
}

// Deprecated: use structured logging instead.
func (s *Singleton) InfofUnstruct(format string, args ...interface{}) {
	s.RLock()
	defer s.RUnlock()
	for _, logInstance := range s.loggers {
		if logInstance.enabled.Load() {
			logInstance.logger.Sugar().Infof(format, args...)
		}
	}
}

// Deprecated: use structured logging instead.
func (s *Singleton) WarnfUnstruct(format string, args ...interface{}) {
	s.RLock()
	defer s.RUnlock()
	for _, logInstance := range s.loggers {
		if logInstance.enabled.Load() {
			logInstance.logger.Sugar().Warnf(format, args...)
		}
	}
}

// Deprecated: use structured logging instead.
func (s *Singleton) WarnfIgnoreCancelUnstruct(ctx context.Context, format string, args ...interface{}) {
	if ctx.Err() != nil {
		return
	}
	s.WarnfUnstruct(format, args)
}

// Deprecated: use structured logging instead.
func (s *Singleton) ErrorfUnstruct(format string, args ...interface{}) {
	s.RLock()
	defer s.RUnlock()
	for _, logInstance := range s.loggers {
		if logInstance.enabled.Load() {
			logInstance.logger.Sugar().Errorf(format, args...)
		}
	}
}

// Deprecated: use structured logging instead.
func (s *Singleton) ErrorfIgnoreCancelUnstruct(ctx context.Context, format string, args ...interface{}) {
	if ctx.Err() != nil {
		return
	}
	s.ErrorfUnstruct(format, args)
}

// Deprecated: use structured logging instead.
func (s *Singleton) PanicfUnstruct(format string, args ...interface{}) {
	s.RLock()
	defer s.RUnlock()
	var foundLogger bool
	for _, logInstance := range s.loggers {
		if logInstance.enabled.Load() {
			foundLogger = true
			logInstance.logger.Sugar().Panicf(format, args...)
		}
	}
	if !foundLogger {
		panic(fmt.Sprintf(format, args...))
	}
}

// Deprecated: use structured logging instead.
func (s *Singleton) DPanicfUnstruct(format string, args ...interface{}) {
	s.RLock()
	defer s.RUnlock()
	var foundLogger bool
	for _, logInstance := range s.loggers {
		if logInstance.enabled.Load() {
			foundLogger = true
			logInstance.logger.Sugar().DPanicf(format, args...)
		}
	}
	if !foundLogger {
		panic(fmt.Sprintf(format, args...))
	}
}

// Deprecated: use structured logging instead.
func (s *Singleton) FatalfUnstruct(format string, args ...interface{}) {
	s.RLock()
	defer s.RUnlock()
	var foundLogger bool
	for _, logInstance := range s.loggers {
		if logInstance.enabled.Load() {
			foundLogger = true
			logInstance.logger.Sugar().Fatalf(format, args...)
		}
	}
	if !foundLogger {
		fmt.Println(fmt.Sprintf(format, args...))
		os.Exit(1)
	}
}

// ErrorInLoggerWriter is used by log Writer sinks added with AddLogger() to log messages to standard console & file loggers
// that are enabled so the error in the logger can be trapped somewhere and without an error loop in the logger that triggered it
func (s *Singleton) ErrorInLoggerWriter(format string, args ...interface{}) {
	s.RLock()
	defer s.RUnlock()
	if s.loggers[fileKey] != nil && s.loggers[fileKey].enabled.Load() {
		s.loggers[fileKey].logger.Sugar().Errorf(format, args...)
	}
	if s.loggers[consoleKey] != nil && s.loggers[consoleKey].enabled.Load() {
		s.loggers[consoleKey].logger.Sugar().Errorf(format, args...)
	}
}

func (s *Singleton) ErrorInLoggerWriterIgnoreCancel(ctx context.Context, format string, args ...interface{}) {
	if ctx.Err() != nil {
		return
	}
	s.ErrorInLoggerWriter(format, args)
}

func (s *Singleton) IsLevelEnabled(level Level) bool {
	s.RLock()
	defer s.RUnlock()
	for _, logInstance := range s.loggers {
		if level >= Level(logInstance.level.Level()) {
			return true
		}
	}
	return false
}

// IsInterruptError returns whether an error was returned by a process that
// was terminated by an interrupt signal (SIGINT).
//func IsInterruptError(err error) bool {
//	exitErr, ok := err.(*exec.ExitError)
//	if !ok || exitErr.ExitCode() >= 0 {
//		return false
//	}
//	status := exitErr.Sys().(syscall.WaitStatus)
//	return status.Signal() == syscall.SIGINT
//}

func fieldsToZapFields(fields ...Field) []zap.Field {
	var zapFields []zap.Field
	for _, f := range fields {
		zapFields = append(zapFields, zap.Field(f))
	}
	return zapFields
}

func getTaskLogPrefix(taskName string, format string) string {
	var sb strings.Builder
	sb.WriteString("[")
	sb.WriteString(taskName)
	sb.WriteString("] ")
	sb.WriteString(format)
	return sb.String()
}
