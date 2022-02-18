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

package task

import (
	"context"
	"github.com/davidwartell/go-commons-drw/logger"
	"runtime/debug"
	"strings"
)

type Task interface {
	StartTask()
	StopTask()
}

type BaseTask struct{}

func LogTraceStruct(taskName string, msg string, fields ...logger.Field) {
	fieldsWithService := append(fields, logger.String("task", taskName))
	logger.Instance().Trace(getTaskLogPrefix(taskName, msg), fieldsWithService...)
}

func LogDebugStruct(taskName string, msg string, fields ...logger.Field) {
	fieldsWithService := append(fields, logger.String("task", taskName))
	logger.Instance().Debug(getTaskLogPrefix(taskName, msg), fieldsWithService...)
}

func LogInfoStruct(taskName string, msg string, fields ...logger.Field) {
	fieldsWithService := append(fields, logger.String("task", taskName))
	logger.Instance().Info(getTaskLogPrefix(taskName, msg), fieldsWithService...)
}

func LogWarnStruct(taskName string, msg string, fields ...logger.Field) {
	fieldsWithService := append(fields, logger.String("task", taskName))
	logger.Instance().Warn(getTaskLogPrefix(taskName, msg), fieldsWithService...)
}

func LogWarnStructIgnoreCancel(ctx context.Context, taskName string, msg string, fields ...logger.Field) {
	fieldsWithService := append(fields, logger.String("task", taskName))
	logger.Instance().WarnIgnoreCancel(ctx, getTaskLogPrefix(taskName, msg), fieldsWithService...)
}

func LogErrorStruct(taskName string, msg string, fields ...logger.Field) {
	fieldsWithService := append(fields, logger.String("task", taskName))
	logger.Instance().Error(getTaskLogPrefix(taskName, msg), fieldsWithService...)
}

func LogErrorStructIgnoreCancel(ctx context.Context, taskName string, msg string, fields ...logger.Field) {
	fieldsWithService := append(fields, logger.String("task", taskName))
	logger.Instance().ErrorIgnoreCancel(ctx, getTaskLogPrefix(taskName, msg), fieldsWithService...)
}

// LogTracef Deprecated
func LogTracef(taskName string, format string, args ...interface{}) {
	logger.Instance().TracefUnstruct(getTaskLogPrefix(taskName, format), args...)
}

// LogTrace Deprecated
func LogTrace(taskName string, msg string) {
	logger.Instance().TraceUnstruct(getTaskLogPrefix(taskName, msg))
}

// LogDebugf Deprecated
func LogDebugf(taskName string, format string, args ...interface{}) {
	logger.Instance().DebugfUnstruct(getTaskLogPrefix(taskName, format), args...)
}

// LogDebug Deprecated
//goland:noinspection GoUnusedExportedFunction
func LogDebug(taskName string, msg string) {
	logger.Instance().DebugUnstruct(getTaskLogPrefix(taskName, msg))
}

// LogInfof Deprecated
func LogInfof(taskName string, format string, args ...interface{}) {
	logger.Instance().InfofUnstruct(getTaskLogPrefix(taskName, format), args...)
}

// LogInfo Deprecated
func LogInfo(taskName string, msg string) {
	logger.Instance().InfoUnstruct(getTaskLogPrefix(taskName, msg))
}

// LogErrorf Deprecated
func LogErrorf(taskName string, format string, args ...interface{}) {
	logger.Instance().ErrorfUnstruct(getTaskLogPrefix(taskName, format), args...)
}

// LogError Deprecated
func LogError(taskName string, msg string) {
	logger.Instance().ErrorUnstruct(getTaskLogPrefix(taskName, msg))
}

// LogErrorfIgnoreCancel Deprecated
func LogErrorfIgnoreCancel(ctx context.Context, taskName string, format string, args ...interface{}) {
	logger.Instance().ErrorfIgnoreCancelUnstruct(ctx, getTaskLogPrefix(taskName, format), args...)
}

// LogErrorIgnoreCancel Deprecated
//goland:noinspection GoUnusedExportedFunction
func LogErrorIgnoreCancel(ctx context.Context, taskName string, msg string) {
	logger.Instance().ErrorIgnoreCancelUnstruct(ctx, getTaskLogPrefix(taskName, msg))
}

// LogWarnf Deprecated
func LogWarnf(taskName string, format string, args ...interface{}) {
	logger.Instance().WarnfUnstruct(getTaskLogPrefix(taskName, format), args...)
}

// LogWarn Deprecated
//goland:noinspection GoUnusedExportedFunction
func LogWarn(taskName string, msg string) {
	logger.Instance().WarnUnstruct(getTaskLogPrefix(taskName, msg))
}

func HandlePanic(taskName string) {
	if err := recover(); err != nil {
		LogErrorf(taskName, "panic occurred: %v stacktrace from panic: %s", err, string(debug.Stack()))
	}
}

func getTaskLogPrefix(taskName string, format string) string {
	var sb strings.Builder
	sb.WriteString("[")
	sb.WriteString(taskName)
	sb.WriteString("] ")
	sb.WriteString(format)
	return sb.String()
}
