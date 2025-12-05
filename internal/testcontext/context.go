// Copyright (c) 2025 Palantir Technologies. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package testcontext

import (
	"context"
	"os"
	"testing"

	"github.com/palantir/pkg/metrics"
	"github.com/palantir/witchcraft-go-logging/wlog"
	"github.com/palantir/witchcraft-go-logging/wlog/diaglog/diag1log"
	"github.com/palantir/witchcraft-go-logging/wlog/evtlog/evt2log"
	"github.com/palantir/witchcraft-go-logging/wlog/metriclog/metric1log"

	// Use zap as logger implementation
	_ "github.com/palantir/witchcraft-go-logging/wlog-zap"
	"github.com/palantir/witchcraft-go-logging/wlog/svclog/svc1log"
	"github.com/palantir/witchcraft-go-logging/wlog/trclog/trc1log"
	"github.com/palantir/witchcraft-go-tracing/wtracing"
	"github.com/palantir/witchcraft-go-tracing/wzipkin"
	"github.com/stretchr/testify/require"
)

// GetTestContext provides context for local unit testing purpose
func GetTestContext(tb testing.TB) context.Context {
	ctx := context.Background()
	ctx = svc1log.WithLogger(ctx, svc1log.New(os.Stdout, wlog.DebugLevel))
	ctx = svc1log.WithLoggerParams(ctx, svc1log.OriginFromCallLine())
	ctx = evt2log.WithLogger(ctx, evt2log.New(os.Stdout))
	ctx = metric1log.WithLogger(ctx, metric1log.New(os.Stdout))
	registry := metrics.NewRootMetricsRegistry()
	ctx = metrics.WithRegistry(ctx, registry)
	ctx = diag1log.WithLogger(ctx, diag1log.New(os.Stdout))
	traceLogger := trc1log.DefaultLogger()
	ctx = trc1log.WithLogger(ctx, traceLogger)
	tracer, err := wzipkin.NewTracer(traceLogger)
	require.NoError(tb, err)
	ctx = wtracing.ContextWithTracer(ctx, tracer)
	return ctx
}
