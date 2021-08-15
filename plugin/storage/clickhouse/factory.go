// Copyright (c) 2019 The Jaeger Authors.
// Copyright (c) 2017 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package clickhouse

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/spf13/viper"
	"github.com/uber/jaeger-lib/metrics"
	"go.uber.org/zap"

	"github.com/jaegertracing/jaeger/storage/dependencystore"
	"github.com/jaegertracing/jaeger/storage/spanstore"
)

// Factory implements storage.Factory and creates storage components backed by clickhouse store.
type Factory struct {
	options        Options
	metricsFactory metrics.Factory
	logger         *zap.Logger

	store *Store
}

// NewFactory creates a new Factory.
func NewFactory() *Factory {
	return &Factory{}
}

// AddFlags implements plugin.Configurable
func (f *Factory) AddFlags(flagSet *flag.FlagSet) {
	AddFlags(flagSet)
}

// InitFromViper implements plugin.Configurable
func (f *Factory) InitFromViper(v *viper.Viper, logger *zap.Logger) {
	f.options.InitFromViper(v)
}

// InitFromOptions initializes factory from the supplied options
func (f *Factory) InitFromOptions(opts Options) {
	f.options = opts
}

// Initialize implements storage.Factory
func (f *Factory) Initialize(metricsFactory metrics.Factory, logger *zap.Logger) error {
	f.metricsFactory, f.logger = metricsFactory, logger
	f.store = WithConfiguration(f.options.Configuration, f.logger)
	logger.Info("Clickhouse storage initialized", zap.Any("configuration", f.store.config))
	// trap SIGINT to trigger a shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	ticker := time.NewTicker(time.Second)
	go func() {
		for {
			select {
			case t := <-ticker.C:

				f.store.rowsChan <- RowsExecuteMessage{
					Action:    TimeTickerAction,
					Key:       "",
					Row:       nil,
					Timestamp: t,
				}

			case <-signals:
				ticker.Stop()
				f.store.rowsChan <- RowsExecuteMessage{
					Action:    ExecuteAction,
					Key:       "",
					Row:       nil,
					Timestamp: time.Now(),
				}
				f.store.logger.Info("😭 collector clickhouse say bye!")
				return
			}
		}
	}()

	go func() {
		for rowsMessage := range f.store.rowsChan {
			switch rowsMessage.Action {
			case AddAction:
				rows, ok := f.store.rowsMap[rowsMessage.Key]
				if !ok {
					rows = [][]interface{}{}
				}

				if len(rows) >= f.store.config.MaxCommitCount {
					go f.store.MultiInsertClickHouse(rowsMessage.Key, rows)
					rows = [][]interface{}{}
					f.store.lastCommitMap[rowsMessage.Key] = time.Now()
				}

				rows = append(rows, rowsMessage.Row)
				f.store.rowsMap[rowsMessage.Key] = rows
			case ExecuteAction:

				for key, rows := range f.store.rowsMap {
					if len(rows) >= 0 {
						go f.store.MultiInsertClickHouse(key, rows)
						f.store.logger.Info(fmt.Sprintf("%s  rows_count : %d execute\n", key, len(rows)))
						rows = [][]interface{}{}
						f.store.lastCommitMap[key] = time.Now()
					}
				}

			case TimeTickerAction:

				t := rowsMessage.Timestamp
				for key, rows := range f.store.rowsMap {
					if last, ok := f.store.lastCommitMap[key]; ok && len(rows) > 0 && (t.Unix()-last.Unix()) >= f.store.config.MaxCommitTime {
						go f.store.MultiInsertClickHouse(key, rows)
						f.store.logger.Info(fmt.Sprintf("%s  rows_count : %d time ticker : %s last : %s\n", key, len(rows), t.In(f.store.TimeZone).Format(f.store.YYMMDDHHIISSFormat), last.In(f.store.TimeZone).Format(f.store.YYMMDDHHIISSFormat)))
						rows = [][]interface{}{}
						f.store.lastCommitMap[key] = time.Now()
					} else if !ok && len(rows) > 0 {
						go f.store.MultiInsertClickHouse(key, rows)
						f.store.logger.Info(fmt.Sprintf("%s  rows_count : %d time ticker : %s last : %s\n", key, len(rows), t.In(f.store.TimeZone).Format(f.store.YYMMDDHHIISSFormat), last.In(f.store.TimeZone).Format(f.store.YYMMDDHHIISSFormat)))
						rows = [][]interface{}{}
						f.store.lastCommitMap[key] = time.Now()
					}
				}
			}
		}
	}()

	// consume partitions
	f.store.logger.Info("😊 collector clickhouse say hello!")
	return nil
}

// CreateSpanReader implements storage.Factory
func (f *Factory) CreateSpanReader() (spanstore.Reader, error) {
	return f.store, nil
}

// CreateSpanWriter implements storage.Factory
func (f *Factory) CreateSpanWriter() (spanstore.Writer, error) {
	return f.store, nil
}

// CreateDependencyReader implements storage.Factory
func (f *Factory) CreateDependencyReader() (dependencystore.Reader, error) {
	return f.store, nil
}
