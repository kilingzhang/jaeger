// Copyright (c) 2018 The Jaeger Authors.
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

	"github.com/spf13/viper"

	"github.com/jaegertracing/jaeger/pkg/clickhouse/config"
)

const dataSourceName = "clickhouse.data_source_name"
const maxCommitCount = "clickhouse.max_commit_count"
const maxCommitTime = "clickhouse.max_commit_time"

// Options stores the configuration entries for this storage
type Options struct {
	Configuration config.Configuration `mapstructure:",squash"`
}

// AddFlags from this storage to the CLI
func AddFlags(flagSet *flag.FlagSet) {
	flagSet.String(dataSourceName, "", "data source name")
	flagSet.Int(maxCommitCount, 10000, "max commit count")
	flagSet.Int64(maxCommitTime, 60, "max commit time")
}

// InitFromViper initializes the options struct with values from Viper
func (opt *Options) InitFromViper(v *viper.Viper) {
	opt.Configuration.DataSourceName = v.GetString(dataSourceName)
	opt.Configuration.MaxCommitCount = v.GetInt(maxCommitCount)
	opt.Configuration.MaxCommitTime = v.GetInt64(maxCommitTime)
}
