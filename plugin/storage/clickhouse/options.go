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
	"time"

	"github.com/spf13/viper"

	"github.com/jaegertracing/jaeger/pkg/clickhouse/config"
)

const suffixDataSource = "clickhouse.data-source"
const suffixMaxCommitCount = "clickhouse.max-commit-count"
const suffixMaxCommitTime = "clickhouse.max-commit-time"
const suffixMaxSpanAge = "clickhouse.max-span-age"
const suffixTimeZone = "clickhouse.timezone"

// Options stores the configuration entries for this storage
type Options struct {
	Configuration config.Configuration `mapstructure:",squash"`
}

// AddFlags from this storage to the CLI
func AddFlags(flagSet *flag.FlagSet) {
	flagSet.String(suffixDataSource, "", "data source name")
	flagSet.Int(suffixMaxCommitCount, 10000, "max commit count")
	flagSet.Duration(suffixMaxCommitTime, time.Duration(time.Second*60), "max commit commit")
	flagSet.Duration(suffixMaxSpanAge, time.Duration(time.Second*60*60*24*7), "max span age")
	flagSet.String(suffixTimeZone, "Asia/Shanghai", "timezone")
}

// InitFromViper initializes the options struct with values from Viper
func (opt *Options) InitFromViper(v *viper.Viper) {
	opt.Configuration.DataSourceName = v.GetString(suffixDataSource)
	opt.Configuration.MaxCommitCount = v.GetInt(suffixMaxCommitCount)
	opt.Configuration.MaxCommitTime = v.GetDuration(suffixMaxCommitTime)
	opt.Configuration.MaxSpanAge = v.GetDuration(suffixMaxSpanAge)
	opt.Configuration.TimeZone = v.GetString(suffixTimeZone)
}
