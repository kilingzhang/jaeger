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

package config

// Configuration describes the options to customize the storage behavior
type Configuration struct {
	DataSourceName string `yaml:"data_source_name" mapstructure:"data_source_name"`
	MaxCommitCount int    `yaml:"max_commit_count" mapstructure:"max_commit_count"`
	MaxCommitTime  int64  `yaml:"max_commit_time" mapstructure:"max_commit_count"`
}
