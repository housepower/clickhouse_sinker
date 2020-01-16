/*Copyright [2019] housepower

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package creator

import (
	"github.com/housepower/clickhouse_sinker/input"
	"github.com/housepower/clickhouse_sinker/output"
	"github.com/housepower/clickhouse_sinker/parser"
	"github.com/housepower/clickhouse_sinker/task"
	"github.com/housepower/clickhouse_sinker/util"
)

// GenTasks generate the tasks via config
func (config *Config) GenTasks() []*task.Service {
	res := make([]*task.Service, 0, len(config.Tasks))
	for _, taskConfig := range config.Tasks {
		kafka := config.GenInput(taskConfig)
		ck := config.GenOutput(taskConfig)
		p := parser.NewParser(taskConfig.Parser, taskConfig.CsvFormat, taskConfig.Delimiter)

		taskImpl := task.NewTaskService(kafka, ck, p)

		util.IngestConfig(taskConfig, taskImpl)

		if taskImpl.FlushInterval == 0 {
			taskImpl.FlushInterval = config.Common.FlushInterval
		}

		if taskImpl.BufferSize == 0 {
			taskImpl.BufferSize = config.Common.BufferSize
		}

		if taskImpl.MinBufferSize == 0 {
			taskImpl.MinBufferSize = config.Common.MinBufferSize
		}

		res = append(res, taskImpl)
	}
	return res
}

// GenInput generate the input via config
func (config *Config) GenInput(taskCfg *Task) *input.Kafka {
	kfkCfg := config.Kafka[taskCfg.Kafka]

	inputImpl := input.NewKafka()
	util.IngestConfig(taskCfg, inputImpl)
	util.IngestConfig(kfkCfg, inputImpl)
	return inputImpl
}

// GenOutput generate the output via config
func (config *Config) GenOutput(taskCfg *Task) *output.ClickHouse {
	ckCfg := config.Clickhouse[taskCfg.Clickhouse]

	outputImpl := output.NewClickHouse()

	util.IngestConfig(ckCfg, outputImpl)
	util.IngestConfig(taskCfg, outputImpl)
	util.IngestConfig(config.Common, outputImpl)
	return outputImpl
}
