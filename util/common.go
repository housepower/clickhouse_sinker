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

package util

import (
	"strings"
	"time"

	"github.com/fagongzi/goetty"
)

var (
	GlobalTimerWheel  *goetty.TimeoutWheel //the global timer wheel
	GlobalWorkerPool1 *WorkerPool          //the global worker pool for cpu intensive works
	GlobalWorkerPool2 *WorkerPool          //the global worker pool for network intensive works
)

// InitGlobalTimerWheel initialize the global timer wheel
func InitGlobalTimerWheel() {
	GlobalTimerWheel = goetty.NewTimeoutWheel(goetty.WithTickInterval(time.Second))
}

// InitGlobalWorkerPool1 initialize GlobalWorkerPool1
func InitGlobalWorkerPool1(maxWorkers int) {
	GlobalWorkerPool1 = NewWorkerPool(maxWorkers, 10*maxWorkers)
}

// InitGlobalWorkerPool2 initialize GlobalWorkerPool2
func InitGlobalWorkerPool2(maxWorkers int) {
	GlobalWorkerPool2 = NewWorkerPool(maxWorkers, 10*maxWorkers)
}

// StringContains check if contains string in array
func StringContains(arr []string, str string) bool {
	for _, s := range arr {
		if s == str {
			return true
		}
	}
	return false
}

// GetSourceName returns the field name in message for the given ClickHouse column
func GetSourceName(name string) (sourcename string) {
	sourcename = strings.Replace(name, ".", "\\.", -1)
	if strings.HasPrefix(sourcename, "_") && !strings.HasPrefix(sourcename, "__") {
		sourcename = "@" + sourcename[1:]
	}
	return
}

// GetShift returns the smallest `shift` which 1<<shift is no smaller than s
func GetShift(s int) (shift int) {
	for shift = 0; (1 << shift) < s; shift++ {
	}
	return
}
