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
	"fmt"

	"go.uber.org/zap"
)

func Run(appName string, initFunc, jobFunc, cleanupFunc func() error) {
	Logger.Info(appName + " initialization")
	if err := initFunc(); err != nil {
		Logger.Fatal(appName+" initialization failed", zap.Error(err))
	}
	Logger.Info(appName + " initialization completed")
	go func() {
		if err := jobFunc(); err != nil {
			Logger.Fatal(appName+" run failed", zap.Error(err))
		}
	}()

	s := WaitForExitSign()
	Logger.Info(fmt.Sprintf("%s got the exit signal %s, start to clean", appName, s))
	if err := cleanupFunc(); err != nil {
		Logger.Fatal(appName+" clean failed", zap.Error(err))
	}
	Logger.Info(appName + " clean completed, exit")
}
