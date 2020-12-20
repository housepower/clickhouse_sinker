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
	"log"
)

func Run(appName string, initFunc, jobFunc, cleanupFunc func() error) {
	log.Printf("Initial [%s]", appName)
	if err := initFunc(); err != nil {
		log.Printf("Initial [%s] failure: [%s]", appName, err)
		panic(err)
	}
	log.Printf("Initial [%s] complete", appName)
	go func() {
		if err := jobFunc(); err != nil {
			log.Printf("[%s] run error: [%v]", appName, err)
			panic(err)
		}
	}()

	WaitForExitSign()
	log.Printf("[%s] watched the exit signal, start to clean", appName)
	if err := cleanupFunc(); err != nil {
		log.Printf("[%s] clean failed: [%v]", appName, err)
		panic(err)
	}
	log.Printf("[%s] clean complete, exited", appName)
}

func Funcs(funcs ...func() error) func() error {
	return func() error {
		for _, fun := range funcs {
			if err := fun(); err != nil {
				return err
			}
		}
		return nil
	}
}
func LogWrapper(msg string, fun func() error) func() error {
	return func() error {
		log.Println(msg + " start")
		if err := fun(); err != nil {
			log.Printf("%s failed: %v", msg, err)
			return err
		}
		log.Println(msg + " success")
		return nil
	}
}
