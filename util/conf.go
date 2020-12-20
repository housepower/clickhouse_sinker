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
	"encoding/json"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/pkg/errors"
)

const extendTag = "@extend:"

func ExtendFile(filePath string) (string, error) {
	fi, err := os.Stat(filePath)
	if err != nil {
		return "", err
	} else if fi.IsDir() {
		return "", errors.New("error in ExtendFile, " + filePath + " is not a file")
	}
	var b []byte
	b, err = ioutil.ReadFile(filePath)
	if err != nil {
		return "", err
	}
	dir, err := filepath.Abs(filepath.Dir(filePath))
	if err != nil {
		return "", err
	}
	return ExtendFileContent(dir, b)
}

func ExtendFileContent(dir string, content []byte) (string, error) {
	//检查是不是规范的json
	test := new(interface{})
	err := json.Unmarshal(content, &test)
	if err != nil {
		return "", err
	}

	//替换子json文件
	reg := regexp.MustCompile(`"` + extendTag + `.*?"`)
	ret := reg.ReplaceAllStringFunc(string(content), func(match string) string {
		match = match[len(extendTag)+1 : len(match)-1]
		var p = match
		if !strings.HasPrefix(match, "/") {
			p = dir + "/" + match
		}
		sb, err2 := ExtendFile(p)
		if err2 != nil {
			err = errors.Wrapf(err2, "replace json config [%s]failed", match)
		}
		return sb
	})
	return ret, err
}
