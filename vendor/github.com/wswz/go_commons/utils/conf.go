package utils

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

const extendTag = "@extend:"

func ExtendFile(filePath string) (string, error) {
	fi, err := os.Stat(filePath)
	if err != nil {
		return "", err
	} else if fi.IsDir() {
		return "", errors.New(filePath + " is not a file.")
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
			err = fmt.Errorf("替换json配置[%s]失败：%s\n", match, err2.Error())
		}
		return string(sb)
	})
	return ret, err
}
