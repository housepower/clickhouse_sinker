package main

import (
	"github.com/bytedance/sonic"
)

func JSONMarshal(obj interface{}) ([]byte, error) {
	return sonic.Marshal(obj)
}
