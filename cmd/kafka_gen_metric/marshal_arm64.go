package main

import (
	"encoding/json"
)

func JSONMarshal(obj interface{}) ([]byte, error) {
	return json.Marshal(obj)
}
