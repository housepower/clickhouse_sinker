package util

import (
	"encoding/json"
)

// 将config的配置注入到entity中
func IngestConfig(config interface{}, entity interface{}) {
	bs, _ := json.Marshal(config)
	json.Unmarshal(bs, entity)
}
