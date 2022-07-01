package util

import (
	"encoding/json"

	"github.com/thanos-io/thanos/pkg/errors"
)

func JSONMarshal(obj interface{}) (b []byte, err error) {
	if b, err = json.Marshal(obj); err != nil {
		err = errors.Wrapf(err, "")
	}
	return
}
