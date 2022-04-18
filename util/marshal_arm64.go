package util

import (
	"encoding/json"

	"github.com/pkg/errors"
)

func JsonMarshal(obj interface{}) (b []byte, err error) {
	if b, err = json.Marshal(obj); err != nil {
		err = errors.Wrapf(err, "")
	}
	return
}
