package util

import (
	"github.com/bytedance/sonic"

	"github.com/thanos-io/thanos/pkg/errors"
)

func JSONMarshal(obj interface{}) (b []byte, err error) {
	if b, err = sonic.Marshal(obj); err != nil {
		err = errors.Wrapf(err, "")
	}
	return
}
