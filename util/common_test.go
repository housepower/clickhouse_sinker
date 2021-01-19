package util

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestJksToPem(t *testing.T) {
	var err error
	var certPemPath, keyPemPath string
	jksPaths := []string{"kafka.client.truststore.jks", "kafka.client.keystore.jks"}
	jksPassword := "123456"
	for _, jksPath := range jksPaths {
		if _, err = os.Stat(jksPath); err != nil {
			continue
		}
		certPemPath, keyPemPath, err = JksToPem(jksPath, jksPassword, true)
		require.Nil(t, err, "err should be nothing")
		fmt.Printf("converted %s to %s, %s\n", jksPath, certPemPath, keyPemPath)
	}
}
