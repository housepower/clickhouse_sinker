package util

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// keytool -genkeypair -alias certificatekey -dname "CN=hello world, OU=eoi, O=eoi, L=shanghai, ST=shanghai, C=CN" -keyalg RSA -validity 3650 -keystore kafka.client.keystore.jks
// keytool -export -alias certificatekey -keystore kafka.client.keystore.jks  -rfc -file selfsignedcert.cer
// keytool -import -alias certificatekey -file selfsignedcert.cer  -keystore kafka.client.truststore.jks
func TestJksToPem(t *testing.T) {
	var err error
	var certPemPath, keyPemPath string
	jksPaths := []string{"../test/kafka.client.truststore.jks", "../test/kafka.client.keystore.jks"}
	jksPassword := "HelloWorld"
	for _, jksPath := range jksPaths {
		if _, err = os.Stat(jksPath); err != nil {
			require.Nil(t, err)
		}
		certPemPath, keyPemPath, err = JksToPem(jksPath, jksPassword, true)
		require.Nil(t, err, "err should be nothing")
		t.Logf("converted %s to %s, %s\n", jksPath, certPemPath, keyPemPath)
	}
}
