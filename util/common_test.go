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

func TestStringContains(t *testing.T) {
	tests := []struct {
		name   string
		array  []string
		result bool
	}{
		{
			name:   "false",
			array:  []string{""},
			result: false,
		},
		{
			name:   "true",
			array:  []string{"true", "hi"},
			result: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := StringContains(tt.array, tt.name)
			require.Equal(t, result, tt.result)
		})
	}
}

func TestGetSourceName(t *testing.T) {
	tests := []struct {
		parser, name, result string
	}{
		{
			parser: "gjson",
			name:   "a.b.c",
			result: "a\\.b\\.c",
		},
		{
			parser: "csv",
			name:   "a.b.c",
			result: "a.b.c",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := GetSourceName(tt.parser, tt.name)
			require.Equal(t, result, tt.result)
		})
	}
}

func TestMisc(t *testing.T) {
	require.Equal(t, uint(6), GetShift(64))
	require.Equal(t, uint(7), GetShift(65))

	tests := []struct {
		parser, name, result string
	}{
		{
			parser: "gjson",
			name:   "a.b.c",
			result: "a\\.b\\.c",
		},
		{
			parser: "csv",
			name:   "a.b.c",
			result: "a.b.c",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := GetSourceName(tt.parser, tt.name)
			require.Equal(t, result, tt.result)
		})
	}
}

func TestReplaceTemplateString(t *testing.T) {
	replace := map[string]interface{}{
		"a": "111",
		"b": "2222",
		"c": "3333",
	}
	src := `[{{.a}}] is [{{.b}}], [{{.a}}] is [{{.c}}]`
	err := ReplaceTemplateString(&src, replace)
	require.Nil(t, err)
	t.Log(src)
	templ := map[string]interface{}{
		"metricTable":       "default.metric",
		"seriesTable":       "default.metric_series",
		"activeSeriesRange": 3600,
	}

	src = `WITH (SELECT max(timestamp) FROM {{.metricTable}}) AS m
	SELECT DISTINCT count() FROM {{.seriesTable}} WHERE __series_id GLOBAL IN (
	SELECT DISTINCT __series_id FROM {{.metricTable}} WHERE timestamp >= addSeconds(m, -{{.activeSeriesRange}}));`
	err = ReplaceTemplateString(&src, templ)
	require.Nil(t, err)
	t.Log(src)
}
