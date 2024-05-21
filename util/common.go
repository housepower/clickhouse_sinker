/*Copyright [2019] housepower

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package util

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"

	"github.com/google/uuid"
	"github.com/thanos-io/thanos/pkg/errors"
)

var (
	Logger       *zap.Logger
	logAtomLevel zap.AtomicLevel
	logPaths     []string
	logTrace     bool
)

type CmdOptions struct {
	ShowVer  bool
	LogLevel string // "debug", "info", "warn", "error", "dpanic", "panic", "fatal"
	LogPaths string // comma-separated paths. "stdout" means the console stdout

	// HTTPHost to bind to. If empty, outbound ip of machine
	// is automatically determined and used.
	HTTPHost string
	HTTPPort int // 0 means a randomly chosen port.

	PushGatewayAddrs string
	PushInterval     int
	LocalCfgFile     string
	NacosAddr        string
	NacosNamespaceID string
	NacosGroup       string
	NacosUsername    string
	NacosPassword    string
	NacosDataID      string
	NacosServiceName string // participate in assignment management if not empty

	Credentials
}

type Credentials struct {
	ClickhouseUsername  string
	ClickhousePassword  string
	KafkaUsername       string
	KafkaPassword       string
	KafkaGSSAPIUsername string
	KafkaGSSAPIPassword string
}

// StringContains check if contains string in array
func StringContains(arr []string, str string) bool {
	for _, s := range arr {
		if s == str {
			return true
		}
	}
	return false
}

// GetSourceName returns the field name in message for the given ClickHouse column
func GetSourceName(parser, name string) (sourcename string) {
	if parser == "gjson" {
		sourcename = strings.Replace(name, ".", "\\.", -1)
	} else {
		sourcename = name
	}
	return
}

// GetShift returns the smallest `shift` which 1<<shift is no smaller than s
func GetShift(s int) (shift uint) {
	for shift = 0; (1 << shift) < s; shift++ {
	}
	return
}

// Refers to:
// https://medium.com/processone/using-tls-authentication-for-your-go-kafka-client-3c5841f2a625
// https://github.com/denji/golang-tls
// https://www.baeldung.com/java-keystore-truststore-difference
func NewTLSConfig(caCertFiles, clientCertFile, clientKeyFile string, insecureSkipVerify bool) (*tls.Config, error) {
	tlsConfig := tls.Config{}
	// Load client cert
	if clientCertFile != "" && clientKeyFile != "" {
		cert, err := tls.LoadX509KeyPair(clientCertFile, clientKeyFile)
		if err != nil {
			err = errors.Wrapf(err, "")
			return &tlsConfig, err
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	// Load CA cert if it exists.  Not needed for OS trusted certs
	if caCertFiles != "" {
		caCertPool := x509.NewCertPool()
		for _, caCertFile := range strings.Split(caCertFiles, ",") {
			caCert, err := os.ReadFile(caCertFile)
			if err != nil {
				err = errors.Wrapf(err, "")
				return &tlsConfig, err
			}
			caCertPool.AppendCertsFromPEM(caCert)
		}
		tlsConfig.RootCAs = caCertPool
	}
	tlsConfig.InsecureSkipVerify = insecureSkipVerify
	return &tlsConfig, nil
}

func EnvStringVar(value *string, key string) {
	realKey := strings.ReplaceAll(strings.ToUpper(key), "-", "_")
	val, found := os.LookupEnv(realKey)
	if found {
		*value = val
	}
}

func EnvIntVar(value *int, key string) {
	realKey := strings.ReplaceAll(strings.ToUpper(key), "-", "_")
	val, found := os.LookupEnv(realKey)
	if found {
		valInt, err := strconv.Atoi(val)
		if err == nil {
			*value = valInt
		}
	}
}

func EnvBoolVar(value *bool, key string) {
	realKey := strings.ReplaceAll(strings.ToUpper(key), "-", "_")
	if _, found := os.LookupEnv(realKey); found {
		*value = true
	}
}

// JksToPem converts JKS to PEM
// Refers to:
// https://serverfault.com/questions/715827/how-to-generate-key-and-crt-file-from-jks-file-for-httpd-apache-server
func JksToPem(jksPath, jksPassword string, overwrite bool) (certPemPath, keyPemPath string, err error) {
	dir, fn := filepath.Split(jksPath)
	certPemPath = filepath.Join(dir, fn+".cert.pem")
	keyPemPath = filepath.Join(dir, fn+".key.pem")
	pkcs12Path := filepath.Join(dir, fn+".p12")
	if overwrite {
		for _, fp := range []string{certPemPath, keyPemPath, pkcs12Path} {
			if err = os.RemoveAll(fp); err != nil {
				err = errors.Wrapf(err, "")
				return
			}
		}
	} else {
		for _, fp := range []string{certPemPath, keyPemPath, pkcs12Path} {
			if _, err = os.Stat(fp); err == nil {
				return
			}
		}
	}
	cmds := [][]string{
		{"keytool", "-importkeystore", "-srckeystore", jksPath, "-destkeystore", pkcs12Path, "-deststoretype", "PKCS12"},
		{"openssl", "pkcs12", "-in", pkcs12Path, "-nokeys", "-out", certPemPath, "-passin", "env:password"},
		{"openssl", "pkcs12", "-in", pkcs12Path, "-nodes", "-nocerts", "-out", keyPemPath, "-passin", "env:password"},
	}
	for _, cmd := range cmds {
		if Logger != nil {
			Logger.Info(strings.Join(cmd, " "))
		}
		exe := exec.Command(cmd[0], cmd[1:]...)
		if cmd[0] == "keytool" {
			exe.Stdin = bytes.NewReader([]byte(jksPassword + "\n" + jksPassword + "\n" + jksPassword))
		} else if cmd[0] == "openssl" {
			exe.Env = []string{fmt.Sprintf("password=%s", jksPassword)}
		}
		var out []byte
		out, err = exe.CombinedOutput()
		if Logger != nil {
			Logger.Info(string(out))
		}
		if err != nil {
			err = errors.Wrapf(err, "")
			return
		}
	}
	return
}

func InitLogger(newLogPaths []string) {
	if reflect.DeepEqual(logPaths, newLogPaths) {
		return
	}
	logAtomLevel = zap.NewAtomicLevel()
	logPaths = newLogPaths
	var syncers []zapcore.WriteSyncer
	for _, p := range logPaths {
		switch p {
		case "stdout":
			syncers = append(syncers, zapcore.AddSync(os.Stdout))
		case "stderr":
			syncers = append(syncers, zapcore.AddSync(os.Stderr))
		default:
			writeFile := zapcore.AddSync(&lumberjack.Logger{
				Filename:   p,
				MaxSize:    100, // megabytes
				MaxBackups: 10,
				LocalTime:  true,
			})
			syncers = append(syncers, writeFile)
		}
	}

	cfg := zap.NewProductionEncoderConfig()
	cfg.EncodeTime = zapcore.ISO8601TimeEncoder
	core := zapcore.NewCore(
		zapcore.NewJSONEncoder(cfg),
		zapcore.NewMultiWriteSyncer(syncers...),
		logAtomLevel,
	)
	Logger = zap.New(core, zap.AddStacktrace(zap.ErrorLevel), zap.AddCaller())
}

func SetLogLevel(newLogLevel string) {
	if Logger != nil {
		var lvl zapcore.Level
		if err := lvl.Set(newLogLevel); err != nil {
			lvl = zap.InfoLevel
		}
		logAtomLevel.SetLevel(lvl)
	}
}

func GenTraceId() string {
	return uuid.NewString()
}

const (
	TraceKindFetchStart   string = "fetch start"
	TraceKindFetchEnd     string = "fetch end"
	TraceKindProcessStart string = "process start"
	TraceKindProcessEnd   string = "process end"
	TraceKindWriteStart   string = "loopwrite start"
	TraceKindWriteEnd     string = "loopwrite end"
	TraceKindProcessing   string = "process continue"
)

func SetLogTrace(enabled bool) {
	logTrace = enabled
}

func LogTrace(traceId string, kind string, fields ...zapcore.Field) {
	if logTrace {
		allFields := []zapcore.Field{
			zap.String("trace_id", traceId),
			zap.String("trace_kind", kind)}
		allFields = append(allFields, fields...)
		Logger.Info("===trace===", allFields...)
	}
}

// set v2 to v1, if v1 didn't bind any value
// FIXME: how about v1 bind default value?
func TrySetValue(v1, v2 interface{}) bool {
	var ok bool
	rt := reflect.TypeOf(v1)
	rv := reflect.ValueOf(v1)

	if rt.Kind() != reflect.Ptr {
		return ok
	}
	for rt.Kind() == reflect.Ptr {
		rt = rt.Elem()
		rv = rv.Elem()
	}

	if rv.IsValid() && rv.IsZero() {
		ok = true
		switch rt.Kind() {
		case reflect.Uint:
			v, _ := v2.(uint)
			rv.SetUint(uint64(v))
		case reflect.Uint8:
			v, _ := v2.(uint8)
			rv.SetUint(uint64(v))
		case reflect.Uint16:
			v, _ := v2.(uint16)
			rv.SetUint(uint64(v))
		case reflect.Uint32:
			v, _ := v2.(uint32)
			rv.SetUint(uint64(v))
		case reflect.Uint64:
			v, _ := v2.(uint64)
			rv.SetUint(uint64(v))
		case reflect.Int:
			v, _ := v2.(int)
			rv.SetInt(int64(v))
		case reflect.Int8:
			v, _ := v2.(int8)
			rv.SetInt(int64(v))
		case reflect.Int16:
			v, _ := v2.(int16)
			rv.SetInt(int64(v))
		case reflect.Int32:
			v, _ := v2.(int32)
			rv.SetInt(int64(v))
		case reflect.Int64:
			v, _ := v2.(int64)
			rv.SetInt(int64(v))
		case reflect.Float32:
			v, _ := v2.(float32)
			rv.SetFloat(float64(v))
		case reflect.Float64:
			v, _ := v2.(float64)
			rv.SetFloat(float64(v))
		case reflect.String:
			rv.SetString(v2.(string))
		case reflect.Bool:
			rv.SetBool(v2.(bool))
		default:
			ok = false
		}
	}
	return ok
}
