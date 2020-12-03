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
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/fagongzi/goetty"
	"github.com/pkg/errors"
)

var (
	GlobalTimerWheel  *goetty.TimeoutWheel //the global timer wheel
	GlobalParsingPool *WorkerPool          //for all tasks' parsing, cpu intensive
	GlobalWritingPool *WorkerPool          //the all tasks' writing ClickHouse, cpu-net balance
)

// InitGlobalTimerWheel initialize the global timer wheel
func InitGlobalTimerWheel() {
	GlobalTimerWheel = goetty.NewTimeoutWheel(goetty.WithTickInterval(time.Second))
}

// InitGlobalParsingPool initialize GlobalParsingPool
func InitGlobalParsingPool(maxWorkers int) {
	GlobalParsingPool = NewWorkerPool(maxWorkers, 100*runtime.NumCPU())
}

// InitGlobalWritingPool initialize GlobalWritingPool
func InitGlobalWritingPool(maxWorkers int) {
	queueSize := runtime.NumCPU() / 4
	if queueSize < 3 {
		queueSize = 3
	}
	GlobalWritingPool = NewWorkerPool(maxWorkers, queueSize)
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
func GetSourceName(name string) (sourcename string) {
	sourcename = strings.Replace(name, ".", "\\.", -1)
	return
}

// GetShift returns the smallest `shift` which 1<<shift is no smaller than s
func GetShift(s int) (shift int) {
	for shift = 0; (1 << shift) < s; shift++ {
	}
	return
}

// GetOutboundIP get preferred outbound ip of this machine
//https://stackoverflow.com/questions/23558425/how-do-i-get-the-local-ip-address-in-go
func GetOutboundIP() net.IP {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	localAddr := conn.LocalAddr().(*net.UDPAddr)
	return localAddr.IP
}

// GetSpareTCPPort find a spare TCP port
func GetSpareTCPPort(ip string, portBegin int) (port int) {
LOOP:
	for port = portBegin; ; port++ {
		addr := fmt.Sprintf("%s:%d", ip, port)
		ln, err := net.Listen("tcp", addr)
		if err == nil {
			ln.Close()
			break LOOP
		}
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

	// Load CA cert
	caCertPool := x509.NewCertPool()
	for _, caCertFile := range strings.Split(caCertFiles, ",") {
		caCert, err := ioutil.ReadFile(caCertFile)
		if err != nil {
			err = errors.Wrapf(err, "")
			return &tlsConfig, err
		}
		caCertPool.AppendCertsFromPEM(caCert)
	}
	tlsConfig.RootCAs = caCertPool
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
	_, found := os.LookupEnv(realKey)
	if found {
		*value = true
	}
}
