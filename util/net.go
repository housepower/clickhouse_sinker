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
	"fmt"
	"math"
	"net"

	"github.com/thanos-io/thanos/pkg/errors"
)

func GetIP4Byname(host string) (ips []string, err error) {
	addrs, err := net.LookupIP(host)
	if err != nil {
		return
	}
	ips = make([]string, len(addrs))
	for i, addr := range addrs {
		if ipv4 := addr.To4(); ipv4 != nil {
			ips[i] = ipv4.String()
		}
	}
	return
}

// GetOutboundIP gets preferred outbound ip of this machine
// https://stackoverflow.com/questions/23558425/how-do-i-get-the-local-ip-address-in-go.
func GetOutboundIP() (ip net.IP, err error) {
	var conn net.Conn
	if conn, err = net.Dial("udp", "8.8.8.8:80"); err != nil {
		err = errors.Wrapf(err, "")
		return
	}
	defer conn.Close()
	localAddr, _ := conn.LocalAddr().(*net.UDPAddr)
	ip = localAddr.IP
	return
}

// GetSpareTCPPort finds a spare TCP port.
func GetSpareTCPPort(portBegin int) int {
	for port := portBegin; port < math.MaxInt; port++ {
		if err := testListenOnPort(port); err == nil {
			return port
		}
	}
	return 0
}

// https://stackoverflow.com/questions/50428176/how-to-get-ip-and-port-from-net-addr-when-it-could-be-a-net-udpaddr-or-net-tcpad
func GetNetAddrPort(addr net.Addr) (port int) {
	switch addr := addr.(type) {
	case *net.UDPAddr:
		port = addr.Port
	case *net.TCPAddr:
		port = addr.Port
	}
	return
}

func testListenOnPort(port int) error {
	addr := fmt.Sprintf(":%d", port)
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	ln.Close() //nolint:errcheck
	return nil
}
