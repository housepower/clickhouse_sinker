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

import "net"

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
