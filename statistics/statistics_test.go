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
package statistics

import (
	"context"
	"net"
	"testing"
	"time"
)

func TestPusher_IsExternalIP(t *testing.T) {
	addrs := []string{"172.24.25.1:9091"}
	interval := 30
	pusher := NewPusher(addrs, interval)

	external := pusher.IsExternalIP(net.ParseIP("192.168.154.134"))
	if external != false {
		t.Errorf("192.168.154.134 should not be external ip")
	}

	external = pusher.IsExternalIP(net.ParseIP("127.0.0.1"))
	if external != false {
		t.Errorf("127.0.0.1 should not be external ip")
	}

	external = pusher.IsExternalIP(net.ParseIP("43.230.88.7"))
	if external != true {
		t.Errorf("43.230.88.7 should be external ip")
	}
}

func TestPusher(t *testing.T) {
	addrs := []string{"172.24.25.1:9091", "172.24.25.2:9091"}
	interval := 1
	pusher := NewPusher(addrs, interval)

	err := pusher.Init()
	if err != nil {
		t.Fatalf("pusher init failed")
	}

	ctx := context.Background()
	go pusher.Run(ctx)
	time.Sleep(10 * time.Second)
	pusher.Stop()
}
