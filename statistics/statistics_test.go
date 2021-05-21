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
	"fmt"
	"testing"
	"time"

	"github.com/housepower/clickhouse_sinker/util"
	"github.com/stretchr/testify/require"
)

func TestPusher(t *testing.T) {
	addrs := []string{"172.24.25.1:9091", "172.24.25.2:9091"}
	interval := 1
	selfIP, _ := util.GetOutboundIP()
	selfPort := util.GetSpareTCPPort(1024)
	selfAddr := fmt.Sprintf("%s:%d", selfIP, selfPort)
	pusher := NewPusher(addrs, interval, selfAddr)

	err := pusher.Init()
	require.Nilf(t, err, "pusher init failed")

	ctx := context.Background()
	go pusher.Run(ctx)
	time.Sleep(10 * time.Second)
	pusher.Stop()
}
