package rcm

import (
	"strings"
	"testing"

	"github.com/housepower/clickhouse_sinker/util"
)

func TestSelectBackend(t *testing.T) {
	cases := []struct {
		name    string
		ops     util.CmdOptions
		want    string
		wantErr string
	}{
		{name: "local file", ops: util.CmdOptions{}, want: ""},
		{name: "nacos", ops: util.CmdOptions{NacosDataID: "d"}, want: "nacos"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := selectBackend(&c.ops)
			if c.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), c.wantErr) {
					t.Fatalf("want error containing %q, got %v", c.wantErr, err)
				}
				return
			}
			if err != nil || got != c.want {
				t.Fatalf("got %q, %v; want %q", got, err, c.want)
			}
		})
	}
}
