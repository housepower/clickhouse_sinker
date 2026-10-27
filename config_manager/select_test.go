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
		{name: "etcd", ops: util.CmdOptions{EtcdKey: "/k"}, want: "etcd"},
		{name: "nacos and etcd", ops: util.CmdOptions{NacosDataID: "d", EtcdKey: "/k"}, wantErr: "--nacos-dataid, --etcd-key"},
		{name: "apollo", ops: util.CmdOptions{ApolloNamespace: "n"}, want: "apollo"},
		{name: "etcd and apollo", ops: util.CmdOptions{EtcdKey: "/k", ApolloNamespace: "n"}, wantErr: "--etcd-key, --apollo-namespace"},
		{name: "http", ops: util.CmdOptions{HttpUrls: "http://a/cfg"}, want: "http"},
		{name: "http and nacos", ops: util.CmdOptions{HttpUrls: "http://a/cfg", NacosDataID: "d"}, wantErr: "--nacos-dataid, --http-urls"},
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
