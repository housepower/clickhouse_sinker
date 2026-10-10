package util

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestCmdOptionsRedacted(t *testing.T) {
	secrets := []string{"s-nacos", "s-etcd", "s-apollosecret", "s-apollotoken", "s-ch", "s-kafka", "s-gss"}
	o := CmdOptions{
		NacosAddr: "127.0.0.1:8848", EtcdKey: "/sinker", EtcdUsername: "u",
		NacosPassword: secrets[0], EtcdPassword: secrets[1], ApolloSecret: secrets[2], ApolloToken: secrets[3],
		Credentials: Credentials{ClickhouseUsername: "chu", ClickhousePassword: secrets[4],
			KafkaPassword: secrets[5], KafkaGSSAPIPassword: secrets[6]},
	}
	r := o.Redacted()
	b, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range secrets {
		if strings.Contains(string(b), s) {
			t.Errorf("secret %q leaked: %s", s, b)
		}
	}
	if r.NacosAddr != o.NacosAddr || r.EtcdKey != "/sinker" || r.EtcdUsername != "u" || r.ClickhouseUsername != "chu" {
		t.Errorf("non-secret fields changed: %+v", r)
	}
	if r.EtcdPassword != "******" || r.KafkaPassword != "******" {
		t.Errorf("secrets not masked: %+v", r)
	}
	if o.NacosPassword != secrets[0] || o.ClickhousePassword != secrets[4] {
		t.Errorf("original mutated: %+v", o)
	}
	if (CmdOptions{}).Redacted().EtcdPassword != "" {
		t.Errorf("empty secret should stay empty")
	}
}
