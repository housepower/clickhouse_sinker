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

type redactProbe struct {
	Brokers  string
	Password string
	Nested   struct {
		TrustStorePassword string
		ApiToken           string
		Realm              string
	}
	Security map[string]string
	Hosts    [][]string
}

func TestRedactedForLog(t *testing.T) {
	var p redactProbe
	p.Brokers = "k1:9092"
	p.Password = "pw-top"
	p.Nested.TrustStorePassword = "pw-truststore"
	p.Nested.ApiToken = "tok-api"
	p.Nested.Realm = "EXAMPLE.COM"
	p.Security = map[string]string{"sasl.jaas.config": `user="u" password="pw-jaas"`}
	p.Hosts = [][]string{{"ch1"}}

	bs, err := json.Marshal(RedactedForLog(p))
	if err != nil {
		t.Fatal(err)
	}
	out := string(bs)
	for _, secret := range []string{"pw-top", "pw-truststore", "tok-api", "pw-jaas"} {
		if strings.Contains(out, secret) {
			t.Fatalf("secret %q leaked: %s", secret, out)
		}
	}
	for _, kept := range []string{"k1:9092", "EXAMPLE.COM", "ch1", "sasl.jaas.config"} {
		if !strings.Contains(out, kept) {
			t.Fatalf("non-secret %q missing: %s", kept, out)
		}
	}
	if p.Password != "pw-top" || p.Security["sasl.jaas.config"] == "******" {
		t.Fatal("the original value must not be modified")
	}
}
