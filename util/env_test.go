package util

import "testing"

func TestEnvStringVarPrefersPrefixed(t *testing.T) {
	t.Setenv("NACOS_ADDR", "legacy:8848")
	t.Setenv("SINKER_NACOS_ADDR", "prefixed:8848")
	v := "default"
	EnvStringVar(&v, "nacos-addr")
	if v != "prefixed:8848" {
		t.Fatalf("got %q, want prefixed:8848", v)
	}
}

func TestEnvStringVarFallsBackToLegacy(t *testing.T) {
	t.Setenv("NACOS_ADDR", "legacy:8848")
	v := "default"
	EnvStringVar(&v, "nacos-addr")
	if v != "legacy:8848" {
		t.Fatalf("got %q, want legacy:8848", v)
	}
}

func TestEnvIntAndBoolVarPrefixed(t *testing.T) {
	t.Setenv("SINKER_HTTP_PORT", "3000")
	t.Setenv("SINKER_V", "")
	port, ver := 0, false
	EnvIntVar(&port, "http-port")
	EnvBoolVar(&ver, "v")
	if port != 3000 || !ver {
		t.Fatalf("got port=%d ver=%v, want 3000 true", port, ver)
	}
}

func TestEnvStringVarPrefixedIgnoresUnprefixed(t *testing.T) {
	t.Setenv("ETCD_KEY", "/owned/by/etcd")
	v := "default"
	EnvStringVarPrefixed(&v, "etcd-key")
	if v != "default" {
		t.Fatalf("got %q, unprefixed ETCD_KEY must be ignored", v)
	}
	t.Setenv("SINKER_ETCD_KEY", "/sinker")
	EnvStringVarPrefixed(&v, "etcd-key")
	if v != "/sinker" {
		t.Fatalf("got %q, want /sinker", v)
	}
}
