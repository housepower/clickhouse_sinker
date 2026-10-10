package rcm

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestEtcdOptionsValidate(t *testing.T) {
	cases := []struct {
		name    string
		o       EtcdOptions
		wantErr string
	}{
		{name: "ok", o: EtcdOptions{Endpoints: "127.0.0.1:2379", Key: "/k"}},
		{name: "no endpoints", o: EtcdOptions{Key: "/k"}, wantErr: "--etcd-endpoints"},
		{name: "blank endpoints", o: EtcdOptions{Endpoints: " , ", Key: "/k"}, wantErr: "--etcd-endpoints"},
		{name: "no key", o: EtcdOptions{Endpoints: "e:2379"}, wantErr: "--etcd-key"},
		{name: "cert without key", o: EtcdOptions{Endpoints: "e:2379", Key: "/k", CertFile: "c"}, wantErr: "--etcd-key-file"},
		{name: "key without cert", o: EtcdOptions{Endpoints: "e:2379", Key: "/k", KeyFile: "k"}, wantErr: "--etcd-cert-file"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := c.o.validate()
			if c.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected error %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), c.wantErr) {
				t.Fatalf("want error containing %q, got %v", c.wantErr, err)
			}
		})
	}
}

func TestEtcdOptionsEndpoints(t *testing.T) {
	got := EtcdOptions{Endpoints: " a:2379, ,b:2379 "}.endpoints()
	if strings.Join(got, "|") != "a:2379|b:2379" {
		t.Fatalf("got %v", got)
	}
}

func TestNewEtcdMissingCAFile(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "no-such-ca.pem")
	_, err := NewEtcd(EtcdOptions{Endpoints: "127.0.0.1:2379", Key: "/k", CAFile: missing})
	if err == nil || !strings.Contains(err.Error(), "TLS") {
		t.Fatalf("want TLS load error, got %v", err)
	}
}
