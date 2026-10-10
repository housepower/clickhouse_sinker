package rcm

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"go.etcd.io/etcd/client/pkg/v3/transport"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/housepower/clickhouse_sinker/util"
)

type EtcdOptions struct {
	Endpoints string // comma-separated
	Key       string
	Username  string
	Password  string
	CAFile    string // CA only: one-way TLS
	CertFile  string // with KeyFile: mutual TLS
	KeyFile   string
}

func (o EtcdOptions) endpoints() []string {
	var eps []string
	for _, ep := range strings.Split(o.Endpoints, ",") {
		if ep = strings.TrimSpace(ep); ep != "" {
			eps = append(eps, ep)
		}
	}
	return eps
}

func (o EtcdOptions) validate() error {
	if len(o.endpoints()) == 0 {
		return errors.New("etcd: --etcd-endpoints is required")
	}
	if o.Key == "" {
		return errors.New("etcd: --etcd-key is required")
	}
	if o.CertFile != "" && o.KeyFile == "" {
		return errors.New("etcd: --etcd-cert-file requires --etcd-key-file")
	}
	if o.KeyFile != "" && o.CertFile == "" {
		return errors.New("etcd: --etcd-key-file requires --etcd-cert-file")
	}
	return nil
}

// Etcd is a Source and Writer backed by one etcd key.
type Etcd struct {
	cli *clientv3.Client
	key string
}

var (
	_ Source = (*Etcd)(nil)
	_ Writer = (*Etcd)(nil)
)

func NewEtcd(o EtcdOptions) (*Etcd, error) {
	if err := o.validate(); err != nil {
		return nil, err
	}
	cfg := clientv3.Config{
		Endpoints:   o.endpoints(),
		Username:    o.Username,
		Password:    o.Password,
		DialTimeout: opTimeout,
		Logger:      util.Logger.Named("etcd"),
	}
	if o.CAFile != "" || o.CertFile != "" {
		tlsInfo := transport.TLSInfo{CertFile: o.CertFile, KeyFile: o.KeyFile, TrustedCAFile: o.CAFile}
		tlsCfg, err := tlsInfo.ClientConfig()
		if err != nil {
			return nil, fmt.Errorf("etcd: load TLS files: %w", err)
		}
		cfg.TLS = tlsCfg
	}
	cli, err := clientv3.New(cfg)
	if err != nil {
		return nil, fmt.Errorf("etcd: create client: %w", err)
	}
	return &Etcd{cli: cli, key: o.Key}, nil
}

func (e *Etcd) Fetch(ctx context.Context) ([]byte, error) {
	bs, _, err := e.read(ctx)
	return bs, err
}

// read returns the value with its ModRevision as version. A missing key is an
// error: create it with etcdctl before starting the sinker.
func (e *Etcd) read(ctx context.Context) ([]byte, string, error) {
	resp, err := e.cli.Get(ctx, e.key)
	if err != nil {
		return nil, "", fmt.Errorf("key %s: %w", e.key, err)
	}
	if len(resp.Kvs) == 0 {
		return nil, "", fmt.Errorf("key %s does not exist", e.key)
	}
	kv := resp.Kvs[0]
	return kv.Value, strconv.FormatInt(kv.ModRevision, 10), nil
}

func (e *Etcd) write(ctx context.Context, content []byte, version string) (bool, error) {
	rev, err := strconv.ParseInt(version, 10, 64)
	if err != nil {
		return false, fmt.Errorf("key %s: bad revision %q: %w", e.key, version, err)
	}
	resp, err := e.cli.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision(e.key), "=", rev)).
		Then(clientv3.OpPut(e.key, string(content))).
		Commit()
	if err != nil {
		return false, fmt.Errorf("key %s: %w", e.key, err)
	}
	return !resp.Succeeded, nil
}

func (e *Etcd) Update(ctx context.Context, fn UpdateFunc) error {
	return updateWithCAS(ctx, fn, e.read, e.write)
}

func (e *Etcd) Close() error {
	return e.cli.Close()
}
