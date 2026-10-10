package rcm

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/apolloconfig/agollo/v4"
	agconfig "github.com/apolloconfig/agollo/v4/env/config"

	"github.com/housepower/clickhouse_sinker/util"
)

// apolloContentKey holds the whole text of a txt/json namespace.
const apolloContentKey = "content"

type ApolloOptions struct {
	Meta      string
	AppID     string
	Cluster   string // empty means "default"
	Namespace string
	Secret    string

	// Portal Open API, required for write-back only.
	Portal   string
	Token    string
	Env      string
	Operator string
}

func (o ApolloOptions) cluster() string {
	if o.Cluster == "" {
		return "default"
	}
	return o.Cluster
}

func (o ApolloOptions) validateRead() error {
	if o.Meta == "" || o.AppID == "" || o.Namespace == "" {
		return errors.New("apollo: --apollo-meta, --apollo-app-id and --apollo-namespace are required")
	}
	return nil
}

// ApolloSource reads one txt/json namespace through agollo's local cache,
// which agollo keeps fresh by long polling.
type ApolloSource struct {
	client    agollo.Client
	namespace string
}

var _ Source = (*ApolloSource)(nil)

// agollo's logger is process-global.
var setApolloLoggerOnce sync.Once

func NewApolloSource(o ApolloOptions) (*ApolloSource, error) {
	if err := o.validateRead(); err != nil {
		return nil, err
	}
	setApolloLoggerOnce.Do(func() {
		agollo.SetLogger(util.Logger.Named("apollo").Sugar())
	})
	client, err := agollo.StartWithConfig(func() (*agconfig.AppConfig, error) {
		return &agconfig.AppConfig{
			AppID:         o.AppID,
			Cluster:       o.cluster(),
			NamespaceName: o.Namespace,
			IP:            o.Meta,
			Secret:        o.Secret,
			// Like nacos' NotLoadCacheAtStart: never start from a stale on-disk backup.
			IsBackupConfig:    false,
			MustStart:         true,
			SyncServerTimeout: int(opTimeout / time.Second),
		}, nil
	})
	if err != nil {
		return nil, fmt.Errorf("apollo: appId %s namespace %s: %w", o.AppID, o.Namespace, err)
	}
	return &ApolloSource{client: client, namespace: o.Namespace}, nil
}

func (a *ApolloSource) Fetch(ctx context.Context) ([]byte, error) {
	cfg := a.client.GetConfig(a.namespace)
	if cfg == nil {
		return nil, fmt.Errorf("namespace %s is not loaded", a.namespace)
	}
	return []byte(cfg.GetValue(apolloContentKey)), nil
}

func (a *ApolloSource) Close() error {
	a.client.Close()
	return nil
}
