package rcm

import (
	"context"
	"errors"
	"testing"

	"github.com/nacos-group/nacos-sdk-go/v2/clients/config_client"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"
)

// fakeConfigClient overrides GetConfig; any other method panics (nil embed).
type fakeConfigClient struct {
	config_client.IConfigClient
	content string
	err     error
}

func (f *fakeConfigClient) GetConfig(vo.ConfigParam) (string, error) { return f.content, f.err }

func TestNacosReadMissingDataIDPublishesWithoutCAS(t *testing.T) {
	n := &Nacos{client: &fakeConfigClient{err: errors.New(nacosNotFoundMsg)}, group: "G", dataID: "d"}
	bs, version, err := n.read(context.Background())
	if err != nil || bs != nil || version != "" {
		t.Fatalf("got %q, %q, %v; want an empty read", bs, version, err)
	}
}

// Only the server's not-found answer may skip CAS. Matching the wrapped error
// would also match a dataId that happens to contain the phrase.
func TestNacosReadOtherErrorsAreReturned(t *testing.T) {
	n := &Nacos{
		client: &fakeConfigClient{err: errors.New("context deadline exceeded")},
		group:  "G",
		dataID: "sinker-" + nacosNotFoundMsg,
	}
	if _, _, err := n.read(context.Background()); err == nil {
		t.Fatal("a timeout must be returned, not treated as a missing dataId")
	}
}
