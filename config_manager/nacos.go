package rcm

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/nacos-group/nacos-sdk-go/v2/clients"
	"github.com/nacos-group/nacos-sdk-go/v2/clients/config_client"
	"github.com/nacos-group/nacos-sdk-go/v2/common/constant"
	"github.com/nacos-group/nacos-sdk-go/v2/common/logger"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"
	"go.uber.org/zap"

	"github.com/housepower/clickhouse_sinker/util"
)

// nacosCASConflictMsg is part of the error nacos 2.x returns when casMd5 no
// longer matches ("Cas publish fail,server md5 may have changed."). Pinned by
// TestNacosCASConflictDetected (integration). Note nacos with embedded storage
// (standalone Derby) ignores casMd5, so the CAS is only strict with an
// external MySQL datasource.
const nacosCASConflictMsg = "Cas publish fail"

// nacosNotFoundMsg is what nacos-sdk-go v2 returns from GetConfig for a dataId
// that does not exist yet (error code 300).
const nacosNotFoundMsg = "config data not exist"

type NacosOptions struct {
	Addr        string // comma-separated host[:port], port defaults to 8848
	Username    string
	Password    string
	NamespaceID string // Neither DEFAULT_NAMESPACE_ID("public") nor namespace name work!
	Group       string // Empty means DEFAULT_GROUP
	DataID      string
	CacheDir    string
}

// Nacos is a Source and Writer backed by one nacos dataId.
type Nacos struct {
	client config_client.IConfigClient
	group  string
	dataID string
}

var (
	_ Source = (*Nacos)(nil)
	_ Writer = (*Nacos)(nil)
)

func parseNacosServers(addr string) ([]constant.ServerConfig, error) {
	var sc []constant.ServerConfig
	for _, serverAddr := range strings.Split(addr, ",") {
		fields := strings.SplitN(strings.TrimSpace(serverAddr), ":", 2)
		var port uint64 = 8848
		if len(fields) == 2 {
			var err error
			if port, err = strconv.ParseUint(fields[1], 10, 64); err != nil {
				return nil, fmt.Errorf("invalid nacos address %q: %w", serverAddr, err)
			}
		}
		sc = append(sc, constant.ServerConfig{IpAddr: fields[0], Port: port})
	}
	return sc, nil
}

func NewNacos(o NacosOptions) (*Nacos, error) {
	if o.DataID == "" {
		return nil, errors.New("nacos: --nacos-dataid is required")
	}
	sc, err := parseNacosServers(o.Addr)
	if err != nil {
		return nil, fmt.Errorf("nacos: %w", err)
	}
	group := o.Group
	if group == "" {
		group = constant.DEFAULT_GROUP
	}
	cc := constant.ClientConfig{
		NamespaceId:         o.NamespaceID,
		TimeoutMs:           uint64(opTimeout.Milliseconds()),
		NotLoadCacheAtStart: true,
		// Without this GetConfig silently returns the local snapshot when the
		// server is unreachable; the poll loop already keeps the current config
		// on fetch errors.
		DisableUseSnapShot: true,
		CacheDir:           o.CacheDir,
		LogDir:             o.CacheDir,
		Username:           o.Username,
		Password:           o.Password,
	}
	client, err := clients.NewConfigClient(vo.NacosClientParam{ClientConfig: &cc, ServerConfigs: sc})
	if err != nil {
		return nil, fmt.Errorf("nacos: create client: %w", err)
	}
	// Creating the client initializes nacos' own logger, so route it into the
	// sinker log only afterwards.
	logger.SetLogger(&NacosLogger{util.Logger.Sugar()})
	return &Nacos{client: client, group: group, dataID: o.DataID}, nil
}

func (n *Nacos) Fetch(ctx context.Context) ([]byte, error) {
	content, err := n.client.GetConfig(vo.ConfigParam{DataId: n.dataID, Group: n.group})
	if err != nil {
		return nil, n.wrap(err)
	}
	return []byte(content), nil
}

func (n *Nacos) wrap(err error) error {
	return fmt.Errorf("dataId %s group %s: %w", n.dataID, n.group, err)
}

func (n *Nacos) Update(ctx context.Context, fn UpdateFunc) error {
	return updateWithCAS(ctx, fn, n.read, n.write)
}

// read returns the content with its md5 as version. A missing dataId gives an
// empty version, which publishes without CAS (first-time creation).
func (n *Nacos) read(ctx context.Context) ([]byte, string, error) {
	content, err := n.client.GetConfig(vo.ConfigParam{DataId: n.dataID, Group: n.group})
	if err != nil {
		// Match the server's own message, before our wrapping adds the dataId.
		if strings.Contains(err.Error(), nacosNotFoundMsg) {
			return nil, "", nil
		}
		return nil, "", n.wrap(err)
	}
	bs := []byte(content)
	if len(bs) == 0 {
		return bs, "", nil
	}
	sum := md5.Sum(bs)
	return bs, hex.EncodeToString(sum[:]), nil
}

func (n *Nacos) write(ctx context.Context, content []byte, casMd5 string) (bool, error) {
	_, err := n.client.PublishConfig(vo.ConfigParam{
		DataId:  n.dataID,
		Group:   n.group,
		Content: string(content),
		CasMd5:  casMd5,
	})
	if err != nil {
		if strings.Contains(err.Error(), nacosCASConflictMsg) {
			return true, nil
		}
		return false, n.wrap(err)
	}
	return false, nil
}

func (n *Nacos) Close() error {
	n.client.CloseClient()
	return nil
}

type NacosLogger struct {
	*zap.SugaredLogger
}

// override the loglevel to avoid log blow up
func (nlog *NacosLogger) Info(args ...interface{}) {
	nlog.Debug(args...)
}

// override the loglevel to avoid log blow up
func (nlog *NacosLogger) Infof(fmt string, args ...interface{}) {
	nlog.Debugf(fmt, args...)
}
