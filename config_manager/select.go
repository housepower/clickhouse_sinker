package rcm

import (
	"fmt"
	"path/filepath"
	"strings"

	"go.uber.org/zap"

	"github.com/housepower/clickhouse_sinker/util"
)

// selectBackend decides the config backend from the options that name a config
// location. "" means the local config file.
func selectBackend(cmdOps *util.CmdOptions) (string, error) {
	var names, flags []string
	if cmdOps.NacosDataID != "" {
		names, flags = append(names, "nacos"), append(flags, "--nacos-dataid")
	}
	if cmdOps.EtcdKey != "" {
		names, flags = append(names, "etcd"), append(flags, "--etcd-key")
	}
	if cmdOps.ApolloNamespace != "" {
		names, flags = append(names, "apollo"), append(flags, "--apollo-namespace")
	}
	if cmdOps.HttpUrls != "" {
		names, flags = append(names, "http"), append(flags, "--http-urls")
	}
	if len(names) > 1 {
		return "", fmt.Errorf("only one config backend may be configured, got %s", strings.Join(flags, ", "))
	}
	if len(names) == 0 {
		return "", nil
	}
	return names[0], nil
}

// New builds the Manager for the configured backend. It returns (nil, nil) when
// no backend is configured, meaning the local config file is used.
func New(cmdOps *util.CmdOptions) (*Manager, error) {
	backend, err := selectBackend(cmdOps)
	if err != nil {
		return nil, err
	}
	switch backend {
	case "nacos":
		n, err := NewNacos(NacosOptions{
			Addr:        cmdOps.NacosAddr,
			Username:    cmdOps.NacosUsername,
			Password:    cmdOps.NacosPassword,
			NamespaceID: cmdOps.NacosNamespaceID,
			Group:       cmdOps.NacosGroup,
			DataID:      cmdOps.NacosDataID,
			CacheDir:    filepath.Join(logDir(cmdOps.LogPaths), "nacos_cache"),
		})
		if err != nil {
			return nil, err
		}
		util.Logger.Info("get config from nacos",
			zap.String("addr", cmdOps.NacosAddr), zap.String("namespaceId", cmdOps.NacosNamespaceID),
			zap.String("group", cmdOps.NacosGroup), zap.String("dataId", cmdOps.NacosDataID))
		return NewManager("nacos", n, n), nil
	case "etcd":
		e, err := NewEtcd(EtcdOptions{
			Endpoints: cmdOps.EtcdEndpoints,
			Key:       cmdOps.EtcdKey,
			Username:  cmdOps.EtcdUsername,
			Password:  cmdOps.EtcdPassword,
			CAFile:    cmdOps.EtcdCAFile,
			CertFile:  cmdOps.EtcdCertFile,
			KeyFile:   cmdOps.EtcdKeyFile,
		})
		if err != nil {
			return nil, err
		}
		util.Logger.Info("get config from etcd",
			zap.String("endpoints", cmdOps.EtcdEndpoints), zap.String("key", cmdOps.EtcdKey))
		return NewManager("etcd", e, e), nil
	case "apollo":
		o := ApolloOptions{
			Meta:      cmdOps.ApolloMeta,
			AppID:     cmdOps.ApolloAppID,
			Cluster:   cmdOps.ApolloCluster,
			Namespace: cmdOps.ApolloNamespace,
			Secret:    cmdOps.ApolloSecret,
			Portal:    cmdOps.ApolloPortal,
			Token:     cmdOps.ApolloToken,
			Env:       cmdOps.ApolloEnv,
			Operator:  cmdOps.ApolloOperator,
		}
		writable, err := o.writeConfigured()
		if err != nil {
			return nil, err
		}
		src, err := NewApolloSource(o)
		if err != nil {
			return nil, err
		}
		var w Writer
		if writable {
			if o.plainHTTPPortal() {
				util.Logger.Warn("--apollo-portal uses plain http, the Open API token is sent unencrypted",
					zap.String("portal", o.Portal))
			}
			w = newApolloOpenAPI(o)
		}
		util.Logger.Info("get config from apollo",
			zap.String("meta", o.Meta), zap.String("appId", o.AppID),
			zap.String("cluster", o.cluster()), zap.String("namespace", o.Namespace),
			zap.Bool("writable", writable))
		return NewManager("apollo", src, w), nil
	case "http":
		h, err := NewHTTP(HTTPOptions{
			URLs:               cmdOps.HttpUrls,
			Namespace:          cmdOps.HttpNamespace,
			DataID:             cmdOps.HttpDataID,
			Group:              cmdOps.HttpGroup,
			Headers:            cmdOps.HttpHeaders,
			ConfigPath:         cmdOps.HttpConfigPath,
			InsecureSkipVerify: cmdOps.HttpInsecureSkipVerify,
		})
		if err != nil {
			return nil, err
		}
		util.Logger.Info("get config from http", zap.Strings("urls", h.URLs()))
		return NewManager("http", h, nil), nil
	}
	return nil, nil
}

// logDir is the directory of the last file in --log-paths, "." if none.
func logDir(logPaths string) string {
	dir := "."
	for _, logPath := range strings.Split(logPaths, ",") {
		if logPath != "stdout" && logPath != "stderr" {
			dir, _ = filepath.Split(logPath)
		}
	}
	if abs, err := filepath.Abs(dir); err == nil {
		return abs
	}
	return dir
}
