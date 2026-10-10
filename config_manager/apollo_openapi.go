package rcm

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// writeConfigured reports whether write-back is enabled: all four Open API
// options, or none of them.
func (o ApolloOptions) writeConfigured() (bool, error) {
	set := 0
	for _, v := range []string{o.Portal, o.Token, o.Env, o.Operator} {
		if v != "" {
			set++
		}
	}
	switch set {
	case 0:
		return false, nil
	case 4:
		return true, nil
	}
	return false, errors.New("apollo: --apollo-portal, --apollo-token, --apollo-env and --apollo-operator " +
		"must be set together to enable write-back")
}

// plainHTTPPortal reports whether the Open API token would travel unencrypted.
func (o ApolloOptions) plainHTTPPortal() bool {
	return strings.HasPrefix(strings.ToLower(o.Portal), "http://")
}

// apolloOpenAPI writes a namespace through the Apollo Portal Open API.
// Apollo has no compare-and-swap, so Update reads the latest content right
// before writing and refuses to run when someone has unreleased edits.
type apolloOpenAPI struct {
	nsURL    string // .../openapi/v1/envs/{env}/apps/{appId}/clusters/{cluster}/namespaces/{ns}
	token    string
	operator string
	ns       string
	client   *http.Client
}

var _ Writer = (*apolloOpenAPI)(nil)

func newApolloOpenAPI(o ApolloOptions) *apolloOpenAPI {
	nsURL := fmt.Sprintf("%s/openapi/v1/envs/%s/apps/%s/clusters/%s/namespaces/%s",
		strings.TrimRight(o.Portal, "/"), url.PathEscape(o.Env), url.PathEscape(o.AppID),
		url.PathEscape(o.cluster()), url.PathEscape(o.Namespace))
	return &apolloOpenAPI{
		nsURL:    nsURL,
		token:    o.Token,
		operator: o.Operator,
		ns:       o.Namespace,
		client:   &http.Client{Timeout: opTimeout},
	}
}

func (c *apolloOpenAPI) do(ctx context.Context, method, u string, body, out any) error {
	var rd io.Reader
	if body != nil {
		bs, err := json.Marshal(body)
		if err != nil {
			return err
		}
		rd = bytes.NewReader(bs)
	}
	req, err := http.NewRequestWithContext(ctx, method, u, rd)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", c.token)
	req.Header.Set("Content-Type", "application/json;charset=UTF-8")
	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("%s %s: HTTP %d: %s", method, u, resp.StatusCode, bodySnippet(respBody))
	}
	if out != nil && len(bytes.TrimSpace(respBody)) > 0 {
		if err = json.Unmarshal(respBody, out); err != nil {
			return fmt.Errorf("%s %s: decode response %s: %w", method, u, bodySnippet(respBody), err)
		}
	}
	return nil
}

// bodySnippet keeps error messages short when the Portal (or a proxy in front
// of it) answers with an HTML page.
func bodySnippet(body []byte) string {
	const max = 512
	body = bytes.TrimSpace(body)
	if len(body) <= max {
		return string(body)
	}
	return string(body[:max]) + "...(truncated)"
}

// current returns the latest edited content and whether it differs from the
// latest release, i.e. someone has unreleased edits in this namespace.
func (c *apolloOpenAPI) current(ctx context.Context) (string, bool, error) {
	var ns struct {
		Items []struct {
			Key   string `json:"key"`
			Value string `json:"value"`
		} `json:"items"`
	}
	if err := c.do(ctx, http.MethodGet, c.nsURL, nil, &ns); err != nil {
		return "", false, err
	}
	var content string
	for _, it := range ns.Items {
		if it.Key == apolloContentKey {
			content = it.Value
		}
	}
	var rel struct {
		Configurations map[string]string `json:"configurations"`
	}
	if err := c.do(ctx, http.MethodGet, c.nsURL+"/releases/latest", nil, &rel); err != nil {
		return "", false, err
	}
	return content, content != rel.Configurations[apolloContentKey], nil
}

func (c *apolloOpenAPI) Update(ctx context.Context, fn UpdateFunc) error {
	old, pending, err := c.current(ctx)
	if err != nil {
		return fmt.Errorf("namespace %s: %w", c.ns, err)
	}
	if pending {
		// Releasing now would also release someone else's half-done edits.
		return fmt.Errorf("namespace %s: %w", c.ns, ErrPendingChanges)
	}
	neu, err := fn([]byte(old))
	if err != nil {
		return err
	}
	if string(neu) == old {
		return nil
	}
	item := map[string]string{
		"key":                      apolloContentKey,
		"value":                    string(neu),
		"dataChangeCreatedBy":      c.operator,
		"dataChangeLastModifiedBy": c.operator,
	}
	if err = c.do(ctx, http.MethodPut, c.nsURL+"/items/"+apolloContentKey+"?createIfNotExists=true", item, nil); err != nil {
		return fmt.Errorf("namespace %s: modify: %w", c.ns, err)
	}
	release := map[string]string{
		"releaseTitle":   time.Now().Format("20060102150405") + "-clickhouse_sinker",
		"releaseComment": "clickhouse_sinker discovery write-back",
		"releasedBy":     c.operator,
	}
	if err = c.do(ctx, http.MethodPost, c.nsURL+"/releases", release, nil); err != nil {
		return fmt.Errorf("namespace %s: modification committed but NOT released, release it in Apollo Portal: %w", c.ns, err)
	}
	return nil
}
