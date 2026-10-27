package rcm

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/tidwall/gjson"

	"github.com/housepower/clickhouse_sinker/util"
)

// maxConfigSize bounds one config response body.
const maxConfigSize = 4 << 20

type HTTPOptions struct {
	URLs               string // comma-separated, tried in order
	Namespace          string // query param appended to every url when non-empty
	DataID             string // ditto
	Group              string // ditto
	Headers            string // semicolon-separated "Key: Value" pairs, sent verbatim
	ConfigPath         string // gjson path of the config in a JSON response, e.g. "data.config"; empty means the body is the config
	InsecureSkipVerify bool   // accept self-signed certs on https urls
}

// HTTP is a read-only Source that GETs the config from a list of urls, trying
// each in order until one answers 200 with a non-empty body. When every url
// fails, Fetch returns an error and the sinker keeps its current config.
type HTTP struct {
	urls       []string
	query      url.Values
	headers    http.Header
	configPath string
	cli        *http.Client
}

var _ Source = (*HTTP)(nil)

// URLs returns the configured urls for logging; headers are never logged.
func (h *HTTP) URLs() []string { return h.urls }

func NewHTTP(o HTTPOptions) (*HTTP, error) {
	var urls []string
	for _, u := range strings.Split(o.URLs, ",") {
		if u = strings.TrimSpace(u); u != "" {
			urls = append(urls, u)
		}
	}
	if len(urls) == 0 {
		return nil, errors.New("http: --http-urls is required")
	}
	for _, u := range urls {
		pu, err := url.Parse(u)
		if err != nil || pu.Scheme == "" || pu.Host == "" {
			return nil, fmt.Errorf("http: bad url %q", u)
		}
		if pu.Scheme != "http" && pu.Scheme != "https" {
			return nil, fmt.Errorf("http: url %q must use http or https", u)
		}
	}

	query := url.Values{}
	for k, v := range map[string]string{"namespace": o.Namespace, "data_id": o.DataID, "group": o.Group} {
		if v != "" {
			query.Set(k, v)
		}
	}

	headers := http.Header{}
	for _, pair := range strings.Split(o.Headers, ";") {
		if pair = strings.TrimSpace(pair); pair == "" {
			continue
		}
		k, v, ok := strings.Cut(pair, ":")
		if !ok || strings.TrimSpace(k) == "" {
			return nil, fmt.Errorf("http: bad header %q, want \"Key: Value\" pairs separated by ';'", pair)
		}
		headers.Set(strings.TrimSpace(k), strings.TrimSpace(v))
	}

	transport := http.DefaultTransport.(*http.Transport).Clone()
	if o.InsecureSkipVerify {
		transport.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	}
	return &HTTP{urls: urls, query: query, headers: headers, configPath: o.ConfigPath, cli: &http.Client{Transport: transport}}, nil
}

// Fetch tries the urls in order and returns the first 200 response with a
// non-empty body. The remaining context budget is split evenly across the
// remaining urls so a hung endpoint cannot starve the ones behind it.
func (h *HTTP) Fetch(ctx context.Context) ([]byte, error) {
	var errs []error
	for i, u := range h.urls {
		tryCtx, cancel := h.tryContext(ctx, len(h.urls)-i)
		bs, err := h.fetchOne(tryCtx, u)
		cancel()
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", u, err))
			util.Logger.Warn("http config url failed, trying the next one",
				zap.String("url", u), zap.Error(err))
			continue
		}
		return bs, nil
	}
	return nil, fmt.Errorf("all %d config urls failed: %w", len(h.urls), errors.Join(errs...))
}

// tryContext bounds one url attempt to its fair share of the remaining ctx
// budget, or to opTimeout when the caller set no deadline.
func (h *HTTP) tryContext(ctx context.Context, remaining int) (context.Context, context.CancelFunc) {
	budget := opTimeout
	if d, ok := ctx.Deadline(); ok {
		budget = time.Until(d)
	}
	return context.WithTimeout(ctx, budget/time.Duration(remaining))
}

func (h *HTTP) fetchOne(ctx context.Context, rawURL string) ([]byte, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, err
	}
	q := u.Query()
	for k := range h.query {
		q.Set(k, h.query.Get(k))
	}
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}
	for k := range h.headers {
		req.Header.Set(k, h.headers.Get(k))
	}
	resp, err := h.cli.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status %s", resp.Status)
	}
	bs, err := io.ReadAll(io.LimitReader(resp.Body, maxConfigSize))
	if err != nil {
		return nil, err
	}
	// A JSON wrapper is unwrapped at the configured path; anything else
	// (text/plain, or no path configured) is the config itself.
	if h.configPath != "" && strings.Contains(resp.Header.Get("Content-Type"), "json") {
		r := gjson.GetBytes(bs, h.configPath)
		if !r.Exists() {
			return nil, fmt.Errorf("config not found at path %q", h.configPath)
		}
		bs = []byte(r.Raw)
	}
	if len(bytes.TrimSpace(bs)) == 0 {
		return nil, errors.New("empty config")
	}
	return bs, nil
}

func (h *HTTP) Close() error {
	h.cli.CloseIdleConnections()
	return nil
}
