package rcm

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// configServer returns a test server that asserts the request carries the
// expected query params and headers before serving body with the given status.
func configServer(t *testing.T, status int, body string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		if q.Get("namespace") != "ns1" || q.Get("data_id") != "d1" || q.Get("group") != "g1" {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprintf(w, "bad query: %s", r.URL.RawQuery)
			return
		}
		if r.Header.Get("Authorization") != "Bearer t0ken" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		w.WriteHeader(status)
		fmt.Fprint(w, body)
	}))
}

func testOptions(urls ...string) HTTPOptions {
	return HTTPOptions{
		URLs:      strings.Join(urls, ","),
		Namespace: "ns1",
		DataID:    "d1",
		Group:     "g1",
		Headers:   "Authorization: Bearer t0ken",
	}
}

func TestHTTPFetchSuccess(t *testing.T) {
	srv := configServer(t, http.StatusOK, `{"kafka":{}}`)
	defer srv.Close()

	h, err := NewHTTP(testOptions(srv.URL))
	if err != nil {
		t.Fatal(err)
	}
	bs, err := h.Fetch(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if string(bs) != `{"kafka":{}}` {
		t.Fatalf("got %q", bs)
	}
}

func TestHTTPFetchFailover(t *testing.T) {
	bad := configServer(t, http.StatusInternalServerError, "boom")
	defer bad.Close()
	good := configServer(t, http.StatusOK, `{"from":"second"}`)
	defer good.Close()

	h, err := NewHTTP(testOptions(bad.URL, good.URL))
	if err != nil {
		t.Fatal(err)
	}
	bs, err := h.Fetch(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if string(bs) != `{"from":"second"}` {
		t.Fatalf("got %q, want the second url's content", bs)
	}
}

func TestHTTPFetchAllFail(t *testing.T) {
	s1 := configServer(t, http.StatusInternalServerError, "boom")
	defer s1.Close()
	s2 := configServer(t, http.StatusNotFound, "nope")
	defer s2.Close()

	h, err := NewHTTP(testOptions(s1.URL, s2.URL))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = h.Fetch(context.Background()); err == nil {
		t.Fatal("want error when every url fails")
	}
}

// A 200 with an empty body is as useless as a failure: try the next url.
func TestHTTPFetchEmptyBodyFailsOver(t *testing.T) {
	empty := configServer(t, http.StatusOK, "  ")
	defer empty.Close()
	good := configServer(t, http.StatusOK, `{"from":"second"}`)
	defer good.Close()

	h, err := NewHTTP(testOptions(empty.URL, good.URL))
	if err != nil {
		t.Fatal(err)
	}
	bs, err := h.Fetch(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if string(bs) != `{"from":"second"}` {
		t.Fatalf("got %q, want the second url's content", bs)
	}
}

// Params are appended to a url that already has a query string.
func TestHTTPFetchExistingQueryPreserved(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("a") != "b" || r.URL.Query().Get("namespace") != "ns1" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		fmt.Fprint(w, "ok")
	}))
	defer srv.Close()

	o := testOptions(srv.URL + "?a=b")
	o.Headers = ""
	h, err := NewHTTP(o)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = h.Fetch(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestHTTPFetchInsecureSkipVerify(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "ok")
	}))
	defer srv.Close()

	o := testOptions(srv.URL)
	o.Headers = ""
	o.Namespace, o.DataID, o.Group = "", "", ""

	h, err := NewHTTP(o)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = h.Fetch(context.Background()); err == nil {
		t.Fatal("want certificate error without insecure-skip-verify")
	}

	o.InsecureSkipVerify = true
	h, err = NewHTTP(o)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = h.Fetch(context.Background()); err != nil {
		t.Fatalf("insecure-skip-verify should accept the self-signed cert: %v", err)
	}
}

// A slow first url must not eat the budget of the remaining ones.
func TestHTTPFetchSlowUrlFailsOver(t *testing.T) {
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(5 * time.Second)
	}))
	defer slow.Close()
	good := configServer(t, http.StatusOK, "ok")
	defer good.Close()

	h, err := NewHTTP(testOptions(slow.URL, good.URL))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
	defer cancel()
	bs, err := h.Fetch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if string(bs) != "ok" {
		t.Fatalf("got %q", bs)
	}
}

// A JSON response is unwrapped at the configured path; a text response is
// used as-is even when a path is configured.
func TestHTTPFetchConfigPath(t *testing.T) {
	wrapped := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"code":"0000","msg":"ok","data":{"clusterId":"123","config":{"logLevel":"debug"}}}`)
	}))
	defer wrapped.Close()

	o := testOptions(wrapped.URL)
	o.Headers = ""
	o.Namespace, o.DataID, o.Group = "", "", ""
	o.ConfigPath = "data.config"
	h, err := NewHTTP(o)
	if err != nil {
		t.Fatal(err)
	}
	bs, err := h.Fetch(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if string(bs) != `{"logLevel":"debug"}` {
		t.Fatalf("got %q, want the unwrapped config", bs)
	}
}

// An error response carries no config at the path: fail over to the next url.
func TestHTTPFetchConfigPathMissingFailsOver(t *testing.T) {
	errResp := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"code":"5001","msg":"bad appKey"}`)
	}))
	defer errResp.Close()
	good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"code":"0000","data":{"config":{"logLevel":"info"}}}`)
	}))
	defer good.Close()

	o := testOptions(errResp.URL, good.URL)
	o.Headers = ""
	o.Namespace, o.DataID, o.Group = "", "", ""
	o.ConfigPath = "data.config"
	h, err := NewHTTP(o)
	if err != nil {
		t.Fatal(err)
	}
	bs, err := h.Fetch(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if string(bs) != `{"logLevel":"info"}` {
		t.Fatalf("got %q, want the second url's config", bs)
	}
}

// A text response is the config itself; the configured path does not apply.
func TestHTTPFetchTextIgnoresConfigPath(t *testing.T) {
	text := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		fmt.Fprint(w, "logLevel: debug\n")
	}))
	defer text.Close()

	o := testOptions(text.URL)
	o.Headers = ""
	o.Namespace, o.DataID, o.Group = "", "", ""
	o.ConfigPath = "data.config"
	h, err := NewHTTP(o)
	if err != nil {
		t.Fatal(err)
	}
	bs, err := h.Fetch(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if string(bs) != "logLevel: debug\n" {
		t.Fatalf("got %q, want the raw text body", bs)
	}
}

func TestHTTPNewValidation(t *testing.T) {
	cases := []struct {
		name string
		ops  HTTPOptions
	}{
		{name: "no urls", ops: HTTPOptions{}},
		{name: "bad url", ops: HTTPOptions{URLs: "://nope"}},
		{name: "not http", ops: HTTPOptions{URLs: "ftp://x/cfg"}},
		{name: "header without colon", ops: HTTPOptions{URLs: "http://x/cfg", Headers: "Authorization"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, err := NewHTTP(c.ops); err == nil {
				t.Fatal("want validation error")
			}
		})
	}
}

// The TLS transport must not pin the system cert pool when skipping verify.
var _ = tls.Config{}
