package rcm

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

// fakePortal mimics the Apollo Portal Open API for one namespace.
type fakePortal struct {
	mu          sync.Mutex
	edited      string // latest item value, possibly unreleased
	modifiedBy  string // who made the latest item change
	released    string
	failRelease bool
	puts        int
	paths       []string // escaped request paths
	lastItem    map[string]string
	srv         *httptest.Server
}

func newFakePortal(t *testing.T, edited, released string) *fakePortal {
	p := &fakePortal{edited: edited, released: released, modifiedBy: "alice"}
	p.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p.mu.Lock()
		defer p.mu.Unlock()
		p.paths = append(p.paths, r.URL.EscapedPath())
		if r.Header.Get("Authorization") != "tok" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		path := r.URL.Path
		switch {
		case r.Method == http.MethodGet && strings.HasSuffix(path, "/releases/latest"):
			_ = json.NewEncoder(w).Encode(map[string]any{"configurations": map[string]string{apolloContentKey: p.released}})
		case r.Method == http.MethodGet:
			_ = json.NewEncoder(w).Encode(map[string]any{"items": []map[string]string{{
				"key": apolloContentKey, "value": p.edited, "dataChangeLastModifiedBy": p.modifiedBy,
			}}})
		case r.Method == http.MethodPut && strings.HasSuffix(path, "/items/"+apolloContentKey):
			var item map[string]string
			_ = json.NewDecoder(r.Body).Decode(&item)
			p.lastItem = item
			p.edited = item["value"]
			p.modifiedBy = item["dataChangeLastModifiedBy"]
			p.puts++
		case r.Method == http.MethodPost && strings.HasSuffix(path, "/releases"):
			if p.failRelease {
				http.Error(w, "release denied", http.StatusForbidden)
				return
			}
			p.released = p.edited
			_, _ = w.Write([]byte("{}"))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(p.srv.Close)
	return p
}

func testOpenAPIOptions(portal string) ApolloOptions {
	return ApolloOptions{AppID: "sinker", Cluster: "default", Namespace: testApolloNS,
		Portal: portal, Token: "tok", Env: "PRO", Operator: "apollo"}
}

func TestApolloOpenAPIUpdate(t *testing.T) {
	p := newFakePortal(t, "old", "old")
	w := newApolloOpenAPI(testOpenAPIOptions(p.srv.URL))
	var seen string
	err := w.Update(context.Background(), func(old []byte) ([]byte, error) {
		seen = string(old)
		return []byte("new"), nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if seen != "old" || p.edited != "new" || p.released != "new" {
		t.Fatalf("seen=%q edited=%q released=%q", seen, p.edited, p.released)
	}
	if p.lastItem["dataChangeLastModifiedBy"] != "apollo" {
		t.Fatalf("operator not sent: %v", p.lastItem)
	}
}

func TestApolloOpenAPIPendingChanges(t *testing.T) {
	p := newFakePortal(t, "half-done edit", "released")
	w := newApolloOpenAPI(testOpenAPIOptions(p.srv.URL))
	err := w.Update(context.Background(), func([]byte) ([]byte, error) { return []byte("new"), nil })
	if !errors.Is(err, ErrPendingChanges) {
		t.Fatalf("want ErrPendingChanges, got %v", err)
	}
	if p.puts != 0 || p.released != "released" {
		t.Fatalf("nothing may be written or released, puts=%d released=%q", p.puts, p.released)
	}
}

func TestApolloOpenAPINoChangeSkipsWrite(t *testing.T) {
	p := newFakePortal(t, "same", "same")
	w := newApolloOpenAPI(testOpenAPIOptions(p.srv.URL))
	if err := w.Update(context.Background(), func(old []byte) ([]byte, error) { return old, nil }); err != nil {
		t.Fatal(err)
	}
	if p.puts != 0 {
		t.Fatalf("puts=%d, want 0", p.puts)
	}
}

func TestApolloOpenAPIReleaseFailure(t *testing.T) {
	p := newFakePortal(t, "old", "old")
	p.failRelease = true
	w := newApolloOpenAPI(testOpenAPIOptions(p.srv.URL))
	err := w.Update(context.Background(), func([]byte) ([]byte, error) { return []byte("new"), nil })
	if err == nil || !strings.Contains(err.Error(), "NOT released") || !strings.Contains(err.Error(), "release denied") {
		t.Fatalf("want an error telling the edit is unreleased, got %v", err)
	}
}

func TestApolloOpenAPIEscapesPath(t *testing.T) {
	p := newFakePortal(t, "x", "x")
	o := testOpenAPIOptions(p.srv.URL)
	o.Cluster = "dc 1"
	o.Namespace = "sinker conf.txt"
	w := newApolloOpenAPI(o)
	_ = w.Update(context.Background(), func(old []byte) ([]byte, error) { return old, nil })
	if len(p.paths) == 0 || !strings.Contains(p.paths[0], "/clusters/dc%201/namespaces/sinker%20conf.txt") {
		t.Fatalf("path not escaped: %v", p.paths)
	}
}

func TestApolloWriteConfigured(t *testing.T) {
	none := ApolloOptions{}
	if ok, err := none.writeConfigured(); ok || err != nil {
		t.Fatalf("none: got %v, %v", ok, err)
	}
	all := ApolloOptions{Portal: "p", Token: "t", Env: "PRO", Operator: "o"}
	if ok, err := all.writeConfigured(); !ok || err != nil {
		t.Fatalf("all: got %v, %v", ok, err)
	}
	partial := ApolloOptions{Portal: "p", Token: "t"}
	if _, err := partial.writeConfigured(); err == nil || !strings.Contains(err.Error(), "--apollo-operator") {
		t.Fatalf("partial: want error listing the flags, got %v", err)
	}
}

// An HTML login page or proxy error must not flood the log line.
func TestApolloOpenAPIErrorBodyIsTruncated(t *testing.T) {
	big := strings.Repeat("x", 4096)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, big, http.StatusUnauthorized)
	}))
	defer srv.Close()
	w := newApolloOpenAPI(testOpenAPIOptions(srv.URL))
	err := w.Update(context.Background(), func(old []byte) ([]byte, error) { return old, nil })
	if err == nil {
		t.Fatal("want an error for HTTP 401")
	}
	if len(err.Error()) > 1024 || !strings.Contains(err.Error(), "HTTP 401") || !strings.Contains(err.Error(), "truncated") {
		t.Fatalf("error not truncated (len %d): %.200s", len(err.Error()), err)
	}
}

func TestApolloOptionsPlainHTTPPortal(t *testing.T) {
	for portal, want := range map[string]bool{
		"http://portal:8070":  true,
		"HTTP://portal:8070":  true,
		"https://portal:8070": false,
		"":                    false,
	} {
		if got := (ApolloOptions{Portal: portal}).plainHTTPPortal(); got != want {
			t.Fatalf("plainHTTPPortal(%q) = %v, want %v", portal, got, want)
		}
	}
}

// A previous write-back that was modified but not released leaves an
// unreleased edit by our own operator. The next write-back must release it,
// even when it has nothing new to add, instead of waiting for it forever.
func TestApolloOpenAPIReleasesOwnPendingEdit(t *testing.T) {
	p := newFakePortal(t, "patched", "old")
	p.modifiedBy = "apollo" // testOpenAPIOptions' operator
	w := newApolloOpenAPI(testOpenAPIOptions(p.srv.URL))
	var seen string
	err := w.Update(context.Background(), func(old []byte) ([]byte, error) {
		seen = string(old)
		return nil, ErrNoChange
	})
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	if seen != "patched" || p.released != "patched" || p.puts != 0 {
		t.Fatalf("seen=%q released=%q puts=%d; want our pending edit released as is", seen, p.released, p.puts)
	}
}

func TestApolloOpenAPIReleaseAfterOwnPendingEditChanges(t *testing.T) {
	p := newFakePortal(t, "patched", "old")
	p.modifiedBy = "apollo"
	w := newApolloOpenAPI(testOpenAPIOptions(p.srv.URL))
	if err := w.Update(context.Background(), func([]byte) ([]byte, error) { return []byte("patched-again"), nil }); err != nil {
		t.Fatal(err)
	}
	if p.released != "patched-again" {
		t.Fatalf("released=%q, want patched-again", p.released)
	}
}
