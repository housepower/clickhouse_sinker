package rcm

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

const testApolloNS = "clickhouse_sinker.txt"

// fakeConfigService mimics the Apollo Config Service endpoints agollo uses.
type fakeConfigService struct {
	mu      sync.Mutex
	content string
	notifID int
	srv     *httptest.Server
}

func (f *fakeConfigService) set(content string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.content = content
	f.notifID++
}

func newFakeConfigService(t *testing.T, content string) *fakeConfigService {
	f := &fakeConfigService{content: content, notifID: 1}
	mux := http.NewServeMux()
	mux.HandleFunc("/services/config", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode([]map[string]string{{
			"appName": "APOLLO-CONFIGSERVICE", "instanceId": "fake", "homepageUrl": f.srv.URL + "/",
		}})
	})
	mux.HandleFunc("/configfiles/json/", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		_ = json.NewEncoder(w).Encode(map[string]string{apolloContentKey: f.content})
	})
	mux.HandleFunc("/configs/", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		_ = json.NewEncoder(w).Encode(map[string]any{
			"appId": "sinker", "cluster": "default", "namespaceName": testApolloNS,
			"configurations": map[string]string{apolloContentKey: f.content},
			"releaseKey":     fmt.Sprint(f.notifID),
		})
	})
	// Long poll: answer as soon as the notification id moves, else 304 after 500ms.
	mux.HandleFunc("/notifications/v2", func(w http.ResponseWriter, r *http.Request) {
		var req []struct {
			NotificationID int `json:"notificationId"`
		}
		_ = json.Unmarshal([]byte(r.URL.Query().Get("notifications")), &req)
		for deadline := time.Now().Add(500 * time.Millisecond); time.Now().Before(deadline); time.Sleep(50 * time.Millisecond) {
			f.mu.Lock()
			id := f.notifID
			f.mu.Unlock()
			if len(req) > 0 && id != req[0].NotificationID {
				_ = json.NewEncoder(w).Encode([]map[string]any{{"namespaceName": testApolloNS, "notificationId": id}})
				return
			}
		}
		w.WriteHeader(http.StatusNotModified)
	})
	f.srv = httptest.NewServer(mux)
	t.Cleanup(f.srv.Close)
	return f
}

func TestApolloSourceFetchAndHotReload(t *testing.T) {
	f := newFakeConfigService(t, "{kafka: {brokers: \"k1\"}}")
	src, err := NewApolloSource(ApolloOptions{Meta: f.srv.URL, AppID: "sinker", Namespace: testApolloNS})
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()

	got, err := src.Fetch(context.Background())
	if err != nil || string(got) != "{kafka: {brokers: \"k1\"}}" {
		t.Fatalf("initial fetch got %q, %v", got, err)
	}

	f.set("{kafka: {brokers: \"k2\"}}")
	deadline := time.Now().Add(15 * time.Second)
	for {
		got, err = src.Fetch(context.Background())
		if err == nil && string(got) == "{kafka: {brokers: \"k2\"}}" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("change not observed, last fetch %q, %v", got, err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func TestApolloSourceUnreachableFailsFast(t *testing.T) {
	_, err := NewApolloSource(ApolloOptions{Meta: "http://127.0.0.1:1", AppID: "sinker", Namespace: testApolloNS})
	if err == nil {
		t.Fatal("start must fail when Apollo is unreachable instead of using a stale backup")
	}
	if !strings.Contains(err.Error(), testApolloNS) {
		t.Fatalf("error must name the namespace, got %v", err)
	}
}

func TestApolloOptionsValidateRead(t *testing.T) {
	ok := ApolloOptions{Meta: "http://m", AppID: "a", Namespace: "n"}
	if err := ok.validateRead(); err != nil {
		t.Fatal(err)
	}
	for _, o := range []ApolloOptions{
		{AppID: "a", Namespace: "n"},
		{Meta: "http://m", Namespace: "n"},
	} {
		if err := o.validateRead(); err == nil {
			t.Fatalf("%+v must be rejected", o)
		}
	}
}

// agollo reads ./<appId>-<namespace>.json on sync failure even with
// IsBackupConfig=false; start-up must not accept that stale file.
func TestApolloSourceIgnoresDiskBackup(t *testing.T) {
	t.Chdir(t.TempDir())
	stale := `{"appId":"sinker","cluster":"default","namespaceName":"` + testApolloNS +
		`","releaseKey":"1","configurations":{"content":"{stale: true}"}}`
	if err := os.WriteFile("sinker-"+testApolloNS+".json", []byte(stale), 0o600); err != nil {
		t.Fatal(err)
	}
	src, err := NewApolloSource(ApolloOptions{Meta: "http://127.0.0.1:1", AppID: "sinker", Namespace: testApolloNS})
	if err == nil {
		got, _ := src.Fetch(context.Background())
		src.Close()
		t.Fatalf("start must fail instead of loading the disk backup, fetched %q", got)
	}
}
