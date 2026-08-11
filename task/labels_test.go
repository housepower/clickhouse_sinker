package task

import (
	"regexp"
	"testing"
)

func TestBuildLabelsJSONFormat(t *testing.T) {
	pairs := []labelPair{{"host", "dev-21-47"}, {"queue", "namespace"}}
	got := buildLabelsJSON(pairs, labelFilter{})
	want := `{"host": "dev-21-47", "queue": "namespace"}`
	if got != want {
		t.Fatalf("buildLabelsJSON = %s, want %s", got, want)
	}
}

func TestBuildLabelsJSONEmpty(t *testing.T) {
	if got := buildLabelsJSON(nil, labelFilter{}); got != "{}" {
		t.Fatalf("buildLabelsJSON(nil) = %s, want {}", got)
	}
}

func TestBuildLabelsJSONQuotesSpecialChars(t *testing.T) {
	pairs := []labelPair{{`a"b`, "c\\d"}}
	got := buildLabelsJSON(pairs, labelFilter{})
	want := `{"a\"b": "c\\d"}`
	if got != want {
		t.Fatalf("buildLabelsJSON = %s, want %s", got, want)
	}
}

func TestBuildLabelsJSONDedupKeepsFirst(t *testing.T) {
	pairs := []labelPair{{"host", "first"}, {"host", "second"}}
	got := buildLabelsJSON(pairs, labelFilter{})
	want := `{"host": "first"}`
	if got != want {
		t.Fatalf("buildLabelsJSON = %s, want %s", got, want)
	}
}

func TestBuildLabelsJSONExcludesNameKeyAndLe(t *testing.T) {
	pairs := []labelPair{
		{"__name__", "some.metric"},
		{"le", "0.5"},
		{"host", "dev-21-47"},
	}
	got := buildLabelsJSON(pairs, labelFilter{nameKey: "__name__"})
	want := `{"host": "dev-21-47"}`
	if got != want {
		t.Fatalf("buildLabelsJSON = %s, want %s", got, want)
	}
}

func TestBuildLabelsJSONExcludesBlackList(t *testing.T) {
	pairs := []labelPair{{"__series_key__", "x"}, {"host", "dev-21-47"}}
	f := labelFilter{blkList: regexp.MustCompile(`^__.*__$`)}
	got := buildLabelsJSON(pairs, f)
	want := `{"host": "dev-21-47"}`
	if got != want {
		t.Fatalf("buildLabelsJSON = %s, want %s", got, want)
	}
}

func TestBuildLabelsJSONKeepsEmptyValue(t *testing.T) {
	pairs := []labelPair{{"host", ""}}
	got := buildLabelsJSON(pairs, labelFilter{})
	want := `{"host": ""}`
	if got != want {
		t.Fatalf("buildLabelsJSON = %s, want %s", got, want)
	}
}

func TestPairLabelArraysEqualLength(t *testing.T) {
	pairs, dropped := pairLabelArrays([]string{"a", "b"}, []string{"1", "2"})
	if dropped != 0 {
		t.Fatalf("dropped = %d, want 0", dropped)
	}
	if len(pairs) != 2 || pairs[0] != (labelPair{"a", "1"}) || pairs[1] != (labelPair{"b", "2"}) {
		t.Fatalf("pairs = %v, want [{a 1} {b 2}]", pairs)
	}
}

func TestPairLabelArraysMoreKeysThanValues(t *testing.T) {
	pairs, dropped := pairLabelArrays([]string{"a", "b", "c"}, []string{"1"})
	if dropped != 2 {
		t.Fatalf("dropped = %d, want 2", dropped)
	}
	if len(pairs) != 1 || pairs[0] != (labelPair{"a", "1"}) {
		t.Fatalf("pairs = %v, want [{a 1}]", pairs)
	}
}

func TestPairLabelArraysMoreValuesThanKeys(t *testing.T) {
	pairs, dropped := pairLabelArrays([]string{"a"}, []string{"1", "2", "3"})
	if dropped != 2 {
		t.Fatalf("dropped = %d, want 2", dropped)
	}
	if len(pairs) != 1 || pairs[0] != (labelPair{"a", "1"}) {
		t.Fatalf("pairs = %v, want [{a 1}]", pairs)
	}
}

func TestPairLabelArraysEmpty(t *testing.T) {
	pairs, dropped := pairLabelArrays(nil, nil)
	if len(pairs) != 0 || dropped != 0 {
		t.Fatalf("pairs = %v, dropped = %d, want empty/0", pairs, dropped)
	}
}
