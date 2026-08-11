package output

import (
	"context"
	"testing"
	"time"

	"github.com/housepower/clickhouse_sinker/model"
)

func TestSleepWithCtxCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if sleepWithCtx(ctx, time.Hour) {
		t.Fatal("sleepWithCtx should return false when ctx already canceled")
	}
}

func TestSleepWithCtxElapses(t *testing.T) {
	if !sleepWithCtx(context.Background(), 10*time.Millisecond) {
		t.Fatal("sleepWithCtx should return true when timer elapses")
	}
}

func strCol(name string, array bool) *model.ColumnWithType {
	return &model.ColumnWithType{Name: name, Type: &model.TypeInfo{Type: model.String, Array: array}}
}

func TestDetectNameKeySkipsArrayColumns(t *testing.T) {
	dims := []*model.ColumnWithType{
		{Name: "__series_id__", Type: &model.TypeInfo{Type: model.Int64}},
		{Name: "__mgmt_id__", Type: &model.TypeInfo{Type: model.Int64}},
		strCol("labels", false),
		strCol("__labels_key__", true),
		strCol("__name__", false),
	}
	if got := detectNameKey(dims, 3); got != "__name__" {
		t.Fatalf("detectNameKey = %q, want __name__", got)
	}
}

func TestDetectNameKeyDefaultsWhenNoScalarString(t *testing.T) {
	dims := []*model.ColumnWithType{
		{Name: "__series_id__", Type: &model.TypeInfo{Type: model.Int64}},
		{Name: "__mgmt_id__", Type: &model.TypeInfo{Type: model.Int64}},
		strCol("labels", false),
		strCol("__labels_key__", true),
	}
	if got := detectNameKey(dims, 3); got != "__name__" {
		t.Fatalf("detectNameKey = %q, want __name__", got)
	}
}

func TestLocatePromLabelsArrayDisabled(t *testing.T) {
	dims := []*model.ColumnWithType{strCol("labels", false)}
	k, v, err := locatePromLabelsArray(dims, 1, "", "")
	if err != nil || k != -1 || v != -1 {
		t.Fatalf("locatePromLabelsArray = %d,%d,%v; want -1,-1,nil", k, v, err)
	}
}

func TestLocatePromLabelsArrayFound(t *testing.T) {
	dims := []*model.ColumnWithType{
		strCol("labels", false),
		strCol("host", false),
		strCol("__labels_key__", true),
		strCol("__labels_value__", true),
	}
	k, v, err := locatePromLabelsArray(dims, 1, "__labels_key__", "__labels_value__")
	if err != nil {
		t.Fatalf("locatePromLabelsArray failed: %v", err)
	}
	if k != 2 || v != 3 {
		t.Fatalf("locatePromLabelsArray = %d,%d; want 2,3", k, v)
	}
}

func TestLocatePromLabelsArrayMissingColumn(t *testing.T) {
	dims := []*model.ColumnWithType{strCol("labels", false), strCol("__labels_key__", true)}
	if _, _, err := locatePromLabelsArray(dims, 1, "__labels_key__", "__labels_value__"); err == nil {
		t.Fatal("locatePromLabelsArray should fail when valueColumn is absent")
	}
}

func TestLocatePromLabelsArrayWrongType(t *testing.T) {
	dims := []*model.ColumnWithType{
		strCol("labels", false),
		strCol("__labels_key__", false), // 非 Array
		strCol("__labels_value__", true),
	}
	if _, _, err := locatePromLabelsArray(dims, 1, "__labels_key__", "__labels_value__"); err == nil {
		t.Fatal("locatePromLabelsArray should fail when keyColumn is not Array(String)")
	}
}

func TestLocatePromLabelsArrayBeforeStartIsInvisible(t *testing.T) {
	dims := []*model.ColumnWithType{
		strCol("__labels_key__", true),
		strCol("__labels_value__", true),
		strCol("labels", false),
	}
	if _, _, err := locatePromLabelsArray(dims, 2, "__labels_key__", "__labels_value__"); err == nil {
		t.Fatal("columns before start (the fixed series columns) must not be accepted")
	}
}
