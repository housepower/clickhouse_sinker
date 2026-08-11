package task

import (
	"regexp"
	"testing"
	"time"

	"github.com/housepower/clickhouse_sinker/config"
	"github.com/housepower/clickhouse_sinker/model"
	"github.com/housepower/clickhouse_sinker/output"
	"github.com/housepower/clickhouse_sinker/parser"
	"golang.org/x/time/rate"
)

const arraySample = `{
	"__name__": "kubernetes.controller_manager.queue_work_unfinished_duration.sec",
	"__series_id": 1129336697220775,
	"__mgmt_id": 1101941880299034,
	"value": 0,
	"__labels_key__": ["host", "queue", "le", "__series_key__"],
	"__labels_value__": ["dev-21-47", "namespace", "0.5", "noise"],
	"objectType": "kubernetes"
}`

// newArrayTestService 手工装配一个 Service，绕开需要 ClickHouse 连接的 Init()。
// dims 布局与 initSeriesSchema 的产物一致：
// [value, __series_id, __mgmt_id, labels, objectType, __labels_key__, __labels_value__]
func newArrayTestService(t *testing.T, lblFilter labelFilter, withArray bool) *Service {
	t.Helper()
	taskCfg := &config.TaskConfig{Name: "test", PrometheusSchema: true}
	ck := output.NewClickHouse(&config.Config{}, taskCfg)
	ck.DimSerID = "__series_id"
	ck.DimMgmtID = "__mgmt_id"
	ck.SetSeriesQuota(&model.SeriesQuota{
		BmSeries:       make(map[int64]int64),
		NextResetQuota: time.Now().Add(time.Hour),
		Birth:          time.Now(),
	})

	str := func(name string, array bool) *model.ColumnWithType {
		return &model.ColumnWithType{Name: name, SourceName: name,
			Type: &model.TypeInfo{Type: model.String, Array: array}}
	}
	i64 := func(name string) *model.ColumnWithType {
		return &model.ColumnWithType{Name: name, SourceName: name, Type: &model.TypeInfo{Type: model.Int64}}
	}
	dims := []*model.ColumnWithType{
		{Name: "value", SourceName: "value", Type: &model.TypeInfo{Type: model.Float64}},
		i64("__series_id"),
		i64("__mgmt_id"),
		str("labels", false),
		str("objectType", false),
		str("__labels_key__", true),
		str("__labels_value__", true),
	}
	svc := &Service{
		clickhouse: ck,
		taskCfg:    taskCfg,
		dims:       dims,
		numDims:    len(dims),
		idxSerID:   1,
		nameKey:    "__name__",
		lblFilter:  lblFilter,
		idxLblKey:  -1,
		idxLblVal:  -1,
		// metric2Row 在数组长度不匹配时会用 lblLimiter 限流打日志，手工装配时必须给上，
		// 否则那条分支一走到就 nil pointer。
		lblLimiter: rate.NewLimiter(rate.Every(10*time.Second), 1),
	}
	if withArray {
		svc.idxLblKey = 5
		svc.idxLblVal = 6
	}
	return svc
}

func parseArraySample(t *testing.T) (model.Metric, func()) {
	t.Helper()
	pp, err := parser.NewParserPool("fastjson", nil, "", "", 1.0, "")
	if err != nil {
		t.Fatalf("NewParserPool failed: %v", err)
	}
	p, err := pp.Get()
	if err != nil {
		t.Fatalf("pp.Get failed: %v", err)
	}
	metric, err := p.Parse([]byte(arraySample))
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	return metric, func() { pp.Put(p) }
}

func TestMetric2RowBuildsLabelsFromArrays(t *testing.T) {
	svc := newArrayTestService(t, labelFilter{nameKey: "__name__"}, true)
	metric, done := parseArraySample(t)
	defer done()

	row := svc.metric2Row(metric, &model.InputMessage{})
	if row == nil {
		t.Fatal("metric2Row returned nil")
	}
	// le 被排除；objectType 是标量列，启用数组后不参与拼接
	want := `{"host": "dev-21-47", "queue": "namespace", "__series_key__": "noise"}`
	if got, _ := (*row)[3].(string); got != want {
		t.Fatalf("labels = %s, want %s", got, want)
	}
	// 数组列本身仍然原样落盘
	keys, ok := (*row)[5].([]string)
	if !ok || len(keys) != 4 || keys[0] != "host" {
		t.Fatalf("__labels_key__ column = %v (ok=%v), want the raw []string", (*row)[5], ok)
	}
}

func TestMetric2RowAppliesBlackListToArrayKeys(t *testing.T) {
	svc := newArrayTestService(t, labelFilter{
		nameKey: "__name__",
		blkList: regexp.MustCompile(`^__.*__$`),
	}, true)
	metric, done := parseArraySample(t)
	defer done()

	row := svc.metric2Row(metric, &model.InputMessage{})
	want := `{"host": "dev-21-47", "queue": "namespace"}`
	if got, _ := (*row)[3].(string); got != want {
		t.Fatalf("labels = %s, want %s", got, want)
	}
}

func TestMetric2RowWithoutArrayFallsBackToScalarColumns(t *testing.T) {
	svc := newArrayTestService(t, labelFilter{nameKey: "__name__"}, false)
	metric, done := parseArraySample(t)
	defer done()

	row := svc.metric2Row(metric, &model.InputMessage{})
	// 未启用数组时维持既有行为：只有标量 String 列进 labels，
	// __labels_key__/__labels_value__ 这类 Array 列必须被跳过而不是 panic。
	want := `{"objectType": "kubernetes"}`
	if got, _ := (*row)[3].(string); got != want {
		t.Fatalf("labels = %s, want %s", got, want)
	}
}

func TestMetric2RowArrayLengthMismatchTruncates(t *testing.T) {
	svc := newArrayTestService(t, labelFilter{nameKey: "__name__"}, true)
	pp, err := parser.NewParserPool("fastjson", nil, "", "", 1.0, "")
	if err != nil {
		t.Fatalf("NewParserPool failed: %v", err)
	}
	p, err := pp.Get()
	if err != nil {
		t.Fatalf("pp.Get failed: %v", err)
	}
	defer pp.Put(p)
	sample := `{"__series_id": 1, "__mgmt_id": 2, "value": 0,
		"__labels_key__": ["host", "queue", "extra"],
		"__labels_value__": ["dev-21-47"]}`
	metric, err := p.Parse([]byte(sample))
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	row := svc.metric2Row(metric, &model.InputMessage{})
	want := `{"host": "dev-21-47"}`
	if got, _ := (*row)[3].(string); got != want {
		t.Fatalf("labels = %s, want %s", got, want)
	}
}
