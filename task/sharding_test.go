package task

import (
	"testing"

	"github.com/housepower/clickhouse_sinker/model"
)

func TestSharderBufAlign(t *testing.T) {
	sh := &Sharder{shards: 1, msgBuf: []*model.Rows{}, msgMsgs: [][]*model.InputMessage{}}
	sh.reset(1) // 初始化 1 个 shard 的缓冲
	r1 := model.Row{1}
	r2 := model.Row{2}
	sh.putRaw(0, &r1, &model.InputMessage{Offset: 11})
	sh.putRaw(0, &r2, &model.InputMessage{Offset: 22})
	rows, msgs := sh.takeShard(0)
	if len(*rows) != 2 || len(msgs) != 2 {
		t.Fatalf("len rows=%d msgs=%d, want 2,2", len(*rows), len(msgs))
	}
	if msgs[0].Offset != 11 || msgs[1].Offset != 22 {
		t.Fatalf("msg alignment broken: %d,%d", msgs[0].Offset, msgs[1].Offset)
	}
}
