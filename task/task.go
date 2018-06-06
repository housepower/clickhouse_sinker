package task

import (
	"fmt"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/houseflys/clickhouse_sinker/input"
	"github.com/houseflys/clickhouse_sinker/model"
	"github.com/houseflys/clickhouse_sinker/output"
	"github.com/houseflys/clickhouse_sinker/parser"

	"github.com/wswz/go_commons/log"
)

type TaskService struct {
	stopped    chan struct{}
	kafka      *input.Kafka
	clickhouse *output.ClickHouse
	p          parser.Parser
	sync.Mutex

	FlushInterval int
	BufferSize    int
}

func NewTaskService(kafka *input.Kafka, clickhouse *output.ClickHouse, p parser.Parser) *TaskService {
	return &TaskService{
		stopped:    make(chan struct{}),
		kafka:      kafka,
		clickhouse: clickhouse,
		p:          p,
	}
}

func (service *TaskService) Init() error {
	err := service.kafka.Init()
	if err != nil {
		return err
	}
	return service.clickhouse.Init()
}

func (service *TaskService) Run() {
	service.kafka.Start()

	log.Infof("TaskService %s TaskService has started", service.clickhouse.GetName())
	tick := time.NewTicker(time.Duration(service.FlushInterval) * time.Second)
	msgs := make([]model.LogKV, 0, 100000)
FOR:
	for {
		select {
		case msg, more := <-service.kafka.Msgs():
			if !more {
				break FOR
			}
			msgs = append(msgs, service.parse(msg))
			if len(msgs) >= service.BufferSize {
				service.Lock()
				service.flush(msgs)
				msgs = make([]model.LogKV, 0, 100000)
				tick = time.NewTicker(time.Duration(service.FlushInterval) * time.Second)
				service.Unlock()
			}
		case <-tick.C:
			log.Info(service.clickhouse.GetName() + " tick")
			if len(msgs) == 0 {
				continue
			}
			service.Lock()
			service.flush(msgs)
			msgs = make([]model.LogKV, 0, 100000)
			service.Unlock()
		}
	}
	service.flush(msgs)
	service.stopped <- struct{}{}
	return
}

func (service *TaskService) parse(data []byte) model.LogKV {
	return service.p.Parse(data)
}
func (service *TaskService) flush(metrics []model.LogKV) {
	log.Info("buf size:", len(metrics))
	err := service.clickhouse.Write(metrics)
	if err != nil {
		log.Error("saving msg error", err.Error())
	}
}

func (service *TaskService) Stop() {
	log.Info("close TaskService size:")
	service.kafka.Stop()
	<-service.stopped
	service.clickhouse.Close()
	log.Info("closed TaskService size:")
}

//获取goroutine的id
func GoID() int {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	idField := strings.Fields(strings.TrimPrefix(string(buf[:n]), "goroutine "))[0]
	id, err := strconv.Atoi(idField)
	if err != nil {
		panic(fmt.Sprintf("cannot get goroutine id: %v", err))
	}
	return id
}
