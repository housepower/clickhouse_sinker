package input

import (
	"strings"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	"github.com/wswz/go_commons/log"
)

type Kafka struct {
	consumer *cluster.Consumer
	stopped  chan struct{}
	msgs     chan ([]byte)

	Name          string
	Brokers       string
	ConsumerGroup string
	Topic         string

	Sasl struct {
		Username string
		Password string
	}
}

func NewKafka() *Kafka {
	return &Kafka{}
}

func (k *Kafka) Init() error {
	k.msgs = make(chan []byte, 300000)
	k.stopped = make(chan struct{})
	return nil
}

func (k *Kafka) Msgs() chan []byte {
	return k.msgs
}

func (k *Kafka) Start() error {

	config := cluster.NewConfig()
	config.Version = sarama.V1_0_0_0
	config.Consumer.Return.Errors = true
	if k.Sasl.Username != "" {
		config.Net.SASL.Enable = true
		config.Net.SASL.User = k.Sasl.Username
		config.Net.SASL.Password = k.Sasl.Password
	}
	consumer, err := cluster.NewConsumer(strings.Split(k.Brokers, ","), k.ConsumerGroup, []string{k.Topic}, config)
	if err != nil {
		return err
	}
	k.consumer = consumer
	go func() {
		log.Info("Start kafka services", k.Name)
	FOR:
		for {
			select {
			case msg, more := <-k.consumer.Messages():
				if !more {
					break FOR
				}
				k.msgs <- msg.Value
				k.consumer.MarkOffset(msg, "") // mark message as processed

			case err, more := <-k.consumer.Errors():
				if more {
					log.Errorf("Error: %s\n", err.Error())
				}
			}
		}
		close(k.stopped)
	}()
	return nil
}

func (k *Kafka) Stop() error {
	k.consumer.Close()
	close(k.msgs)
	<-k.stopped
	return nil
}

func (k *Kafka) Description() string {
	return "kafka consumer:" + k.Topic
}

func (k *Kafka) GetName() string {
	return k.Name
}
