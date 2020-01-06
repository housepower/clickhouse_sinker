/*Copyright [2019] housepower

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package input

import (
	"context"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/heptiolabs/healthcheck"
	"github.com/housepower/clickhouse_sinker/health"
	"github.com/housepower/clickhouse_sinker/prom"
	"strings"
	"sync"
	"time"

	"github.com/sundy-li/go_commons/log"
)

type ConsumerError struct {
	UnixTime int64
	Error    error
}

var kafkaConsumerErrors sync.Map

// Kafka reader configuration
type Kafka struct {
	client  sarama.ConsumerGroup
	stopped chan struct{}
	msgs    chan []byte

	Name          string
	Version       string
	Earliest      bool
	Brokers       string
	ConsumerGroup string
	Topic         string

	Sasl struct {
		Username string
		Password string
	}

	consumer *Consumer
	context  context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
}

// NewKafka get instance of kafka reader
func NewKafka() *Kafka {
	return &Kafka{}
}

// Init Initialise the kafka instance with configuration
func (k *Kafka) Init() error {
	k.msgs = make(chan []byte, 300000)
	k.stopped = make(chan struct{})
	k.consumer = &Consumer{
		Name:  k.Name,
		msgs:  k.msgs,
		ready: make(chan bool),
	}
	k.context, k.cancel = context.WithCancel(context.Background())
	return nil
}

// Msgs returns the message from kafka
func (k *Kafka) Msgs() chan []byte {
	return k.msgs
}

func ConsumerHealthCheck(consumerName string) healthcheck.Check {
	return func() error {
		result, _ := kafkaConsumerErrors.Load(consumerName)
		v := result.(ConsumerError)
		thresholdTime := v.UnixTime + 5 // sec
		if v.Error != nil && thresholdTime > time.Now().Unix() {
			return v.Error
		}
		return nil
	}
}

// Start start kafka consumer, uses saram library
func (k *Kafka) Start() error {
	config := sarama.NewConfig()

	if k.Version != "" {
		version, err := sarama.ParseKafkaVersion(k.Version)
		if err != nil {
			return err
		}
		config.Version = version
	}
	// sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)
	// check for authentication
	if k.Sasl.Username != "" {
		config.Net.SASL.Enable = true
		config.Net.SASL.User = k.Sasl.Username
		config.Net.SASL.Password = k.Sasl.Password
	}
	if k.Earliest { // set to read the oldest message from last commit point
		config.Consumer.Offsets.Initial = sarama.OffsetOldest
	}

	log.Info("start to dial kafka ", k.Brokers)
	client, err := sarama.NewConsumerGroup(strings.Split(k.Brokers, ","), k.ConsumerGroup, config)
	if err != nil {
		return err
	}

	k.client = client

	go func() {
		k.wg.Add(1)
		defer k.wg.Done()

		consumerName := fmt.Sprintf("kafka_consumer(%s, %s)", k.ConsumerGroup, k.Topic)
		kafkaConsumerErrors.Store(consumerName, ConsumerError{UnixTime: 0, Error: nil})
		health.Health.AddReadinessCheck(consumerName, ConsumerHealthCheck(consumerName))

		for {
			if err := k.client.Consume(k.context, strings.Split(k.Topic, ","), k.consumer); err != nil {
				kafkaConsumerErrors.Store(consumerName, ConsumerError{UnixTime: time.Now().Unix(), Error: err})
				prom.KafkaConsumerErrors.WithLabelValues(k.Topic, k.ConsumerGroup).Inc()
				log.Error("Error from consumer: %v", err)
			}
			// check if context was cancelled, signaling that the consumer should stop
			if k.context.Err() != nil {
				kafkaConsumerErrors.Delete(consumerName)
				return
			}
			k.consumer.ready = make(chan bool, 0)
		}
	}()

	<-k.consumer.ready
	return nil
}

// Stop kafka consumer and close all connections
func (k *Kafka) Stop() error {
	k.cancel()
	k.wg.Wait()

	_ = k.client.Close()
	close(k.msgs)
	return nil
}

// Description of this kafka consumre, which topic it reads from
func (k *Kafka) Description() string {
	return "kafka consumer:" + k.Topic
}

// GetName name of this kafka consumer instance
func (k *Kafka) GetName() string {
	return k.Name
}

// Consumer represents a Sarama consumer group consumer
type Consumer struct {
	Name  string
	ready chan bool
	msgs  chan []byte
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (consumer *Consumer) Setup(sarama.ConsumerGroupSession) error {
	// Mark the consumer as ready
	close(consumer.ready)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (consumer *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (consumer *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {

	// NOTE:
	// Do not move the code below to a goroutine.
	// The `ConsumeClaim` itself is called within a goroutine, see:
	// https://github.com/Shopify/sarama/blob/master/consumer_group.go#L27-L29
	for message := range claim.Messages() {
		consumer.msgs <- message.Value
		session.MarkMessage(message, "")
	}

	return nil
}
