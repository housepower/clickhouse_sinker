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
	"strings"
	"sync"

	"github.com/Shopify/sarama"
	"github.com/wswz/go_commons/log"
)

// Kafka reader configuration
type Kafka struct {
	client  sarama.ConsumerGroup
	stopped chan struct{}
	msgs    chan ([]byte)

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

func (k *Kafka) Init() error {
	k.msgs = make(chan []byte, 300000)
	k.stopped = make(chan struct{})
	k.consumer = &Consumer{
		msgs:  k.msgs,
		ready: make(chan bool),
	}
	k.context, k.cancel = context.WithCancel(context.Background())
	return nil
}

func (k *Kafka) Msgs() chan []byte {
	return k.msgs
}

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
	if k.Sasl.Username != "" {
		config.Net.SASL.Enable = true
		config.Net.SASL.User = k.Sasl.Username
		config.Net.SASL.Password = k.Sasl.Password
	}
	if k.Earliest {
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
		for {
			if err := k.client.Consume(k.context, strings.Split(k.Topic, ","), k.consumer); err != nil {
				log.Error("Error from consumer: %v", err)
			}
			// check if context was cancelled, signaling that the consumer should stop
			if k.context.Err() != nil {
				return
			}
			k.consumer.ready = make(chan bool, 0)
		}
	}()

	<-k.consumer.ready
	return nil
}

func (k *Kafka) Stop() error {
	k.cancel()
	k.wg.Wait()

	k.client.Close()
	close(k.msgs)
	return nil
}

func (k *Kafka) Description() string {
	return "kafka consumer:" + k.Topic
}

func (k *Kafka) GetName() string {
	return k.Name
}

// Consumer represents a Sarama consumer group consumer
type Consumer struct {
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
