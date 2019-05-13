package input

import (
	"context"
	"strings"

	"github.com/Shopify/sarama"
)

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
}

func NewKafka() *Kafka {
	return &Kafka{}
}

func (k *Kafka) Init() error {
	k.msgs = make(chan []byte, 300000)
	k.stopped = make(chan struct{})
	k.consumer = &Consumer{
		msgs:  k.msgs,
		ready: make(chan struct{}),
	}
	return nil
}

func (k *Kafka) Msgs() chan []byte {
	return k.msgs
}

func (k *Kafka) Start() error {
	config := sarama.NewConfig()

	if k.Version == "" {
		k.Version = "2.0.0"
	}
	// sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)
	version, err := sarama.ParseKafkaVersion(k.Version)
	if err != nil {
		return err
	}
	config.Version = version
	if k.Sasl.Username != "" {
		config.Net.SASL.Enable = true
		config.Net.SASL.User = k.Sasl.Username
		config.Net.SASL.Password = k.Sasl.Password
	}
	if k.Earliest {
		config.Consumer.Offsets.Initial = sarama.OffsetOldest
	}

	client, err := sarama.NewConsumerGroup(strings.Split(k.Brokers, ","), k.ConsumerGroup, config)
	if err != nil {
		return err
	}

	k.client = client
	ctx := context.Background()
	go func() {
		k.client.Consume(ctx, strings.Split(k.Topic, ","), k.consumer)
	}()
	<-k.consumer.ready
	return nil
}

func (k *Kafka) Stop() error {
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
	ready chan struct{}
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
