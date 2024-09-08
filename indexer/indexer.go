package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"strings"

	"github.com/IBM/sarama"
	"github.com/opensearch-project/opensearch-go"
	"github.com/opensearch-project/opensearch-go/opensearchapi"
)

const (
	brokerAddress     = "localhost:9092"
	esAddress         = "http://localhost:9200"
	topic             = "cdc-events"
	indexName         = "cdc"
	consumerGroupName = "cdc-indexer"
)

func main() {
	err := consumeAndIndexEvents()
	if err != nil {
		log.Fatalf("failed to consumer and index events: %v", err)
	}
}

func consumeAndIndexEvents() error {
	cfg := sarama.NewConfig()
	cfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	consumerGroup, err := sarama.NewConsumerGroup([]string{brokerAddress}, consumerGroupName, cfg)
	if err != nil {
		return fmt.Errorf("failed to create consumer group: %w", err)
	}
	defer consumerGroup.Close()

	// Track errors
	go func() {
		for err := range consumerGroup.Errors() {
			log.Println("ERROR", err)
		}
	}()

	ctx := context.Background()
	for {
		topics := []string{topic}
		handler, err := NewConsumerGroupHandlerImpl()
		if err != nil {
			return fmt.Errorf("failed to init consumer group handler: %w", err)
		}

		log.Println("starting consumer")

		// `Consume` should be called inside an infinite loop, when a
		// server-side rebalance happens, the consumer session will need to be
		// recreated to get the new claims
		err = consumerGroup.Consume(ctx, topics, handler)
		if err != nil {
			return fmt.Errorf("failed to start consuming from consumer group")
		}
	}
}

type ConsumerGroupHandlerImpl struct {
	openSearchClient *opensearch.Client
}

func NewConsumerGroupHandlerImpl() (*ConsumerGroupHandlerImpl, error) {
	openSearchClient, err := opensearch.NewClient(opensearch.Config{
		Addresses: []string{esAddress},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to init opensearch client: %w", err)
	}

	return &ConsumerGroupHandlerImpl{
		openSearchClient: openSearchClient,
	}, nil
}

// Setup is run at the beginning of a new session, before ConsumeClaim.
func (c *ConsumerGroupHandlerImpl) Setup(session sarama.ConsumerGroupSession) error {
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
// but before the offsets are committed for the very last time.
func (c *ConsumerGroupHandlerImpl) Cleanup(session sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
// Once the Messages() channel is closed, the Handler must finish its processing
// loop and exit.
func (c *ConsumerGroupHandlerImpl) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {

	for message := range claim.Messages() {
		event := &Event{}
		err := json.Unmarshal(message.Value, event)
		if err != nil {
			return fmt.Errorf("failed to unmarshall event json: %w", err)
		}

		reader := strings.NewReader(string(message.Value))

		indexRequest := opensearchapi.IndexRequest{
			Index:      indexName,
			DocumentID: url.PathEscape(event.After.Key),
			Body:       reader,
		}
		res, err := indexRequest.Do(session.Context(), c.openSearchClient)
		if err != nil {
			log.Println("failed to index the request", err)
			return fmt.Errorf("failed to index the request: %w", err)
		}

		if res != nil && res.StatusCode != 200 {
			log.Println("not ok status recieved while indexing event", res.StatusCode, res.String())
			return fmt.Errorf("not ok status %d recieved while indexing event", res.StatusCode)
		}
		log.Println("successfully indexed ", event.After.Key)

		session.MarkMessage(message, "")
	}

	return nil
}
