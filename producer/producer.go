package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/IBM/sarama"
)

const (
	streamInputFile = "../stream.jsonl"
	brokerAddress   = "localhost:9092"
	topic           = "cdc-events"
)

func main() {
	events, err := readFile()
	if err != nil {
		log.Fatalf("failed to read stream input events: %s", err)
	}
	err = publishToKafka(events)
	if err != nil {
		log.Fatalf("failed to publish events to kafka: %v", err)
	}
}

func readFile() ([]string, error) {
	file, err := os.Open(streamInputFile)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	reader := bufio.NewScanner(file)

	var messages []string
	for reader.Scan() {
		messages = append(messages, reader.Text())
	}

	if err := reader.Err(); err != nil {
		return nil, fmt.Errorf("error while reading file: %w", err)
	}

	return messages, nil
}

func publishToKafka(events []string) error {
	asyncProducer, err := sarama.NewAsyncProducer([]string{brokerAddress}, sarama.NewConfig())
	if err != nil {
		return fmt.Errorf("failed to create async kafka producer: %w", err)
	}
	defer asyncProducer.Close()
	shutDownSignal := make(chan os.Signal, 1)
	signal.Notify(shutDownSignal, os.Interrupt)

	eventsPublished := 0
	for _, event := range events {
		msg := &sarama.ProducerMessage{Topic: topic, Value: sarama.StringEncoder(event)}

		select {
		case asyncProducer.Input() <- msg:
			eventsPublished++
		case err := <-asyncProducer.Errors():
			return fmt.Errorf("recieved error from async producer: %w", err)
		case osSignal := <-shutDownSignal:
			log.Println("shutting down async producer due to os signal", osSignal)
			asyncProducer.Close()
			return nil
		}
	}

	log.Printf("successfully published %d events out of %d events", eventsPublished, len(events))
	return nil
}
