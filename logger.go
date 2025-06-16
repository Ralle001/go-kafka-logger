package gokafklogger

import (
	"encoding/json"
	"log"
	"time"

	"github.com/IBM/sarama"
)

func SendLogToKafka(producer sarama.SyncProducer, service, level, message string) {
	logEntry := map[string]interface{}{
		"level":     level,
		"service":   service,
		"message":   message,
		"timestamp": time.Now().Format(time.RFC3339),
	}
	msgBytes, _ := json.Marshal(logEntry)
	_, _, err := producer.SendMessage(&sarama.ProducerMessage{
		Topic: "logs",
		Value: sarama.ByteEncoder(msgBytes),
	})
	if err != nil {
		log.Printf("Failed to send log message to Kafka: %v", err)
	}
}
