package gokafklogger

import (
	"encoding/json"
	"log"
	"time"

	"github.com/IBM/sarama"
)

func SendLogToKafka(producer sarama.SyncProducer, service, level, message string) error {
	logEntry := map[string]interface{}{
		"level":     level,
		"service":   service,
		"message":   message,
		"timestamp": time.Now().Format(time.RFC3339),
	}
	msgBytes, errorJson := json.Marshal(logEntry)
	if errorJson != nil {
		log.Printf("Failed to marshal log entry: %v", errorJson)
		return errorJson
	}

	const maxRetries = 5
	var err error
	for i := 0; i < maxRetries; i++ {
		_, _, err = producer.SendMessage(&sarama.ProducerMessage{
			Topic: "logs",
			Value: sarama.ByteEncoder(msgBytes),
		})
		if err == nil {
			return nil
		}
		log.Printf("Failed to send log message to Kafka: %v", err)
		time.Sleep(time.Second * time.Duration(i)) // Exponential backoff
	}
	return err
}
