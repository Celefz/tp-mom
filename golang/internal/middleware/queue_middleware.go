package middleware

import (
	amqp "github.com/rabbitmq/amqp091-go"
)

type QueueMiddleware struct {
	Connection *amqp.Connection
	Channel    *amqp.Channel
	Queue      amqp.Queue
}

func (q *QueueMiddleware) StartConsuming(callbackFunc func(msg Message, ack func(), nack func())) error {
	return nil
}

func (q *QueueMiddleware) StopConsuming() error {
	return nil
}

func (q *QueueMiddleware) Send(msg Message) error {
	return nil
}

func (q *QueueMiddleware) Close() error {
	return nil
}
