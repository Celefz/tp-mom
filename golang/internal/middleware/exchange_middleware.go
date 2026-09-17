package middleware

import (
	amqp "github.com/rabbitmq/amqp091-go"
)

type ExchangeMiddleware struct {
	Connection *amqp.Connection
	Channel    *amqp.Channel
	Exchange   string
}

func (e *ExchangeMiddleware) StartConsuming(callbackFunc func(msg Message, ack func(), nack func())) error {
	return nil
}

func (e *ExchangeMiddleware) StopConsuming() error {
	return nil
}

func (e *ExchangeMiddleware) Send(msg Message) error {
	return nil
}

func (e *ExchangeMiddleware) Close() error {
	return nil
}
