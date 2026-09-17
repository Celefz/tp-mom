package factory

import (
	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)

const EXCHANGE_CONSUMER_TAG = "exchange_consumer"

type ExchangeMiddleware struct {
	Connection *amqp.Connection
	Channel    *amqp.Channel
	Exchange   string
	Queue      amqp.Queue
	Keys       []string
}

func (e *ExchangeMiddleware) StartConsuming(callbackFunc func(msg m.Message, ack func(), nack func())) error {
	if e.Channel.IsClosed() || e.Connection.IsClosed() {
		return m.ErrMessageMiddlewareDisconnected
	}

	messages, err := e.Channel.Consume(
		e.Queue.Name,
		EXCHANGE_CONSUMER_TAG,
		false,
		false,
		false,
		false,
		nil,
	)

	if err != nil {
		return m.ErrMessageMiddlewareMessage
	}

	go func() {
		for message := range messages {
			callbackFunc(
				m.Message{Body: string(message.Body)},
				func() { message.Ack(false) },
				func() { message.Nack(false, true) },
			)
		}
	}()
	return nil
}

func (e *ExchangeMiddleware) StopConsuming() error {
	if err := e.Channel.Cancel(EXCHANGE_CONSUMER_TAG, false); err != nil {
		return m.ErrMessageMiddlewareDisconnected
	}
	return nil
}

func (e *ExchangeMiddleware) Send(msg m.Message) error {
	if e.Channel.IsClosed() || e.Connection.IsClosed() {
		return m.ErrMessageMiddlewareDisconnected
	}

	for _, key := range e.Keys {
		err := e.Channel.Publish(
			e.Exchange,
			key,
			false,
			false,
			amqp.Publishing{
				ContentType: "text/plain",
				Body:        []byte(msg.Body),
			},
		)
		if err != nil {
			return m.ErrMessageMiddlewareMessage
		}
	}
	return nil
}

func (e *ExchangeMiddleware) Close() error {
	channelErr := e.Channel.Close()
	connectionErr := e.Connection.Close()

	if channelErr != nil || connectionErr != nil {
		return m.ErrMessageMiddlewareClose
	}

	return nil
}
