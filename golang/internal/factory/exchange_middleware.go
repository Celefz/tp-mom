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
		return err
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
		return err
	}
	return nil
}

func (e *ExchangeMiddleware) Send(msg m.Message) error {
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
			return err
		}
	}
	return nil
}

func (e *ExchangeMiddleware) Close() error {
	var finalErr error

	if err := e.Channel.Close(); err != nil && err != amqp.ErrClosed {
		finalErr = err
	}

	if err := e.Connection.Close(); err != nil && err != amqp.ErrClosed {
		finalErr = err
	}

	return finalErr
}
