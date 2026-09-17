package factory

import (
	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)

const CONSUMER_TAG_SUFFIX = "-consumer"

type ExchangeMiddleware struct {
	connection  *amqp.Connection
	channel     *amqp.Channel
	exchange    string
	queue       amqp.Queue
	keys        []string
	consumerTag string
}

func NewExchangeMiddleware(
	conn *amqp.Connection,
	channel *amqp.Channel,
	exchangeName string,
	queue amqp.Queue,
	keys []string,
) *ExchangeMiddleware {
	consumerTag := queue.Name + CONSUMER_TAG_SUFFIX

	return &ExchangeMiddleware{
		conn,
		channel,
		exchangeName,
		queue,
		keys,
		consumerTag,
	}
}

func (e *ExchangeMiddleware) StartConsuming(callbackFunc func(msg m.Message, ack func(), nack func())) error {
	if e.channel.IsClosed() || e.connection.IsClosed() {
		return m.ErrMessageMiddlewareDisconnected
	}

	messages, err := e.channel.Consume(
		e.queue.Name,
		e.consumerTag,
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
	if err := e.channel.Cancel(e.consumerTag, false); err != nil {
		return m.ErrMessageMiddlewareDisconnected
	}
	return nil
}

func (e *ExchangeMiddleware) Send(msg m.Message) error {
	if e.channel.IsClosed() || e.connection.IsClosed() {
		return m.ErrMessageMiddlewareDisconnected
	}

	for _, key := range e.keys {
		err := e.channel.Publish(
			e.exchange,
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
	channelErr := e.channel.Close()
	connectionErr := e.connection.Close()

	if channelErr != nil || connectionErr != nil {
		return m.ErrMessageMiddlewareClose
	}

	return nil
}
