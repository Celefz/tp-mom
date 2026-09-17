package factory

import (
	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)

type QueueMiddleware struct {
	connection  *amqp.Connection
	channel     *amqp.Channel
	queue       amqp.Queue
	consumerTag string
}

func NewQueueMiddleware(conn *amqp.Connection, channel *amqp.Channel, queue amqp.Queue) *QueueMiddleware {
	consumerTag := queue.Name + CONSUMER_TAG_SUFFIX

	return &QueueMiddleware{
		conn,
		channel,
		queue,
		consumerTag,
	}
}

func (q *QueueMiddleware) StartConsuming(callbackFunc func(msg m.Message, ack func(), nack func())) error {
	if q.channel.IsClosed() || q.connection.IsClosed() {
		return m.ErrMessageMiddlewareDisconnected
	}

	messages, err := q.channel.Consume(
		q.queue.Name,
		q.consumerTag,
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

func (q *QueueMiddleware) StopConsuming() error {
	if err := q.channel.Cancel(q.consumerTag, false); err != nil {
		return m.ErrMessageMiddlewareDisconnected
	}
	return nil
}

func (q *QueueMiddleware) Send(msg m.Message) error {
	if q.channel.IsClosed() || q.connection.IsClosed() {
		return m.ErrMessageMiddlewareDisconnected
	}

	err := q.channel.Publish(
		"",
		q.queue.Name,
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
	return nil
}

func (q *QueueMiddleware) Close() error {
	channelErr := q.channel.Close()
	connectionErr := q.connection.Close()

	if channelErr != nil || connectionErr != nil {
		return m.ErrMessageMiddlewareClose
	}

	return nil
}
