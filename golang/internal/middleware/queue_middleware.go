package middleware

import (
	amqp "github.com/rabbitmq/amqp091-go"
)

const CONSUMER_TAG = "queue-consumer"

type QueueMiddleware struct {
	Connection *amqp.Connection
	Channel    *amqp.Channel
	Queue      amqp.Queue
}

func (q *QueueMiddleware) StartConsuming(callbackFunc func(msg Message, ack func(), nack func())) error {
	messages, err := q.Channel.Consume(
		q.Queue.Name,
		CONSUMER_TAG,
		false,
		false,
		false,
		false,
		nil,
	)

	if err != nil {
		return err
	}

	for message := range messages {
		callbackFunc(
			Message{string(message.Body)},
			func() { message.Ack(false) },
			func() { message.Nack(false, true) },
		)
	}
	return nil
}

func (q *QueueMiddleware) StopConsuming() error {
	if err := q.Channel.Cancel(CONSUMER_TAG, false); err != nil {
		return err
	}
	return nil
}

func (q *QueueMiddleware) Send(msg Message) error {
	err := q.Channel.Publish(
		"",
		q.Queue.Name,
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
	return nil
}

func (q *QueueMiddleware) Close() error {
	var finalErr error

	if err := q.Channel.Close(); err != nil && err != amqp.ErrClosed {
		finalErr = err
	}

	if err := q.Connection.Close(); err != nil && err != amqp.ErrClosed {
		finalErr = err
	}

	return finalErr
}
