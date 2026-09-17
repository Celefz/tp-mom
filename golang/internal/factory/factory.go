package factory

import (
	"fmt"

	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)

func CreateQueueMiddleware(queueName string, connectionSettings m.ConnSettings) (m.Middleware, error) {
	url := fmt.Sprintf("amqp://guest:guest@%s:%d/", connectionSettings.Hostname, connectionSettings.Port)

	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, err
	}

	channel, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, err
	}

	queue, err := channel.QueueDeclare(
		queueName,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		conn.Close()
		channel.Close()
		return nil, err
	}

	return &QueueMiddleware{
		Connection: conn,
		Channel:    channel,
		Queue:      queue,
	}, nil
}

func CreateExchangeMiddleware(exchange string, keys []string, connectionSettings m.ConnSettings) (m.Middleware, error) {
	url := fmt.Sprintf("amqp://guest:guest@%s:%d/", connectionSettings.Hostname, connectionSettings.Port)

	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, err
	}

	channel, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, err
	}

	err = channel.ExchangeDeclare(
		exchange,
		amqp.ExchangeDirect,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		conn.Close()
		channel.Close()
		return nil, err
	}

	queue, err := channel.QueueDeclare(
		"",
		false,
		true,
		true,
		false,
		nil,
	)
	if err != nil {
		conn.Close()
		channel.Close()
		return nil, err
	}

	for _, key := range keys {
		err = channel.QueueBind(
			queue.Name,
			key,
			exchange,
			false,
			nil,
		)
		if err != nil {
			conn.Close()
			channel.Close()
			return nil, err
		}
	}

	return &ExchangeMiddleware{
		Connection: conn,
		Channel:    channel,
		Exchange:   exchange,
		Queue:      queue,
		Keys:       keys,
	}, nil
}
