package factory

import (
	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)

const CONSUMER_TAG_SUFFIX = "-consumer"

type CommonMiddlewareInfo struct {
	connection  *amqp.Connection
	channel     *amqp.Channel
	consumerTag string
}

func NewCommonMiddlewareInfo(conn *amqp.Connection, channel *amqp.Channel, consumerTagPreffix string) *CommonMiddlewareInfo {
	consumerTag := consumerTagPreffix + CONSUMER_TAG_SUFFIX

	return &CommonMiddlewareInfo{
		connection:  conn,
		channel:     channel,
		consumerTag: consumerTag,
	}
}

func (c *CommonMiddlewareInfo) ValidateConnection() error {
	if c.connection.IsClosed() || c.channel.IsClosed() {
		return m.ErrMessageMiddlewareDisconnected
	}
	return nil
}

func (c *CommonMiddlewareInfo) StartConsuming(queueName string, callbackFunc func(msg m.Message, ack func(), nack func())) error {
	if err := c.ValidateConnection(); err != nil {
		return err
	}

	messages, err := c.channel.Consume(
		queueName,
		c.consumerTag,
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

func (c *CommonMiddlewareInfo) StopConsuming() error {
	if err := c.ValidateConnection(); err != nil {
		return err
	}

	if err := c.channel.Cancel(c.consumerTag, false); err != nil {
		return m.ErrMessageMiddlewareDisconnected
	}
	return nil
}

func (c *CommonMiddlewareInfo) Close() error {
	channelErr := c.channel.Close()
	connectionErr := c.connection.Close()

	if channelErr != nil || connectionErr != nil {
		return m.ErrMessageMiddlewareClose
	}
	return nil
}
