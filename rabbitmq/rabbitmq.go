package rabbitmq

import (
	"encoding/json"

	"github.com/streadway/amqp"
)

type RabbitMQ struct {
	// 信道
	channel *amqp.Channel

	Name     string
	exchange string
}

// 创建一个新的消息信道
func New(s string) *RabbitMQ {
	// TCP连接
	conn, e := amqp.Dial(s)
	if e != nil {
		panic(e)
	}

	ch, e := conn.Channel()
	if e != nil {
		panic(e)
	}

	q, e := ch.QueueDeclare(
		"",
		false,
		true,
		false,
		false,
		nil,
	)
	if e != nil {
		panic(e)
	}
	mq := new(RabbitMQ)
	mq.channel = ch
	mq.Name = q.Name
	return mq
}

// 将自己的消息队列和一个exchange绑定
func (q *RabbitMQ) Bind(exchange string) {
	e := q.channel.QueueBind(
		q.Name,
		"",
		exchange,
		false,
		nil,
	)
	if e != nil {
		panic(e)
	}
	q.exchange = exchange
}

// 往指定消息队列发送消息
func (q *RabbitMQ) Send(queue string, body interface{}) {
	str, err := json.Marshal(body)
	if err != nil {
		panic(err)
	}
	err = q.channel.Publish("",
		queue,
		false,
		false,
		amqp.Publishing{
			ReplyTo: q.Name,
			Body:    []byte(str),
		})
	if err != nil {
		panic(err)
	}
}

// 往指定交换机发送消息
func (mq *RabbitMQ) Publish(exchange string, body interface{}) {
	str, err := json.Marshal(body)
	if err != nil {
		panic(err)
	}
	err = mq.channel.Publish(exchange,
		"",
		false,
		false,
		amqp.Publishing{
			ReplyTo: mq.Name,
			Body:    []byte(str),
		})
	if err != nil {
		panic(err)
	}
}

// 生成一个接受消息的go channel
func (mq *RabbitMQ) Consume() <-chan amqp.Delivery {
	c, err := mq.channel.Consume(
		mq.Name,
		"",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		panic(err)
	}
	return c
}

// 关闭该消息信道
func (mq *RabbitMQ) Close() {
	mq.channel.Close()
}
