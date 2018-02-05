package broker

import (
	"github.com/streadway/amqp"
	"log"
	"time"
	"os"
	"strconv"
)

var (
	maxAttempts, _  = strconv.ParseInt(os.Getenv("BROKER_MAX_ATTEMPT"), 10, 64)
	attemptInterval = parseDuration(os.Getenv("BROKER_ATTEMPT_INTERVAL"), 0) * time.Second
)

type Broker struct {
	uri        string
	connection *amqp.Connection
	channel    *amqp.Channel
	queue      amqp.Queue
}

func NewBroker(uri string) (*Broker) {
	return &Broker{uri: uri}
}

func (broker *Broker) Connect() error {
	var err error
	broker.connection, err = amqp.Dial(broker.uri)
	if err != nil {
		return err
	}

	broker.channel, err = broker.connection.Channel()
	if err != nil {
		return err
	}
	return nil
}

func (broker *Broker) ConnectWithRetry() error {
	var err error = nil
	var attempt int64 = 0
	for ok := true; ok; ok = attempt < maxAttempts && err != nil {
		attempt++
		time.Sleep(attemptInterval)
		broker.connection, err = amqp.Dial(broker.uri)
		if err != nil {
			log.Printf("Attempt %d to connect failed with message : %s \n", attempt, err.Error())
			continue
		}
		broker.channel, err = broker.connection.Channel()
		if err != nil {
			log.Printf("Attempt %d to retrieve channel failed with message : %s \n", attempt, err.Error())
			continue
		}
	}

	return err
}

func (broker *Broker) Declare(queue string) (err error) {
	broker.queue, err = broker.channel.QueueDeclare(
		queue, // name
		true,  // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	return
}

func (broker *Broker) Receive(queue string, receiver AmqpMessageReceiver) (err error) {
	msgs, err := broker.channel.Consume(
		queue, // queue
		"",    // consumer
		true,  // auto-ack
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil,   // args
	)
	if err != nil {
		return
	}

	forever := make(chan bool)

	go func() {
		for message := range msgs {
			receiver.Handle(message)
		}
	}()

	log.Println("Waiting for incoming message")
	<-forever
	return
}

func (broker *Broker) Publish(queue string, message string) (err error) {
	log.Printf("Publishing message in queue : %s", queue)
	err = broker.channel.Publish(
		"",    // exchange
		queue, // routing key
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(message),
		})
	return
}

type AmqpMessageReceiver interface {
	Handle(delivery amqp.Delivery)
}

func parseDuration(durationRepresentation string, defaultDuration time.Duration) (time.Duration) {
	durationInt, err := strconv.ParseInt(durationRepresentation, 10, 64)
	if err != nil {
		return defaultDuration
	}
	return time.Duration(durationInt)
}
