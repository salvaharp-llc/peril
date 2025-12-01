package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/salvaharp/peril/internal/pubsub"
	"github.com/salvaharp/peril/internal/routing"
)

func main() {
	const connString = "amqp://guest:guest@localhost:5672/"

	fmt.Println("Starting Peril server...")

	conn, err := amqp.Dial(connString)
	if err != nil {
		log.Fatalf("Unable to connect with RabbitMQ server: %v", err)
	}
	defer conn.Close()
	fmt.Println("Successfully connected to RabbitMQ")

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("Unable to create RabbitMQ channel: %v", err)
	}
	defer ch.Close()

	err = pubsub.PublishJSON(ch, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{
		IsPaused: true,
	})
	if err != nil {
		log.Printf("Couldn't publish JSON: %v", err)
	}

	// wait for ctrl+c
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan
	fmt.Println("Shutting down RabbitMQ connection")
}
