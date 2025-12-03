package main

import (
	"fmt"
	"log"
	"os"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/salvaharp/peril/internal/gamelogic"
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
	fmt.Println("Server successfully connected to RabbitMQ")

	ch, queue, err := pubsub.DeclareAndBind(
		conn,
		routing.ExchangePerilTopic,
		routing.GameLogSlug,
		routing.GameLogSlug+".*",
		pubsub.SimpleQueueDurable,
	)
	if err != nil {
		log.Fatalf("could not declare %s queue: %v", routing.GameLogSlug, err)
	}
	defer ch.Close()

	fmt.Printf("Queue %v declared and bound!\n", queue.Name)

	gamelogic.PrintServerHelp()
	for {
		words := gamelogic.GetInput()
		if len(words) == 0 {
			continue
		}

		switch words[0] {
		case "pause":
			log.Println("Pausing the game")
			err = pubsub.PublishJSON(ch, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{
				IsPaused: true,
			})
		case "resume":
			log.Println("Resuming the game")
			err = pubsub.PublishJSON(ch, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{
				IsPaused: false,
			})
		case "quit":
			log.Println("Exiting the game")
			os.Exit(0)
		default:
			fmt.Println("Unknown command")
		}
		if err != nil {
			log.Printf("Couldn't publish JSON: %v\n", err)
		}
	}
}
