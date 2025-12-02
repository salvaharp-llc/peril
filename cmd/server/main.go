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

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("Unable to create RabbitMQ channel: %v", err)
	}
	defer ch.Close()

	gamelogic.PrintServerHelp()
	for {
		words := gamelogic.GetInput()
		if len(words) == 0 {
			continue
		}

		switch words[0] {
		case "pause":
			log.Print("Pausing the game")
			err = pubsub.PublishJSON(ch, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{
				IsPaused: true,
			})
		case "resume":
			log.Print("Resuming the game")
			err = pubsub.PublishJSON(ch, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{
				IsPaused: false,
			})
		case "quit":
			log.Println("Exiting the game")
			os.Exit(0)
		default:
			log.Println("Unknown command")
		}
		if err != nil {
			log.Printf("Couldn't publish JSON: %v", err)
		}
	}
}
