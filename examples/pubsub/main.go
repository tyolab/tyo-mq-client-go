// A minimal tyo-mq round trip: one connection produces, another subscribes
// with ACK-enabled durable delivery.
//
// Start a server first (npx tyo-mq, or node server.js in the tyo-mq repo),
// then:
//
//	go run ./examples/pubsub -server http://localhost:17352
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"time"

	tyomq "github.com/tyolab/tyo-mq-client-go"
)

func main() {
	server := flag.String("server", "http://localhost:17352", "tyo-mq server URL")
	token := flag.String("token", "", "auth token (when the server has auth enabled)")
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	connect := func(name string) *tyomq.Client {
		c := tyomq.NewClient(*server, log)
		ready := make(chan struct{})
		go c.Connect(ctx, ready)
		select {
		case <-ready:
		case <-ctx.Done():
			fmt.Fprintln(os.Stderr, "timed out connecting to", *server)
			os.Exit(1)
		}
		if *token != "" {
			if err := c.Authenticate(ctx, *token); err != nil {
				fmt.Fprintln(os.Stderr, name, "auth:", err)
				os.Exit(1)
			}
		}
		return c
	}

	producer := connect("producer")
	defer producer.Close()
	consumer := connect("consumer")
	defer consumer.Close()

	if err := producer.RegisterProducer("go-example"); err != nil {
		panic(err)
	}
	if err := consumer.RegisterConsumer("go-listener"); err != nil {
		panic(err)
	}

	received := make(chan tyomq.ConsumedMessage, 1)
	err := consumer.Subscribe(tyomq.SubscribeReq{
		Producer: "go-example",
		Event:    "greeting",
		Consumer: "go-listener",
		Durable:  true,
		Ack:      true, // auto-ACKed after the handler returns
	}, func(msg tyomq.ConsumedMessage) {
		received <- msg
	})
	if err != nil {
		panic(err)
	}
	time.Sleep(300 * time.Millisecond) // let the subscription register

	if err := producer.Produce("go-example", "greeting", map[string]any{"text": "hello from Go"}); err != nil {
		panic(err)
	}

	select {
	case msg := <-received:
		fmt.Printf("received %s from %s: %s\n", msg.Event, msg.From, msg.Message)
	case <-ctx.Done():
		fmt.Fprintln(os.Stderr, "no message received before timeout")
		os.Exit(1)
	}
}
