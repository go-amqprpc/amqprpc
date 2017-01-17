package amqprpc

import (
	"github.com/satori/go.uuid"
	"github.com/streadway/amqp"
	"testing"
)

func TestClientServerComm(t *testing.T) {

	if amqpURI() == "" {
		t.Skip("Environment variable AMQP_URI not set")
	}

	arith := new(Arith)

	server, err := NewServer(amqpURI())
	if err != nil {
		t.Fatal(err)
	}

	queueName := uuid.NewV4().String()
	server.Register(queueName, arith)
	err = server.Listen()
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	defer deleteQueue(amqpURI(), queueName, t)

	client, err := NewClient(amqpURI())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	args := &Args{7, 8}
	var reply int
	err = client.Call(queueName, "Arith.Multiply", args, &reply)
	if err != nil {
		t.Fatal("arith error:", err)
	}

	if got, want := reply, args.A*args.B; got != want {
		t.Errorf("Expected reply=%d, got=%d", want, got)
	}
}

func deleteQueue(url, key string, t *testing.T) (int, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	defer ch.Close()
	return ch.QueueDelete(key, false, false, false)
}
