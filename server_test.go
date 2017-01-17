package amqprpc

// This source file is part of the AMQP-RPC open source project
// Licensed under Apache License v2.0
// See LICENSE file for license information

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"github.com/satori/go.uuid"
	"github.com/streadway/amqp"
	"math/rand"
	"net/rpc"
	"os"
	"runtime"
	"strconv"
	"testing"
	"time"
)

type Args struct {
	A, B int
}

type Quotient struct {
	Quo, Rem int
}

type Arith int

func (t *Arith) Multiply(args *Args, reply *int) error {
	*reply = args.A * args.B
	return nil
}

func (t *Arith) Divide(args *Args, quo *Quotient) error {
	if args.B == 0 {
		return errors.New("divide by zero")
	}
	quo.Quo = args.A / args.B
	quo.Rem = args.A % args.B
	return nil
}

func failOnError(t *testing.T, err error, msg string) {
	if err != nil {
		t.Fatalf("%s: %s", msg, err)
	}
}

func TestListen_Gob(t *testing.T) {

	if amqpURI() == "" {
		t.Skip("Environment variable AMQP_URI not set")
	}

	// server
	arith := new(Arith)
	s, err := NewServer(amqpURI())
	if err != nil {
		t.Fatal(err)
	}
	q0Name := uuid.NewV4().String()
	q1Name := uuid.NewV4().String()
	s.Register(q0Name, arith)
	s.RegisterName(q1Name, "CustomName", arith)

	go func() {
		err := s.Listen()
		if err != nil {
			fmt.Println(err)
		}
	}()

	// boilerplate AMQP RPC client code with Gob codec
	conn, err := amqp.Dial(amqpURI())
	failOnError(t, err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(t, err, "Failed to open a channel")
	defer ch.Close()

	// message on Q0
	q0, err := ch.QueueDeclare(
		"",    // name
		false, // durable
		false, // delete when unused
		true,  // exclusive
		false, // noWait
		nil,   // arguments
	)
	failOnError(t, err, "Failed to declare a temp queue 0")

	msgs, err := ch.Consume(
		q0.Name, // queue
		"",      // consumer
		false,   // auto-ack
		false,   // exclusive
		false,   // no-local
		false,   // no-wait
		nil,     // args
	)
	failOnError(t, err, "Failed to register a consumer")

	rand.Seed(time.Now().UnixNano())
	seq := uint64(rand.Uint32())
	req := &rpc.Request{
		ServiceMethod: "Arith.Multiply",
		Seq:           seq,
	}
	buf := new(bytes.Buffer)
	enc := gob.NewEncoder(buf)
	enc.Encode(req)
	args := &Args{7, 8}
	enc.Encode(args)

	err = ch.Publish(
		"",     // exchange
		q0Name, // routing key
		false,  // mandatory
		false,  // immediate
		amqp.Publishing{
			ContentType:   binaryContentType,
			CorrelationId: strconv.FormatUint(seq, 10),
			ReplyTo:       q0.Name,
			Body:          buf.Bytes(),
			DeliveryMode:  amqp.Transient,
		})
	failOnError(t, err, "Failed to publish a message")

	for d := range msgs {
		if strconv.FormatUint(seq, 10) == d.CorrelationId {

			respBuf := bytes.NewBuffer(d.Body)
			dec := gob.NewDecoder(respBuf)
			resp := new(rpc.Response)
			err := dec.Decode(resp)
			failOnError(t, err, "Failed to convert body header")
			var val int
			err = dec.Decode(&val)
			failOnError(t, err, "Failed to convert body value")

			if got, want := val, (7 * 8); got != want {
				t.Errorf("Expected '%d', got '%d'", want, got)
			}

			err = ch.Publish(
				d.Exchange, // exchange
				d.ReplyTo,  // routing key
				false,      // mandatory
				false,      // immediate
				amqp.Publishing{
					ContentType:   d.ContentType,
					CorrelationId: d.CorrelationId,
					Body:          respBuf.Bytes(),
					DeliveryMode:  d.DeliveryMode,
				})
			if err != nil {
				t.Error(err)
			}
			d.Ack(false)
			break
		} else {
			t.Errorf("Message with unexpected CorrelationId '%s'", d.CorrelationId)
			d.Reject(false)
		}
	}

	// Message on Q1
	q1, err := ch.QueueDeclare(
		"",    // name
		false, // durable
		false, // delete when unused
		true,  // exclusive
		false, // noWait
		nil,   // arguments
	)
	failOnError(t, err, "Failed to declare a temp queue 1")

	msgs, err = ch.Consume(
		q1.Name, // queue
		"",      // consumer
		false,   // auto-ack
		false,   // exclusive
		false,   // no-local
		false,   // no-wait
		nil,     // args
	)
	failOnError(t, err, "Failed to register a consumer")

	rand.Seed(time.Now().UnixNano())
	seq = uint64(rand.Uint32())
	req = &rpc.Request{
		ServiceMethod: "CustomName.Divide",
		Seq:           seq,
	}
	buf = new(bytes.Buffer)
	enc = gob.NewEncoder(buf)
	enc.Encode(req)
	args = &Args{8, 4}
	enc.Encode(args)

	err = ch.Publish(
		"",     // exchange
		q1Name, // routing key
		false,  // mandatory
		false,  // immediate
		amqp.Publishing{
			ContentType:   binaryContentType,
			CorrelationId: strconv.FormatUint(seq, 10),
			ReplyTo:       q1.Name,
			Body:          buf.Bytes(),
			DeliveryMode:  amqp.Transient,
		})
	failOnError(t, err, "Failed to publish a message")

	for d := range msgs {
		if strconv.FormatUint(seq, 10) == d.CorrelationId {

			respBuf := bytes.NewBuffer(d.Body)
			dec := gob.NewDecoder(respBuf)
			resp := new(rpc.Response)
			err := dec.Decode(resp)
			failOnError(t, err, "Failed to convert body header")
			var val Quotient
			err = dec.Decode(&val)
			failOnError(t, err, "Failed to convert body value")

			if got, want := val.Quo, 2; got != want {
				t.Errorf("Expected Quo='%d', got='%d'", want, got)
			}
			if got, want := val.Rem, 0; got != want {
				t.Errorf("Expected Rem='%d', got='%d'", want, got)
			}

			err = ch.Publish(
				d.Exchange, // exchange
				d.ReplyTo,  // routing key
				false,      // mandatory
				false,      // immediate
				amqp.Publishing{
					ContentType:   d.ContentType,
					CorrelationId: d.CorrelationId,
					Body:          respBuf.Bytes(),
					DeliveryMode:  d.DeliveryMode,
				})
			if err != nil {
				t.Error(err)
			}
			d.Ack(false)
			break
		} else {
			t.Errorf("Message with unexpected CorrelationId '%s'", d.CorrelationId)
			d.Reject(false)
		}
	}

	s.Close()

	msg, err := ch.QueueDelete(q0Name, false, false, false)
	if got, want := msg, 0; got != want {
		t.Errorf("Expected removed messages count=%d, got=%d", want, got)
	}
	if err != nil {
		t.Error(err)
	}

	msg, err = ch.QueueDelete(q1Name, false, false, false)
	if got, want := msg, 0; got != want {
		t.Errorf("Expected removed messages count=%d, got=%d", want, got)
	}
	if err != nil {
		t.Error(err)
	}
}

func TestListen_InvalidCodec(t *testing.T) {

	if amqpURI() == "" {
		t.Skip("Environment variable AMQP_URI not set")
	}

	// server
	arith := new(Arith)
	s, err := NewServer(amqpURI())
	if err != nil {
		t.Fatal(err)
	}
	qName := uuid.NewV4().String()
	s.Register(qName, arith)

	go func() {
		err := s.Listen()
		if err != nil {
			fmt.Println(err)
		}
	}()

	// boilerplate AMQP RPC client code with Gob codec
	conn, err := amqp.Dial(amqpURI())
	failOnError(t, err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(t, err, "Failed to open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"",    // name
		false, // durable
		false, // delete when usused
		true,  // exclusive
		false, // noWait
		nil,   // arguments
	)
	failOnError(t, err, "Failed to declare a temp queue")

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		false,  // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(t, err, "Failed to register a consumer")

	err = ch.Publish(
		"",    // exchange
		qName, // routing key
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			ContentType:   binaryContentType,
			CorrelationId: "X",
			ReplyTo:       q.Name,
			Body:          []byte("invalid RPC message"),
			DeliveryMode:  amqp.Transient,
		})
	failOnError(t, err, "Failed to publish a message")

	for d := range msgs {
		if "X" == d.CorrelationId {
			if got, want := d.ContentType, binaryContentType; got != want {
				t.Errorf("Expected ContentType='%s', got '%s'", want, got)
			}
			respBuf := bytes.NewBuffer(d.Body)
			dec := gob.NewDecoder(respBuf)
			resp := new(rpc.Response)
			err := dec.Decode(resp)
			if err != nil {
				t.Fatal(err)
			}
			if got, want := resp.Error, "unexpected EOF"; got != want {
				t.Errorf("Expected Body='%s', got '%s'", want, got)
			}
			d.Ack(false)
			break
		} else {
			t.Errorf("Message with unexpected CorrelationId '%s'", d.CorrelationId)
			d.Reject(false)
		}
	}

	s.Close()
	msg, err := ch.QueueDelete(qName, false, false, false)
	if got, want := msg, 0; got != want {
		t.Errorf("Expected removed messages count=%d, got=%d", want, got)
	}
	if err != nil {
		t.Fatal(err)
	}
}

func TestListen_Bulk(t *testing.T) {

	if amqpURI() == "" {
		t.Skip("Environment variable AMQP_URI not set")
	}

	// server
	arith := new(Arith)
	s, err := NewServer(amqpURI())
	if err != nil {
		t.Fatal(err)
	}
	qName := uuid.NewV4().String()
	s.RegisterConfig(qName, "", arith, &ServiceConfig{
		PoolSize: uint(2 * runtime.NumCPU()),
	})

	err = s.Listen()
	if err != nil {
		t.Fatal(err)
	}

	// boilerplate AMQP RPC client code with Gob codec
	conn, err := amqp.Dial(amqpURI())
	failOnError(t, err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(t, err, "Failed to open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"",    // name
		false, // durable
		false, // delete when usused
		false, // exclusive
		false, // noWait
		nil,   // arguments
	)
	failOnError(t, err, "Failed to declare a queue")

	failOnError(t, ch.Qos(1, 0, false), "QoS")

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(t, err, "Failed to register a consumer")

	count := 5000

	for i := 0; i < count; i++ {
		seq := uint64(i)
		req := &rpc.Request{
			ServiceMethod: "Arith.Multiply",
			Seq:           seq,
		}
		buf := new(bytes.Buffer)
		enc := gob.NewEncoder(buf)
		enc.Encode(req)
		args := &Args{i, i + 1}
		enc.Encode(args)

		err = ch.Publish(
			"",    // exchange
			qName, // routing key
			false, // mandatory
			false, // immediate
			amqp.Publishing{
				ContentType:   binaryContentType,
				CorrelationId: strconv.FormatInt(int64(i), 10),
				ReplyTo:       q.Name,
				Body:          buf.Bytes(),
				DeliveryMode:  amqp.Transient,
			})
		failOnError(t, err, "Failed to publish a message")
	}

	retCount := 0

	for d := range msgs {
		i, err := strconv.ParseInt(d.CorrelationId, 10, 32)
		failOnError(t, err, "Message with unexpected CorrelationId")
		retCount++

		respBuf := bytes.NewBuffer(d.Body)
		dec := gob.NewDecoder(respBuf)
		resp := new(rpc.Response)
		err = dec.Decode(resp)
		failOnError(t, err, "Failed to convert body header")
		var val int
		err = dec.Decode(&val)
		failOnError(t, err, "Failed to convert body value")

		if got, want := val, int(i*(i+1)); got != want {
			t.Errorf("Expected '%d', got '%d'", want, got)
		}

		if retCount == count {
			break
		}
	}

	s.Close()
	msg, err := ch.QueueDelete(qName, false, false, false)
	if got, want := msg, 0; got != want {
		t.Errorf("Expected removed messages count=%d, got=%d", want, got)
	}
	if err != nil {
		t.Fatal(err)
	}

}

func TestListen_InvalidURI(t *testing.T) {
	s, err := NewServerConfig("amqp://non-existent-host//", &ServerConfig{MaxAttempts: 1, ConnDelay: 0})
	if err != nil {
		t.Fatal(err)
	}
	s.Register("non-existent-queue", new(Arith))
	err = s.Listen()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	for {
		time.Sleep(1 * time.Second)
		if err = s.conn.LastError(); err != nil {
			break
		}
	}
	if err == nil {
		t.Error("Expected error, got <nil>")
	}
}

func TestListen_IsRunning(t *testing.T) {
	s, err := NewServer(amqpURI())
	if err != nil {
		t.Fatal(err)
	}
	s.running = true
	err = s.Listen()
	defer s.Close()
	if err == nil {
		t.Error("Expected error, got <nil>")
	}
	if got, want := err.Error(), "amqprpc.Listen: server is already running"; got != want {
		t.Errorf("Expected error='%s', got='%s'", want, got)
	}
}

func TestListen_Closed(t *testing.T) {
	s, err := NewServer(amqpURI())
	if err != nil {
		t.Fatal(err)
	}
	s.Close()
	s.closing = true
	err = s.Listen()
	if err == nil {
		t.Error("Expected error, got <nil>")
	}
	if got, want := err.Error(), "amqprpc.Listen: server has been closed"; got != want {
		t.Errorf("Expected error='%s', got='%s'", want, got)
	}
	if s.m == nil {
		t.Error("Expected Server.m map, got <nil>")
	}
}

func TestRegisterConfig_NilConfig(t *testing.T) {
	s := new(Server)
	err := s.RegisterConfig("Q", "", new(Arith), new(ServiceConfig))
	if err != nil {
		t.Error(err)
	}
	if s.m == nil {
		t.Error("Expected Server.m map, got <nil>")
	}
}

func TestRegisterConfig_MissingArgs(t *testing.T) {
	s := new(Server)
	err := s.RegisterConfig("", "", new(Arith), new(ServiceConfig))
	if err == nil {
		t.Fatal("Expected error, got <nil>")
	}
	err = s.RegisterConfig("Q", "", nil, new(ServiceConfig))
	if err == nil {
		t.Fatal("Expected error, got <nil>")
	}
	err = s.RegisterConfig("Q", "", new(int), new(ServiceConfig))
	if err == nil {
		t.Fatal("Expected error, got <nil>")
	}
	err = s.RegisterConfig("Q", "", new(Arith), new(ServiceConfig))
	if err != nil {
		t.Fatalf("Expected <nil>, got error=%v", err)
	}
	err = s.RegisterConfig("Q", "", new(Arith), new(ServiceConfig))
	if err == nil {
		t.Fatal("Expected error, got <nil>")
	}
}

func amqpURI() string { return os.Getenv("AMQP_URI") }
