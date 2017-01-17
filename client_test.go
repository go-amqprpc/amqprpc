package amqprpc

// This source file is part of the AMQP-RPC open source project
// Licensed under Apache License v2.0
// See LICENSE file for license information

import (
	"bytes"
	"github.com/satori/go.uuid"
	"github.com/streadway/amqp"
	"net/rpc"
	"testing"
	"time"
)

func TestClient_Call(t *testing.T) {
	// server boilerplate code

	conn, err := amqp.Dial(amqpURI())
	failOnError(t, err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(t, err, "Failed to open a channel")
	defer ch.Close()

	qName := uuid.NewV4().String()
	q, err := ch.QueueDeclare(
		qName, // name
		true,  // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	failOnError(t, err, "Failed to declare a queue")
	defer ch.QueueDelete(q.Name, false, false, false)

	err = ch.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global
	)
	failOnError(t, err, "Failed to set QoS")

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

	go func() {
		for d := range msgs {
			scodec := new(GobServerCodec)
			buf := new(bytes.Buffer)
			cdc := scodec.ServerCodec(bytes.NewBuffer(d.Body), buf)

			r := new(rpc.Request)
			err := cdc.ReadRequestHeader(r)
			if err != nil {
				t.Fatal(err)
			}

			if got, want := r.ServiceMethod, "Arith.Multiply"; got != want {
				t.Errorf("Expected ServiceMethod='%s', got='%s'", want, got)
			}
			if got, want := r.Seq, uint64(1); got != want {
				t.Errorf("Expected Seq='%d', got='%d'", want, got)
			}

			args := new(Args)
			err = cdc.ReadRequestBody(args)
			if err != nil {
				t.Fatal(err)
			}

			arith := new(Arith)
			var v int
			arith.Multiply(args, &v)

			rsp := new(rpc.Response)
			rsp.ServiceMethod = r.ServiceMethod
			rsp.Seq = r.Seq
			cdc.WriteResponse(rsp, v)

			err = ch.Publish(
				d.Exchange,
				d.ReplyTo,
				false,
				false,
				amqp.Publishing{
					CorrelationId: d.CorrelationId,
					Headers:       d.Headers,
					DeliveryMode:  d.DeliveryMode,
					Body:          buf.Bytes(),
					Timestamp:     time.Now(),
					ContentType:   scodec.ContentType(),
				})
			if err != nil {
				t.Error(err)
			}
			d.Ack(false)
			break
		}
	}()

	c, err := NewClientConfig(amqpURI(), &ClientConfig{PoolSize: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	var v int
	err = c.Call(qName, "Arith.Multiply", &Args{2, 3}, &v)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := v, 6; got != want {
		t.Errorf("Expected Reply='%d', got='%d'", want, got)
	}

	if got, want := len(c.pending), 0; got != want {
		t.Errorf("Expected Calls count='%d', got='%d'", want, got)
	}

}
