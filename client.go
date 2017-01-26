package amqprpc

// This source file is part of the AMQP-RPC open source project
// Licensed under Apache License v2.0
// See LICENSE file for license information

// Parts of the code are adopted from Go net/rpc/client.go file to
// provide API compatibility with builtin RPC over TCP functionality

// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

import (
	"bytes"
	"crypto/tls"
	"errors"
	"fmt"
	"github.com/go-amqpirq/amqpirq"
	"github.com/streadway/amqp"
	"log"
	"net/rpc"
	"os"
	"runtime"
	"strconv"
	"sync"
	"time"
)

var (
	defaultPoolSize = uint(runtime.NumCPU())

	defaultClientConfig = &ClientConfig{
		PoolSize: defaultPoolSize,
		Codec:    new(GobClientCodec),
	}
)

// Client is RPC Client
type Client struct {
	seq     uint64
	mutex   sync.Mutex
	conn    *amqpirq.Connection
	closing bool
	pending map[uint64]*Call
	jobs    chan *Call
	codec   ClientCodec

	poolSize int
}

type ClientConfig struct {
	// PoolSize is number of threads sending requests to the server
	// and processing responses
	PoolSize uint
	// TLS is SSL connection configuration
	TLS *tls.Config
	// Codec is an encoder/decoder used for serialization of requests
	Codec ClientCodec
}

// Close sends a close message to underlying connection and empties
// internal pending message pool
func (client *Client) Close() {
	if client.closing {
		return
	}
	client.mutex.Lock()
	defer client.mutex.Unlock()

	close(client.jobs)
	for id, call := range client.pending {
		client.deregisterCall(id)
		call.Error = errors.New("amqprpc.Client: shut down")
		call.done()
	}
	if client.conn != nil {
		client.conn.Close()
	}
	client.closing = true
}

func (client *Client) registerCall(call *Call) uint64 {
	client.mutex.Lock()
	client.seq++
	s := client.seq
	client.pending[s] = call
	client.mutex.Unlock()
	return s
}

func (client *Client) deregisterCall(s uint64) *Call {
	client.mutex.Lock()
	call := client.pending[s]
	delete(client.pending, s)
	client.mutex.Unlock()
	return call
}

func (client *Client) restart() {
	client.requeue()
	worker := amqpirq.NewConnectionWorker(inoutWorker{client})
	go client.conn.Listen(worker)
}

func (client *Client) requeue() {
	var calls []*Call
	client.mutex.Lock()
	for _, call := range client.pending {
		calls = append(calls, call)
	}
	client.pending = make(map[uint64]*Call)
	client.mutex.Unlock()

	for _, call := range calls {
		client.jobs <- call
	}
}

// Call represents an active RPC.
type Call struct {
	ServiceMethod string      // The name of the service and method to call.
	Args          interface{} // The argument to the function (*struct).
	Reply         interface{} // The reply from the function (*struct).
	Error         error       // After completion, the error status.
	Done          chan *Call  // Strobes when call is complete
	key           string      // destination queue name
}

func (call *Call) done() {
	select {
	case call.Done <- call:
	}
}

// NewClient returns a new Client to handle requests to the
// set of services at broker connected using AMQP URI url
func NewClient(url string) (*Client, error) {
	return NewClientConfig(url, nil)
}

// NewClientConfig returns a new Client to handle requests to the
// set of services at broker connected using AMQP URI url customised
// with config parameters
func NewClientConfig(url string, config *ClientConfig) (*Client, error) {
	return newClientDial(url, config)
}

func newClientDial(url string, config *ClientConfig) (*Client, error) {
	if config == nil {
		config = defaultClientConfig
	}
	if config.PoolSize < 1 {
		return nil, errors.New("amqprpc.Client: pool size is required")
	}
	if config.Codec == nil {
		config.Codec = new(GobClientCodec)
	}
	conn, err := amqpirqConnection(url, config.TLS)
	if err != nil {
		return nil, fmt.Errorf("amqprpc.Client: %v", err)
	}

	client := &Client{
		codec:    config.Codec,
		conn:     conn,
		jobs:     make(chan *Call, int(config.PoolSize)*runtime.NumCPU()*10),
		pending:  make(map[uint64]*Call),
		poolSize: int(config.PoolSize),
	}

	go client.restart()

	return client, nil
}

type inConsumer struct {
	client *Client
}

func (consumer *inConsumer) Consume(ch *amqp.Channel, d *amqp.Delivery) {
	defer d.Ack(false)

	seq, err := strconv.ParseUint(d.CorrelationId, 10, 64)
	if err != nil {
		return
	}
	call := consumer.client.deregisterCall(seq)
	if call == nil {
		return
	}

	buf := bytes.NewBuffer(d.Body)
	ccodec := consumer.client.codec.ClientCodec(buf)
	defer ccodec.Close()
	r := new(rpc.Response)
	err = ccodec.ReadResponseHeader(r)
	switch {
	case err != nil:
		call.Error = fmt.Errorf("reading header %s", err.Error())
	case r.Error != "":
		call.Error = errors.New(r.Error)
	default:
		err = ccodec.ReadResponseBody(call.Reply)
		if err != nil {
			call.Error = fmt.Errorf("reading body %s", err.Error())
		}
	}
	call.done()
}

type outWorker struct {
	client  *Client
	appId   string
	replyTo string
}

func (outworker *outWorker) Do(conn *amqp.Connection, done <-chan struct{}) {
	ch, err := conn.Channel()
	if err != nil {
		panic(err)
	}
	if err != nil {
		panic(err)
	}

	for i := 0; i < outworker.client.poolSize; i++ {
		go func() {
			for {
				select {
				case job := <-outworker.client.jobs:
					if job == nil {
						// jobs channel closed
						log.Println("amqprpc.Client: channel closed")
						return
					}
					seq := outworker.client.registerCall(job)
					request := new(rpc.Request)
					request.Seq = seq
					request.ServiceMethod = job.ServiceMethod

					buf := new(bytes.Buffer)
					ccodec := outworker.client.codec.ClientCodec(buf)
					err = ccodec.WriteRequest(request, job.Args)

					if err == nil {
						correlationID := strconv.FormatUint(seq, 10)
						hdrs := make(map[string]interface{}, 2)
						hdrs["ServiceMethod"] = job.ServiceMethod
						hdrs["Seq"] = correlationID

						err = ch.Publish(
							"",
							job.key,
							false,
							false,
							amqp.Publishing{
								ContentType:   outworker.client.codec.ContentType(),
								AppId:         outworker.appId,
								CorrelationId: correlationID,
								ReplyTo:       outworker.replyTo,
								DeliveryMode:  amqp.Transient,
								Body:          buf.Bytes(),
								Timestamp:     time.Now(),
								Headers:       amqp.Table(hdrs),
							})
					}
					if err != nil {
						outworker.client.deregisterCall(seq)
						job.Error = err
						job.done()
					}
				case <-done:
					// avoid leaking goroutines
					return
				}
			}
		}()
	}
}

type inoutWorker struct {
	client *Client
}

func (worker inoutWorker) Do(ch *amqp.Channel, done <-chan struct{}) {
	q, err := ch.QueueDeclare(
		"",    // name
		false, // durable
		false, // delete when unused
		true,  // exclusive
		false, // noWait
		nil,   // arguments
	)
	if err != nil {
		panic(err)
	}

	go func() {
		consumer := new(inConsumer)
		consumer.client = worker.client
		wrk := amqpirq.NewFixedChannelWorker(func(ch *amqp.Channel) (amqp.Queue, error) { return q, nil }, worker.client.poolSize, consumer)
		wrk.Do(ch, done)
	}()

	go func() {
		hostname, _ := os.Hostname()
		wrk := &outWorker{
			client:  worker.client,
			appId:   hostname,
			replyTo: q.Name,
		}
		worker.client.conn.Listen(wrk)
	}()

	<-done
}

// Go invokes the function asynchronously. It returns the Call structure representing
// the invocation. The done channel will signal when the call is complete by returning
// the same Call object. If done is nil, Go will allocate a new channel.
// If non-nil, done must be buffered or Go will deliberately crash.
func (client *Client) Go(key, serviceMethod string, args interface{}, reply interface{}, done chan *Call) *Call {
	call := new(Call)
	call.ServiceMethod = serviceMethod
	call.Args = args
	call.Reply = reply
	call.key = key
	if done == nil {
		done = make(chan *Call, 10*client.poolSize) // buffered.
	} else {
		// If caller passes done != nil, it must arrange that
		// done has enough buffer for the number of simultaneous
		// RPCs that will be using that channel. If the channel
		// is totally unbuffered, it's best not to run at all.
		if cap(done) == 0 {
			panic("rpc: done channel is unbuffered")
		}
	}
	call.Done = done
	client.jobs <- call
	return call
}

// Call invokes the named function, waits for it to complete, and returns its error status.
func (client *Client) Call(key, serviceMethod string, args interface{}, reply interface{}) error {
	call := <-client.Go(key, serviceMethod, args, reply, make(chan *Call, 1)).Done
	return call.Error
}
