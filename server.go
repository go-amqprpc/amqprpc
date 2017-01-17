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
	"github.com/streadway/amqp"
	"gopkg.in/amqpirq.v0"
	"log"
	"math"
	"net/rpc"
	"runtime"
	"strconv"
	"sync"
	"time"
)

const (
	defaultMaxAttempts = -1
	defaultConnDelay   = uint(5)
)

var (
	defaultServiceConfig = &ServiceConfig{
		Codec:    new(GobServerCodec),
		PoolSize: uint(runtime.NumCPU()),
	}

	defaultServerConfig = &ServerConfig{
		MaxAttempts: defaultMaxAttempts,
		ConnDelay:   defaultConnDelay,
	}
)

// Server is a AMQP-RPC server wrapper
type Server struct {
	m       map[string]*service
	running bool
	closing bool
	conn    *amqpirq.Connection
	mutex   sync.Mutex
}

type ServerConfig struct {
	TLS         *tls.Config
	MaxAttempts int
	ConnDelay   uint
}

// NewServer returns a new Server.
func NewServer(url string) (*Server, error) {
	return NewServerConfig(url, nil)
}

// NewServerConfig returns a new Server with customisable max number of
// attempts to re-connect to the broker after failure MaxAttempts,
// delay between connections ConnDelay and TLS connection configuration
// using ServerConfig config.
func NewServerConfig(url string, config *ServerConfig) (*Server, error) {
	return newServerDial(url, config)
}

func newServerDial(url string, config *ServerConfig) (*Server, error) {
	if config == nil {
		config = defaultServerConfig
	}
	conn, err := amqpirqConnection(url, config.TLS)
	if err != nil {
		return nil, fmt.Errorf("amqprpc.Server: %v", err)
	}
	conn.MaxAttempts = config.MaxAttempts
	conn.Delay = config.ConnDelay

	return &Server{
		m:    make(map[string]*service),
		conn: conn,
	}, nil
}

type ServiceConfig struct {
	Queue    func(*amqp.Channel) (amqp.Queue, error)
	Codec    ServerCodec
	PoolSize uint
}

type service struct {
	queue    func(*amqp.Channel) (amqp.Queue, error)
	codec    ServerCodec
	rs       *rpc.Server
	poolSize int
}

// Register is a queue key oriented wrapper for net/rpc.Server.Register that
// configures the set of methods of the receiver value that satisfy the
// following conditions:
//	- exported method of exported type
//	- two arguments, both of exported type
//	- the second argument is a pointer
//	- one return value, of type error
// It returns an error if the receiver is not an exported type or has
// no suitable methods. It also logs the error using package log.
// The client accesses each method using a string of the form "Type.Method",
// where Type is the receiver's concrete type.
func (s *Server) Register(key string, rcvr interface{}) error {
	return s.RegisterName(key, "", rcvr)
}

// RegisterName is like Register but uses the provided name for the type
// instead of the receiver's concrete type.
func (s *Server) RegisterName(key, name string, rcvr interface{}) error {
	return s.RegisterConfig(key, name, rcvr, nil)
}

func (s *Server) RegisterConfig(qn, name string, rcvr interface{}, conf *ServiceConfig) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.m == nil {
		s.m = make(map[string]*service)
	}

	if qn == "" {
		return errors.New("amqprpc.Register: queue name required to register a service")
	}
	if _, present := s.m[qn]; present {
		return fmt.Errorf("amqprpc.Register: service already defined: %s", qn)
	}

	if rcvr == nil {
		return errors.New("amqprpc.Register: receiver required to register a service")
	}

	rs := rpc.NewServer()

	var err error
	if name == "" {
		err = rs.Register(rcvr)
	} else {
		err = rs.RegisterName(name, rcvr)
	}

	if err != nil {
		return err
	}

	if conf == nil {
		conf = defaultServiceConfig
	}

	queue := conf.Queue
	if queue == nil {
		queue = func(ch *amqp.Channel) (amqp.Queue, error) { return amqpirq.NamedReplyQueue(ch, qn) }
	}
	srvCodec := conf.Codec
	if srvCodec == nil {
		srvCodec = new(GobServerCodec)
	}

	s.m[qn] = &service{
		queue:    queue,
		codec:    srvCodec,
		poolSize: int(conf.PoolSize),
		rs:       rs,
	}

	return nil
}

// Close sends request to interrupt underlying connection
func (server *Server) Close() {
	server.mutex.Lock()
	defer server.mutex.Unlock()
	server.closing = true
	server.conn.Close()
	server.conn = nil
}

// ConnError returns last connection reported from underlying
// connection attempt
func (server *Server) ConnError() error {
	return server.conn.LastError()
}

// Listen asynchronously starts all configured services
func (server *Server) Listen() error {
	server.mutex.Lock()
	defer server.mutex.Unlock()

	if server.running {
		return errors.New("amqprpc.Listen: server is already running")
	}
	if server.closing {
		return errors.New("amqprpc.Listen: server has been closed")
	}

	if len(server.m) == 0 {
		return errors.New("amqprpc.Listen: no services have been configured")
	}

	for qn, svc := range server.m {
		go func(key string, serv *service) {
			poolSize := int(math.Max(float64(serv.poolSize), 1.0))
			worker, err := amqpirq.NewParallelConnectionWorker(serv.queue, poolSize, rpcServerConsumer{serv})
			if err != nil {
				panic(err)
			}
			log.Printf("amqprpc.Listen: starting service on queue '%s'", key)
			if err = server.conn.Listen(worker); err != nil {
				fmt.Printf("amqprpc.Listen: service on queue '%s' interrupted: %s\n", key, err.Error())
			}
		}(qn, svc)
	}

	return nil
}

type rpcServerConsumer struct {
	svc *service
}

func (consumer rpcServerConsumer) Consume(ch *amqp.Channel, d *amqp.Delivery) {
	buf := new(bytes.Buffer)
	scodec := consumer.svc.codec.ServerCodec(bytes.NewBuffer(d.Body), buf)
	err := consumer.svc.rs.ServeRequest(scodec)
	body := buf.Bytes()
	if err != nil || len(body) == 0 {
		resp := new(rpc.Response)
		if d.Headers != nil && len(d.Headers) > 0 {
			if v, ok := d.Headers["ServiceMethod"]; ok {
				if v, ok := d.Headers["Seq"]; ok {
					seq, err := strconv.ParseUint(v.(string), 10, 64)
					if err != nil {
						resp.Seq = seq
					}
				}
				resp.ServiceMethod = v.(string)
			}
		}
		resp.Error = err.Error()
		buf = new(bytes.Buffer)
		scodec = consumer.svc.codec.ServerCodec(new(bytes.Buffer), buf)
		scodec.WriteResponse(resp, struct{}{})
		body = buf.Bytes()
	}

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
			ContentType:   consumer.svc.codec.ContentType(),
		})

	if err != nil {
		d.Reject(false)
	} else {
		d.Ack(false)
	}
}

func amqpirqConnection(url string, config *tls.Config) (conn *amqpirq.Connection, err error) {
	if config != nil {
		conn, err = amqpirq.DialTLS(url, config)
	} else {
		conn, err = amqpirq.Dial(url)
	}
	return
}
