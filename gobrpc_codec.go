package amqprpc

// This source file is part of the AMQP-RPC open source project
// Licensed under Apache License v2.0
// See LICENSE file for license information

import (
	"bytes"
	"encoding/gob"
	"net/rpc"
)

const (
	binaryContentType = "application/octet-stream"
)

// ServerCodec is a synchronous codec used to decode amqp.Delivery
// Body using rpc.ServerCodec
type ServerCodec interface {
	// ServerCodec returns a one-time codec used in rpc.Server.ServeRequest
	// call with input byte buffer (invoked with bytes.NewBuffer(d.Body))
	// and a transient output byte buffer for writing a response back to
	// the message broker
	ServerCodec(*bytes.Buffer, *bytes.Buffer) rpc.ServerCodec
	// ContentType is MIME content type for messages encoded with the codec
	ContentType() string
}

type ClientCodec interface {
	// ClientCodec returns a one-time codec for serializing requests
	ClientCodec(*bytes.Buffer) rpc.ClientCodec
	// ContentType is MIME content type for messages encoded with the codec
	ContentType() string
}

type GobServerCodec struct {
	codec *gobCodec
}

func (c GobServerCodec) ContentType() string {
	return binaryContentType
}

func (c GobServerCodec) ServerCodec(in *bytes.Buffer, out *bytes.Buffer) rpc.ServerCodec {
	return &gobCodec{
		dec: gob.NewDecoder(in),
		enc: gob.NewEncoder(out),
	}
}

type GobClientCodec struct {
	codec *gobCodec
}

func (c GobClientCodec) ClientCodec(buf *bytes.Buffer) rpc.ClientCodec {
	return &gobCodec{
		dec: gob.NewDecoder(buf),
		enc: gob.NewEncoder(buf),
	}
}

func (c GobClientCodec) ContentType() string {
	return binaryContentType
}

type gobCodec struct {
	dec *gob.Decoder
	enc *gob.Encoder
}

func (c gobCodec) Close() error {
	return nil
}

// net.ServerCodec

func (c gobCodec) ReadRequestHeader(r *rpc.Request) error {
	return c.dec.Decode(r)
}

func (c gobCodec) ReadRequestBody(body interface{}) error {
	return c.dec.Decode(body)
}

func (c gobCodec) WriteResponse(r *rpc.Response, body interface{}) (err error) {
	if err = c.enc.Encode(r); err != nil {
		return
	}
	if err = c.enc.Encode(body); err != nil {
		return
	}
	return
}

// net.ClientCodec

func (c gobCodec) WriteRequest(r *rpc.Request, body interface{}) (err error) {
	if err = c.enc.Encode(r); err != nil {
		return
	}
	if err = c.enc.Encode(body); err != nil {
		return
	}
	return
}

func (c gobCodec) ReadResponseHeader(r *rpc.Response) error {
	return c.dec.Decode(r)
}

func (c gobCodec) ReadResponseBody(body interface{}) error {
	return c.dec.Decode(body)
}
