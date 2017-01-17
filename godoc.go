package amqprpc

/*
	Project `amqprpc` provides access to the exported methods of an object across
	RabbitMQ connection. It is meant as a near drop-in implementation of TCP based
	built-in `net/rpc` package thus most of characteristics and limitations are
	kept intact. The major difference stems from the fact of using messaging
	queues for transport, one well defined queue for requests and a temporary
	queue established internally by a client for processing responses. The
	implementation is intended to follow to certain extent RabbitMQ tutorial
	http://www.rabbitmq.com/tutorials/tutorial-six-go.html


	A simple example of a use-case adopted from "net/rpc" would look as follows using
	a queue based RPC:

	A server wishes to export an object of type Arith:

		package X

		import "errors"

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

	The server calls for AMQP service:

		package X

		const (
		        RPCQueueName = "rpc_queue"
		        AMQPURI = "amqp://guest:guest@127.0.0.1:5672//"
		)

		arith := new(Arith)
		server, err := amqprpc.NewServer(AMQPURI)
		if err != nil {
		        panic(err)
		}
		defer server.Close()

		server.Register(RPCQueueName, arith)
		err = server.Listen()
		if err != nil {
		        panic(err)
		}

	At this point, clients can see a service "Arith" with methods "Arith.Multiply" and "Arith.Divide".
	To invoke one, a client first dials the server:

		package X

		client, err := amqprpc.NewClient(AMQPURI)
		if err != nil {
		        panic(err)
		}
		defer client.Close()

	Then it can make a remote call:

	// Synchronous call

		args := &X.Args{7,8}
		var reply int
		err = client.Call(RPCQueueName, "Arith.Multiply", args, &reply)
		if err != nil {
		        panic(err)
		}
		fmt.Printf("Arith: %d*%d=%d", args.A, args.B, reply)

	or

	// Asynchronous call

		quotient := new(X.Quotient)
		divCall := client.Go(RPCQueueName, "Arith.Divide", args, quotient, nil)
		replyCall := <-divCall.Done	// will be equal to divCall
		// check errors, print, etc.

*/
