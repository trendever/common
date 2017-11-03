package rabbit

import (
	"common/log"
	"common/stopper"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/streadway/amqp"
	"reflect"
	"strconv"
	"sync"
	"time"
)

// Per topic limit for unconfirmed publishes.
// Extra queries will not be sent to broker until acks for older messages will have been received.
const (
	UnconfirmedPublishLimit    = 10
	EncodedTransferContentType = "application/json"
	ErrorStringContentType     = "text/error"
	RPCNamesPrefix             = "__rpc__"
	DefaultRPCQueueLength      = 50
	DefaultRPCTimeout          = 2 * time.Second
)

type Config struct {
	URL string
}

type Bindable interface {
	GetName() string
	Declare(ch *amqp.Channel) (name string, err error)
	Bind(ch *amqp.Channel, name, exchange, key string) error
	Validate() error
}

type Queue struct {
	Name string
	// Durable queues will survive between server restarts.
	Durable bool
	// Delete queue shortly after exit of last consumer.
	AutoDelete bool
	// Exclusive queues are only accessible by the connection that declares them and will be deleted when the connection closes.
	Exclusive bool
	Args      amqp.Table
}

func (q Queue) GetName() string {
	return q.Name
}

func (q Queue) Declare(ch *amqp.Channel) (name string, err error) {
	// If queue have empty name, broker will grant unique name to it.
	ret, err := ch.QueueDeclare(
		q.Name,
		q.Durable,
		q.AutoDelete,
		q.Exclusive,
		false,
		q.Args,
	)
	return ret.Name, err
}

func (q Queue) Bind(ch *amqp.Channel, name, exchange, key string) error {
	return ch.QueueBind(
		name,
		key,
		exchange,
		false,
		nil, // @TODO Args?
	)
}

func (q Queue) Validate() error {
	return q.Args.Validate()
}

type Exchange struct {
	Name string
	// Each exchange belongs to one of a set of exchange kinds/types implemented by the server.
	Kind string
	// Durable exchanges will survive between server restarts.
	Durable bool
	// Delete exchange when there are no remaining bindings
	AutoDelete bool
	//  Exchanges declared as `internal` do not accept accept publishings. Internal exchanges are useful when you wish to implement inter-exchange topologies that should not be exposed to users of the broker.
	Internal bool
	Args     amqp.Table
}

func (e Exchange) GetName() string {
	return e.Name
}

func (e Exchange) Declare(ch *amqp.Channel) (name string, err error) {
	err = ch.ExchangeDeclare(
		e.Name,
		e.Kind,
		e.Durable,
		e.AutoDelete,
		e.Internal,
		false,
		e.Args,
	)
	return e.Name, err
}

func (e Exchange) Bind(ch *amqp.Channel, name, exchange, key string) error {
	return ch.ExchangeBind(
		name,
		key,
		exchange,
		false,
		nil, // @TODO Args?
	)
}

func (e Exchange) Validate() error {
	if e.Name == "" {
		return errors.New("empty exchange name")
	}
	switch e.Kind {
	case amqp.ExchangeDirect, amqp.ExchangeFanout, amqp.ExchangeTopic, amqp.ExchangeHeaders:
	default:
		return errors.New("unknown kind of exchange")
	}
	return e.Args.Validate()
}

type Binding struct {
	Keys []string
	Node Bindable
}

type Route []Binding

func (r Route) Declare(ch *amqp.Channel, declareAnonymousQueues bool) (lastName string, err error) {
	err = r.Validate()
	if err != nil {
		return "", err
	}
	for i, bind := range r {
		// Do not declare anonymous queue unless argument allows that.
		if !declareAnonymousQueues && i+1 == len(r) {
			if q, ok := bind.Node.(Queue); ok {
				if q.Name == "" {
					break
				}
			}
		}

		curName, err := bind.Node.Declare(ch)
		if err != nil {
			return "", fmt.Errorf("failed to declare node '%v': %v", bind.Node.GetName(), err)
		}
		// There is nothing to to bind to yet.
		if i != 0 {
			for _, key := range bind.Keys {
				err := bind.Node.Bind(ch, curName, lastName, key)
				if err != nil {
					return "", fmt.Errorf("failed to bind '%v' to '%v': %v", bind.Node.GetName(), lastName, err)
				}
			}
		}
		lastName = curName
	}
	return lastName, nil
}

func (r Route) Validate() error {
	if len(r) == 0 {
		return errors.New("zero-lengh route")
	}
	for i, bind := range r {
		if bind.Node == nil {
			return errors.New("nil node")
		}
		if i != 0 {
			if len(bind.Keys) == 0 {
				return errors.New("empty binding keyset")
			}
		}
		if _, ok := bind.Node.(Queue); ok {
			if i+1 != len(r) {
				return errors.New("queue can be only last part of route")
			}
		}
		err := bind.Node.Validate()
		if err != nil {
			return fmt.Errorf("invalid bindable '%v': %v", bind.Node.GetName(), err)
		}
	}
	return nil
}

type pubQuery struct {
	key           string
	data          []byte
	replyTo       string
	correlationId string
	confirmed     bool
	replyChan     chan error
}

type pubWaiter struct {
	query     pubQuery
	confirmed bool
}

type Publisher struct {
	// Local name for use in Publish() func.
	Name            string
	DefaultExchange bool
	// Unless DefaultExchange is true, messages will be published to first exchange of first route.
	// Other router could be useful to declare complicated delivery schema.
	Routes []Route
	// Persistent publishings will be restored in this queue on server restart.
	Persistent bool
	// When true Publish() call will wait to confirmation from broker.
	// For unroutable messages, the broker will issue a confirm once the exchange verifies
	// a message won't route to any queue. See mandatory flag to deal with it.
	Confirm bool
	// Publishes can be undeliverable when the mandatory flag is true
	// and no queue is bound that matches the routing key.
	// Broker will return message to sender in this case.
	// @TODO Add callback for returns.
	Mandatory bool

	deliveryMode uint8
	queryChan    chan pubQuery
	lock         sync.RWMutex
}

func (p *Publisher) connLoop(c conn) {
	defer c.wait.Done()
	for {
		ch, err := c.amqp.Channel()
		// Most likely something is wrong with connection.
		if err != nil {
			c.amqp.Close()
			return
		} else if p.prepareChanel(ch) {
			p.chanLoop(ch)
		}
		ch.Close()
		select {
		case <-c.stopper.Chan():
			return
		case <-time.After(time.Second * 10):
		}
	}
}

func (p *Publisher) prepareChanel(ch *amqp.Channel) bool {
	for i, route := range p.Routes {
		_, err := route.Declare(ch, false)
		if err != nil {
			log.Errorf("rabbit: failed to declare rounte #%v of publisher '%v': %v", i, p.Name, err)
			return false
		}
	}
	return !p.Confirm || ch.Confirm(false) == nil
}

func (p *Publisher) chanLoop(ch *amqp.Channel) {
	// @NOTICE I'm not sure about the way to handle troubles with chanel.
	// We could save all unconfirmed queries and try to send them again later.
	var (
		confirms chan amqp.Confirmation
		waiters  *TagRing
	)
	if p.Confirm {
		confirms = ch.NotifyPublish(make(chan amqp.Confirmation, UnconfirmedPublishLimit))
		waiters = NewTagRing(UnconfirmedPublishLimit)
	}
	returns := ch.NotifyReturn(make(chan amqp.Return))
	closes := ch.NotifyClose(make(chan *amqp.Error))
	haveTroubles := false
	exchange := ""
	if !p.DefaultExchange {
		exchange = p.Routes[0][0].Node.GetName()
	}
	for {
		var queries chan (pubQuery)
		// publish extra messages only if we are not full of waiters
		if !p.Confirm || (!waiters.IsFull() && !haveTroubles) {
			queries = p.queryChan
		}
		select {
		case query := <-queries:
			err := ch.Publish(
				exchange,
				query.key,
				p.Mandatory,
				false, // rabbit do not support immediate exchanges anyway
				amqp.Publishing{
					ContentType:   EncodedTransferContentType,
					Body:          query.data,
					ReplyTo:       query.replyTo,
					CorrelationId: query.correlationId,
					DeliveryMode:  p.deliveryMode,
				},
			)
			// We have troubles with chanel or connection.
			if err != nil {
				haveTroubles = true
				// see notice above
				query.replyChan <- err
				continue
			}
			if p.Confirm {
				waiters.Enqueue(pubWaiter{
					query:     query,
					confirmed: false,
				})
			} else {
				query.replyChan <- nil
			}
		case confirm := <-confirms:
			// Confirms chan will be closed before notify via closes chan.
			// We could get here before case below...
			if confirm.DeliveryTag == 0 {
				confirms = nil
				continue
			}
			p.handleConfirm(waiters, confirm)
		// Message is returned if no route(connected queues) found for mandatory exchanges.
		case ret := <-returns:
			// @TODO add callback? idk whether we need it anyway.
			if ret.ReplyCode == 0 {
				returns = nil
				continue
			}
			log.Warn("rabbit: got return %+v", ret)
		case err := <-closes:
			if confirms != nil {
				// Be sure to read all conforms.
				for confirm := range confirms {
					if confirm.DeliveryTag == 0 {
						break
					}
					p.handleConfirm(waiters, confirm)
				}
			}
			if returns != nil {
				for ret := range returns {
					if ret.ReplyCode == 0 {
						break
					}
					log.Warn("rabbit: got return %+v", ret)
				}
			}
			if p.Confirm {
				// see notice above
				for iface := waiters.Dequeue(); iface != nil; iface = waiters.Dequeue() {
					waiter := iface.(pubWaiter)
					if !waiter.confirmed {
						waiter.query.replyChan <- err
					}
				}
			}
			return
		}
	}
}

func (p *Publisher) handleConfirm(waiters *TagRing, confirm amqp.Confirmation) {
	if !waiters.ContainsTag(confirm.DeliveryTag) {
		// wtf? Something went really wrong.
		log.Errorf("rabbit: got ack with unexpected tag %v", confirm.DeliveryTag)
		return
	}

	var result error
	if !confirm.Ack { // i'm not even sure when it could happen
		result = errors.New("broker rejected message")
	}

	if waiters.FirstTag() == confirm.DeliveryTag {
		waiter := waiters.Dequeue().(pubWaiter)
		waiter.query.replyChan <- result
		// Dequeue waiters which got acks earlier from tail of ring.
		for {
			iface := waiters.Pick()
			if iface == nil {
				break
			}
			waiter = iface.(pubWaiter)
			if !waiter.confirmed {
				break
			}
			waiters.Dequeue()
		}
	} else { // Out of order ack. Well, whatever.
		waiter := waiters.Get(confirm.DeliveryTag).(pubWaiter)
		waiter.query.replyChan <- result
		// Update waiter in order to prevent extra replies.
		waiters.Update(confirm.DeliveryTag, pubWaiter{
			confirmed: true,
		})
	}
}

func Publish(topic, routeKey string, data interface{}) error {
	pub, ok := global.publishers[topic]
	if !ok {
		return errors.New("unknown publish topic")
	}

	bytes, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to marshal data: %v", err)
	}

	pub.lock.RLock()

	select {
	case <-global.conn.stopper.Chan():
		pub.lock.RUnlock()
		return errors.New("connection stopped")
	default:
	}
	replyChan := make(chan error)
	pub.queryChan <- pubQuery{
		key:           routeKey,
		data:          bytes,
		replyTo:       "",
		correlationId: "",
		replyChan:     replyChan,
	}
	pub.lock.RUnlock()
	return <-replyChan
}

// Should be called before Start().
func AddPublishers(pubs ...Publisher) {
	for _, pub := range pubs {
		if pub.Name == "" {
			log.Fatalf("rabbit: empty publisher name")
		}
		for i, route := range pub.Routes {
			err := route.Validate()
			if err != nil {
				log.Fatalf("rabbit: invalid route #%v in publisher '%v': %v", i, pub.Name, err)
			}
		}
		if !pub.DefaultExchange {
			if len(pub.Routes) == 0 {
				log.Fatalf("rabbit: zero routers and no DefaultExchange flag in publisher '%v'", pub.Name)
			}
			if _, ok := pub.Routes[0][0].Node.(Queue); ok {
				log.Fatalf("raabit: publisher '%v': attempt to publish to queue", pub.Name)
			}
		}
		pub.queryChan = make(chan pubQuery)
		if pub.Persistent {
			pub.deliveryMode = amqp.Persistent
		} else {
			pub.deliveryMode = amqp.Transient
		}
		global.publishers[pub.Name] = &pub
	}
}

type Subscription struct {
	// Local name for identify subscription
	Name string
	// Subscription will consume from queue in first route.
	// Multiple routes to same anonymous queue are not supported yet. @TODO Do we need it?
	Routes  []Route
	AutoAck bool
	// When exclusive is true, the server will ensure that this is the sole consumer from this queue.
	Exclusive bool
	Args      amqp.Table
	// Run handlers in gorutines. @TODO It does not work yet, do not use...
	Concurrent bool
	// With a prefetch count greater than zero, the server will deliver that many messages to consumers before acknowledgments are received.
	// The server ignores this option when consumers are started with noAck because no acknowledgments are expected or sent.
	Prefetch int

	// Basic handler. If AutoAck is false, function must call Ack() or Reject() method of delivery before exit.
	Handler func(delivery amqp.Delivery, ch *amqp.Channel)
	// Easier way: data will be decoded with json.Unmarshal, ack or requeue will be performed based on return value.
	// Should be func(decodedArg something) bool.
	// @TODO Do we need a way to reject delivery without requeue? It migh be useful to send it to dead letter exchange.
	DecodedHandler interface{}

	// If set, this will be called after declaration routes.
	// Second argument — name of queue from with subscription will consume.
	// Useful for obtaining names of "anonymous" queues.
	ReconnectCallback func(ch *amqp.Channel, endpoint string) error
}

func (s *Subscription) prepareHandler() {
	if s.DecodedHandler == nil {
		return
	}
	hType := reflect.TypeOf(s.DecodedHandler)
	if hType.Kind() != reflect.Func || hType.NumIn() != 1 ||
		hType.NumOut() != 1 || hType.Out(0).Kind() != reflect.Bool {
		log.Fatalf("rabbit: DecodedHandler for subscription %v has unexpected type", s.Name)
	}
	argType := hType.In(0)
	hValue := reflect.ValueOf(s.DecodedHandler)

	s.Handler = func(m amqp.Delivery, _ *amqp.Channel) {
		if m.ContentType != EncodedTransferContentType {
			log.Errorf("rabbit: got delivery with unexpected mime type %+v", m.ContentType)
			if !s.AutoAck {
				// Remove it from queue any way
				m.Ack(false)
			}
			return
		}
		arg, err := unmarshalToValue(m.Body, argType)
		if err != nil {
			log.Errorf("rabbit: failed to unmarshal argument of subscription '%v': %v\nData: %v", s.Name, err, string(m.Body))
			if !s.AutoAck {
				m.Ack(false)
			}
			return
		}
		args := []reflect.Value{arg}
		success := hValue.Call(args)[0].Bool()
		if !s.AutoAck {
			if success {
				err := m.Ack(false)
				if err != nil {
					log.Errorf("rabbit: failed to acknowledge rabbit about successefuly handled msg: %v", err)
				}
			} else {
				err := m.Reject(true)
				if err != nil {
					// It is not a big problem any way, without ack it will be requeued anyway.
					log.Errorf("rabbit: failed to reject unsuccessefully handled message: %v", err)
				}
			}
		}
	}
}

func (s *Subscription) connLoop(c conn) {
	defer c.wait.Done()
	for {
		ch, err := c.amqp.Channel()
		// Most likely something is wrong with connection.
		if err != nil {
			c.amqp.Close()
			return
		} else if name, ok := s.prepareChanel(ch); !ok {
			// We are trying to redeclare queue with incompatible settings probably, that should be logged.
			log.Errorf("rabbit: failed to declare queue for '%v': %v", s.Name, err)
		} else if deliveries, err := ch.Consume(
			name,
			"",
			s.AutoAck,
			s.Exclusive,
			false, // rabbit do not support noLocal flag
			false,
			s.Args,
		); err != nil {
			log.Errorf("rabbit: failed to start consume on '%v': %v", s.Name, err)
		} else {
			for delivery := range deliveries {
				if s.Concurrent {
					c.wait.Add(1)
					go func() {
						s.Handler(delivery, ch)
						c.wait.Done()
					}()
				} else {
					s.Handler(delivery, ch)
				}
			}
		}
		ch.Close()
		select {
		case <-c.stopper.Chan():
			return
		case <-time.After(time.Second * 10):
		}
	}
}

func (s *Subscription) prepareChanel(ch *amqp.Channel) (name string, ok bool) {
	name, err := s.Routes[0].Declare(ch, true)
	if err != nil {
		log.Errorf("rabbit: failed to declare rounte #%v of subscription '%v': %v", 0, s.Name, err)
		return "", false
	}
	for i, route := range s.Routes[1:] {
		_, err := route.Declare(ch, false)
		if err != nil {
			log.Errorf("rabbit: failed to declare rounte #%v of subscription '%v': %v", i+1, s.Name, err)
			return "", false
		}
	}

	if s.Prefetch > 0 {
		err := ch.Qos(s.Prefetch, 0, false)
		if err != nil {
			log.Errorf("rabbit: failed to set prefetch count on subscription '%v': %v", s.Name, err)
			return "", false
		}
	}

	if s.ReconnectCallback != nil {
		err := s.ReconnectCallback(ch, name)
		if err != nil {
			log.Errorf("rabbit: ReconnectCallback of subscription '%v' returned error: %v", s.Name, err)
			return "", false
		}
	}

	closes := ch.NotifyClose(make(chan *amqp.Error))
	go func() {
		for err := range closes {
			log.Errorf("%+v", err)
		}
	}()

	return name, true
}

func Subscribe(subscriptions ...Subscription) {
	for _, sub := range subscriptions {
		if sub.Name == "" {
			log.Fatalf("rabbit: empty subscription name")
		}
		if len(sub.Routes) == 0 {
			log.Fatalf("rabbit: no routes in subscription '%v'", sub.Name)
		}
		for i, route := range sub.Routes {
			err := route.Validate()
			if err != nil {
				log.Fatalf("rabbit: invalid route #%v in subscription '%v': %v", i, sub.Name, err)
			}
		}
		if _, ok := sub.Routes[0][len(sub.Routes[0])-1].Node.(Queue); !ok {
			log.Fatalf("rabbit: first route in subscription '%v' do not end with queue", sub.Name)
		}
		if err := sub.Args.Validate(); err != nil {
			log.Fatalf("rabbit: invalid arguments in subscripiton '%v': %v", sub.Name, err)
		}
		if sub.DecodedHandler != nil {
			sub.prepareHandler()
		}
		if sub.Handler == nil {
			log.Fatalf("rabbit: subscripiton '%v' do not have any handlers", sub.Name)
		}
		global.subscriptions[sub.Name] = &sub
	}
}

type RPC struct {
	Name        string
	Timeout     time.Duration
	QueueLength int
	// @TODO It does not work yet, do not use...
	Concurrent  bool
	HandlerType interface{}
}

func (rpc RPC) Route() Route {
	return Route{
		{
			Node: Exchange{
				Name: RPCNamesPrefix + rpc.Name,
				Kind: amqp.ExchangeFanout,
				// There is totally no need to keep route when nobody uses it.
				// Anything that was not delivered should disappear.
				AutoDelete: true,
				Durable:    false,
			},
		},
		{
			Keys: []string{""},
			Node: Queue{
				Name:       RPCNamesPrefix + rpc.Name,
				AutoDelete: true,
				Durable:    false,
				Exclusive:  false,
			},
		},
	}
}

func (rpc RPC) Validate() error {
	if rpc.Name == "" {
		return errors.New("empty name")
	}
	if rpc.HandlerType == nil {
		return errors.New("nil HandlerType")
	}
	hType := reflect.TypeOf(rpc.HandlerType)
	if hType.Kind() != reflect.Func {
		return errors.New("HandlerType is not a function")
	}
	if hType.NumIn() != 1 || hType.NumOut() != 2 ||
		!hType.Out(1).Implements(reflect.TypeOf((*error)(nil)).Elem()) {
		return errors.New("HandlerType' has unexpected signatire")
	}
	return nil
}

func unmarshalToValue(data []byte, typ reflect.Type) (reflect.Value, error) {
	var val reflect.Value
	if typ.Kind() != reflect.Ptr {
		val = reflect.New(typ)
	} else {
		val = reflect.New(typ.Elem())
	}
	if err := json.Unmarshal(data, val.Interface()); err != nil {
		return reflect.Zero(typ), err
	}
	if typ.Kind() != reflect.Ptr {
		val = reflect.Indirect(val)
	}
	return val, nil
}

func ServeRPC(desc RPC, handler interface{}) {
	err := desc.Validate()
	if err != nil {
		log.Fatalf("rabbit: invalid description of RPC '%v': %v", desc.Name, err)
	}
	hType := reflect.TypeOf(desc.HandlerType)

	if handler == nil {
		log.Fatalf("rabbit: handler of RPC '%v' is nil")
	}
	if reflect.TypeOf(handler) != hType {
		log.Fatalf("rabbit: handler of RPC '%v' have incompatible type")
	}

	argType := hType.In(0)
	hValue := reflect.ValueOf(handler)

	// Dat naming %)
	realHandler := func(m amqp.Delivery, ch *amqp.Channel) {
		if m.ContentType != EncodedTransferContentType {
			log.Errorf("rabbit: rpc '%v' call argiment unexpected mime type %+v", desc.Name, m.ContentType)
			return
		}

		arg, err := unmarshalToValue(m.Body, argType)
		if err != nil {
			log.Errorf("rabbit: failed to unmarshal call argument of rpc '%v': %v", desc.Name, err)
		}
		ret := hValue.Call([]reflect.Value{arg})
		iface := ret[1].Interface()
		if iface != nil {
			err := iface.(error)
			ch.Publish("", m.ReplyTo, false, false, amqp.Publishing{
				ContentType:   ErrorStringContentType,
				DeliveryMode:  amqp.Transient,
				CorrelationId: m.CorrelationId,
				Body:          []byte(err.Error()),
			})
			return
		}
		data, err := json.Marshal(ret[0].Interface())
		if err != nil {
			log.Errorf("rabbit: failed to marshal reply for rpc '%v': %v", desc.Name, err)
			return
		}
		// Publish reply directly to caller queue.
		ch.Publish("", m.ReplyTo, false, false, amqp.Publishing{
			ContentType:   EncodedTransferContentType,
			DeliveryMode:  amqp.Transient,
			CorrelationId: m.CorrelationId,
			Body:          data,
		})
	}

	Subscribe(Subscription{
		Name:       RPCNamesPrefix + desc.Name,
		Routes:     []Route{desc.Route()},
		AutoAck:    true,
		Exclusive:  false,
		Handler:    realHandler,
		Concurrent: desc.Concurrent,
	})
}

func DeclareRPC(desc RPC, funcPtr interface{}) {
	err := desc.Validate()
	if err != nil {
		log.Fatalf("rabbit: invalid description of RPC '%v': %v", desc.Name, err)
	}

	if desc.QueueLength == 0 {
		desc.QueueLength = DefaultRPCQueueLength
	}
	if desc.Timeout == 0 {
		desc.Timeout = DefaultRPCTimeout
	}

	hType := reflect.TypeOf(desc.HandlerType)

	if funcPtr == nil {
		log.Fatalf("rabbit: funcPtr of RPC '%v' is nil")
	}

	funcPtrType := reflect.TypeOf(funcPtr)
	funcType := funcPtrType.Elem()
	if funcPtrType.Kind() != reflect.Ptr || funcType.Kind() != reflect.Func {
		// x_x
		log.Fatalf("rabbit: funcPtr of RPC '%v' is not pointer to function")
	}

	if funcType != hType {
		log.Fatalf("rabbit: funcPrt of RPC '%v' have incompatible type", desc.Name)
	}

	AddPublishers(Publisher{
		Name:   RPCNamesPrefix + desc.Name,
		Routes: []Route{desc.Route()},
	})

	var (
		lock      sync.Mutex
		queueName string
		waiters   *TagRing = NewTagRing(desc.QueueLength)
		pub                = global.publishers[RPCNamesPrefix+desc.Name]
	)

	type rpcReply struct {
		data []byte
		err  error
	}
	type rpcWaiter struct {
		replyChan chan rpcReply
		done      bool
	}

	Subscribe(Subscription{
		Name:       RPCNamesPrefix + desc.Name + "_reply",
		AutoAck:    true,
		Concurrent: false, // Most of time will be spent behind waiters lock anyway.
		Routes: []Route{
			{{
				Node: Queue{
					Name:       "",
					AutoDelete: true,
					Durable:    false,
					Exclusive:  true,
				},
			}},
		},

		ReconnectCallback: func(ch *amqp.Channel, endpoint string) error {
			lock.Lock()
			queueName = endpoint
			base := waiters.FirstTag()
			for i := uint64(0); i < waiters.Len(); i++ {
				waiter := waiters.Get(base + i).(rpcWaiter)
				if !waiter.done {
					select {
					case waiter.replyChan <- rpcReply{
						err: errors.New("connection aborted"),
					}:
					default:
					}
				}
			}
			lock.Unlock()
			return nil
		},

		Handler: func(delivery amqp.Delivery, ch *amqp.Channel) {
			tag, err := strconv.ParseUint(delivery.CorrelationId, 10, 64)
			if err != nil {
				log.Errorf("rabbit: invalid correlation id '%v' in reply for RPC '%v'", delivery.CorrelationId, desc.Name)
				return
			}

			var reply rpcReply
			if delivery.ContentType == ErrorStringContentType {
				reply.err = errors.New(string(delivery.Body))
			} else {
				reply.data = delivery.Body
			}

			lock.Lock()
			iface := waiters.Get(tag)
			if iface != nil {
				select {
				case iface.(rpcWaiter).replyChan <- reply:
				default:
				}
			}
			lock.Unlock()
		},
	})

	retType := hType.In(0)
	nilErr := reflect.Zero(reflect.TypeOf((*error)(nil)).Elem())

	// Dark reflect magic all round.
	// https://pbs.twimg.com/media/DEvvvUoUIAEpAbU.jpg
	call := func(args []reflect.Value) (results []reflect.Value) {
		// Marshal argument first, no need to resume when it's invalid.
		data, err := json.Marshal(args[0].Interface())
		if err != nil {
			return []reflect.Value{
				reflect.Zero(retType),
				errValue(fmt.Errorf("fialed to marshal argument: %v", err)),
			}
		}

		timer := time.NewTimer(desc.Timeout)
		replyChan := make(chan rpcReply, 1)

		// Add waiter.
		lock.Lock()
		if waiters.IsFull() {
			lock.Unlock()
			return []reflect.Value{reflect.Zero(retType), errValue(errors.New("waiters queue is full"))}
		}
		tag := waiters.Enqueue(rpcWaiter{
			replyChan: replyChan,
		})
		qName := queueName
		lock.Unlock()

		// Remove waiter on exit.
		defer func() {
			lock.Lock()
			if waiters.FirstTag() == tag {
				waiters.Dequeue()
				for {
					iface := waiters.Pick()
					if iface == nil {
						break
					}
					waiter := iface.(rpcWaiter)
					if !waiter.done {
						break
					}
					waiters.Dequeue()
				}
			} else {
				waiters.Update(tag, rpcWaiter{
					done: true,
				})
			}
			lock.Unlock()
		}()

		pubChan := make(chan error)
		// Send publish query.
		select {
		case pub.queryChan <- pubQuery{
			key:           "",
			data:          data,
			replyTo:       qName,
			correlationId: strconv.FormatUint(tag, 10),
			replyChan:     pubChan,
		}:

		case <-timer.C:
			return []reflect.Value{reflect.Zero(retType), errValue(errors.New("timeout"))}
		case <-global.conn.stopper.Chan():
			return []reflect.Value{reflect.Zero(retType), errValue(errors.New("connection stopped"))}
		}

		// Wait for publish.
		select {
		case err := <-pubChan:
			if err != nil {
				return []reflect.Value{
					reflect.Zero(retType),
					errValue(fmt.Errorf("fialed to publish argument: %v", err)),
				}
			}

		case <-timer.C:
			return []reflect.Value{reflect.Zero(retType), errValue(errors.New("timeout"))}
		case <-global.conn.stopper.Chan():
			return []reflect.Value{reflect.Zero(retType), errValue(errors.New("connection stopped"))}
		}

		// Wait for reply.
		select {
		case reply := <-replyChan:
			if reply.err != nil {
				return []reflect.Value{
					reflect.Zero(retType),
					errValue(reply.err),
				}
			}
			val, err := unmarshalToValue(reply.data, retType)
			if err != nil {
				return []reflect.Value{
					reflect.Zero(retType),
					errValue(fmt.Errorf("fialed to unmarshal reply: %v", err)),
				}
			}
			return []reflect.Value{val, nilErr}

		case <-timer.C:
			return []reflect.Value{reflect.Zero(retType), errValue(errors.New("timeout"))}
		case <-global.conn.stopper.Chan():
			return []reflect.Value{reflect.Zero(retType), errValue(errors.New("connection stopped"))}
		}
	}

	reflect.ValueOf(funcPtr).Elem().Set(reflect.MakeFunc(funcType, call))
}

func errValue(err error) reflect.Value {
	return reflect.ValueOf(&err).Elem()
}

type conn struct {
	amqp    *amqp.Connection
	wait    *sync.WaitGroup
	stopper *stopper.Stopper
}

var global = struct {
	conn          *conn
	publishers    map[string]*Publisher
	subscriptions map[string]*Subscription
}{
	publishers:    map[string]*Publisher{},
	subscriptions: map[string]*Subscription{},
}

func Start(config *Config) {
	global.conn = &conn{
		stopper: stopper.NewStopper(),
		wait:    new(sync.WaitGroup),
	}
	global.conn.wait.Add(1)
	go global.conn.reconnectLoop(config)
}

func Stop() {
	log.Info("rabbit: stopping")
	global.conn.stop()
	log.Info("rabbit: stopped")
}

func (c *conn) stop() {
	c.stopper.Stop()
	c.wait.Wait()
	for _, pub := range global.publishers {
		pub.lock.Lock()
	QUERIES:
		for {
			select {
			case query := <-pub.queryChan:
				query.replyChan <- errors.New("connection stopped")
			default:
				break QUERIES
			}
		}
		pub.lock.Unlock()
	}
}

func (c *conn) reconnectLoop(config *Config) {
	defer c.wait.Done()
	for {
		conn, err := amqp.Dial(config.URL)
		if err != nil {
			log.Errorf("rabbit: failed to dial: %v", err)
		} else {
			c.amqp = conn
			errChan := conn.NotifyClose(make(chan *amqp.Error))
			log.Info("rabbit: connetion is ready")
			for _, pub := range global.publishers {
				c.wait.Add(1)
				go pub.connLoop(*c)
			}
			for _, sub := range global.subscriptions {
				c.wait.Add(1)
				go sub.connLoop(*c)
			}
			select {
			case err := <-errChan:
				if err != nil {
					log.Errorf("rabbit: disconnected: %v", err)
				}
			case <-c.stopper.Chan():
			}
		}
		select {
		case <-c.stopper.Chan():
			log.Error(c.amqp.Close())
			return
		case <-time.After(time.Second * 10):
		}
	}
}
