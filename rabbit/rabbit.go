package rabbit

import (
	"common/log"
	"common/stopper"
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"github.com/streadway/amqp"
	"reflect"
	"sync"
	"time"
)

// Per topic limit for unconfirmed publishes.
// Extra queries will not be sent to broker until acks for older messages will have been received.
const UnconfirmedPublishLimit = 10
const encodedTrasferMimeType = "application/json"

type Config struct {
	URL string
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

type pubQuery struct {
	key       string
	data      []byte
	confirmed bool
	replyChan chan error
}

type pubWaiter struct {
	query     pubQuery
	confirmed bool
}

type Publisher struct {
	// Local name for use in Publish() func.
	Name string
	// Ignore exchange below and use default one.
	DefaultExchange bool
	Exchange        Exchange
	// Publishes can be undeliverable when the mandatory flag is true
	// and no queue is bound that matches the routing key.
	// Broker will return message to sender in this case.
	// @TODO Add callback for returns.
	Mandatory  bool
	Persistent bool
	// @TODO Make conformations optional.

	deliveryMode uint8
	queryChan    chan pubQuery
	// @TODO RWLock
	lock sync.Mutex
}

func declareExchange(ch *amqp.Channel, e *Exchange) error {
	return ch.ExchangeDeclare(
		e.Name,
		e.Kind,
		e.Durable,
		e.AutoDelete,
		e.Internal,
		false,
		e.Args,
	)
}

// If queue have empty name, broker will grant unique name to it. That is what returns.
func declareQueue(ch *amqp.Channel, q *Queue) (name string, err error) {
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
	if !p.DefaultExchange {
		err := declareExchange(ch, &p.Exchange)
		if err != nil {
			// Something is wrong with chanel or whole connection.
			// Or we are trying to redeclare exchange with incompatible settings, that should be logged.
			log.Errorf("rabbit: failed to declare exchange %v: %v", p.Exchange.Name, err)
			return false
		}
	}
	return ch.Confirm(false) == nil
}

func (p *Publisher) chanLoop(ch *amqp.Channel) {
	// @NOTICE I'm not sure about the way to handle troubles with chanel.
	// We could save all unconfirmed queries and try to send them again later.
	confirms := ch.NotifyPublish(make(chan amqp.Confirmation, UnconfirmedPublishLimit))
	returns := ch.NotifyReturn(make(chan amqp.Return))
	closes := ch.NotifyClose(make(chan *amqp.Error))
	// @TODO Specialize ring for pubWaiter type if it will not be used elsewhere
	// https://pbs.twimg.com/media/DEvvvUoUIAEpAbU.jpg
	waiters := NewTagRing(UnconfirmedPublishLimit)
	haveTroubles := false
	for {
		var queries chan (pubQuery)
		// publish extra messages only if we are not full of waiters
		if !waiters.IsFull() && !haveTroubles {
			queries = p.queryChan
		}
		select {
		case query := <-queries:
			err := ch.Publish(
				p.Exchange.Name,
				query.key,
				p.Mandatory,
				false, // rabbit do not support immediate exchanges anyway
				amqp.Publishing{
					ContentType:  encodedTrasferMimeType,
					Body:         query.data,
					DeliveryMode: p.deliveryMode,
				},
			)
			// We have troubles with chanel or connection.
			if err != nil {
				haveTroubles = true
				// see notice above
				query.replyChan <- err
				continue
			}
			waiters.Enqueue(pubWaiter{
				query:     query,
				confirmed: false,
			})
		case confirm := <-confirms:
			// Confirms chan will be closed before notify via closes chan.
			// We could get here before case below...
			if confirm.DeliveryTag == 0 {
				confirms = nil
				continue
			}
			p.handleConfirm(&waiters, confirm)
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
					p.handleConfirm(&waiters, confirm)
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
			// see notice above
			for iface := waiters.Dequeue(); iface != nil; iface = waiters.Dequeue() {
				waiter := iface.(pubWaiter)
				if !waiter.confirmed {
					waiter.query.replyChan <- err
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

	pub.lock.Lock()

	select {
	case <-global.conn.stopper.Chan():
		pub.lock.Unlock()
		return errors.New("connection stopped")
	default:
	}
	replyChan := make(chan error)
	pub.queryChan <- pubQuery{
		key:       routeKey,
		data:      bytes,
		replyChan: replyChan,
	}
	pub.lock.Unlock()
	return <-replyChan
}

// Should be called before Start().
func AddPublishers(pubs ...Publisher) {
	for _, pub := range pubs {
		if pub.Name == "" {
			log.Fatalf("rabbit: empty publisher name")
		}
		err := pub.Exchange.Args.Validate()
		if err != nil {
			log.Fatalf("rabbit: invalid arguments for exchange '%v': %v", pub.Exchange.Name, err)
		}
		if pub.DefaultExchange && pub.Exchange.Name != "" {
			log.Fatalf("rabbit: non-empty exchange with DefaultExchange flag in publisher '%v'", pub.Name)
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
	// @TODO routes
	// Local name for identify subscription
	Name    string
	Queue   Queue
	AutoAck bool
	// When exclusive is true, the server will ensure that this is the sole consumer from this queue.
	Exclusive bool
	Args      amqp.Table

	// Basic handler. If AutoAck is false, function must call Ack() or Reject() method of delivery before exit.
	Handler func(delivery amqp.Delivery)
	// Easier way: data will be decoded with json.Unmarshal, ack or requeue will be performed based on return value.
	// Should be func(decodedArg something) bool.
	// @TODO Do we need a way to reject delivery without requeue? It migh be useful to send it to dead letter exchange.
	DecodedHandler interface{}
}

func (s *Subscription) prepareHandler() {
	if s.DecodedHandler == nil {
		return
	}
	hType := reflect.TypeOf(s.DecodedHandler)
	ok := true
	if hType.Kind() != reflect.Func {
		ok = false
	}
	if hType.NumOut() != 1 || hType.Out(0).Kind() != reflect.Bool {
		ok = false
	}
	if hType.NumIn() != 1 {
		ok = false
	}
	if !ok {
		log.Fatalf("rabbit: DecodedHandler for subscription %v has unexpected type", s.Name)
	}
	argType := hType.In(0)
	hValue := reflect.ValueOf(s.DecodedHandler)

	s.Handler = func(m amqp.Delivery) {
		if m.ContentType != encodedTrasferMimeType {
			log.Debug("got delivery with unexpected mime type %+v", m.ContentType)
			if !s.AutoAck {
				// Remove it from queue any way
				m.Ack(false)
			}
			return
		}
		var argPtr reflect.Value
		if argType.Kind() != reflect.Ptr {
			argPtr = reflect.New(argType)
		} else {
			argPtr = reflect.New(argType.Elem())
		}
		if err := json.Unmarshal(m.Body, argPtr.Interface()); err != nil {
			log.Errorf("rabbit: failed to unmarshal argument of subscription '%v': %v\nData: %v", s.Name, err, string(m.Body))
			if !s.AutoAck {
				m.Ack(false)
			}
			return
		}
		if argType.Kind() != reflect.Ptr {
			argPtr = reflect.Indirect(argPtr)
		}
		var args []reflect.Value
		args = []reflect.Value{argPtr}
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
		} else if name, err := declareQueue(ch, &s.Queue); err != nil {
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
			log.Debug("starting consuming(%v)", name)
			for delivery := range deliveries {
				s.Handler(delivery)
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

func Subscribe(subscriptions ...Subscription) {
	for _, sub := range subscriptions {
		if sub.Name == "" {
			log.Fatalf("rabbit: empty subscription name")
		}
		if err := sub.Args.Validate(); err != nil {
			log.Fatalf("rabbit: invalid arguments in subscripiton '%v': %v", sub.Name, err)
		}
		if err := sub.Queue.Args.Validate(); err != nil {
			log.Fatalf("rabbit: invalid arguments for queue in subscription '%v': %v", sub.Name, err)
		}
		if sub.DecodedHandler != nil {
			sub.prepareHandler()
		}
		if sub.Handler == nil {
			log.Fatalf("rabbit: subscripiton '%v' do not have any handlers", sub.Queue.Name)
		}
		global.subscriptions[sub.Name] = &sub
	}
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
