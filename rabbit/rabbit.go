package rabbit

import (
	"common/log"
	"common/stopper"
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"github.com/streadway/amqp"
	"sync"
	"time"
)

// Per topic limit for unconfirmed publishes.
// Extra queries will not be sent to broker until acks for older messages will have been received.
const UnconfirmedPublishLimit = 10

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
	// local name for use in Publish() func
	Name     string
	Exchange Exchange
	// Publishings can be undeliverable when the mandatory flag is true and no queue is bound that matches the routing key, or when the immediate flag is true and no consumer on the matched queue is ready to accept the delivery.
	Mandatory    bool
	Immediate    bool
	Persistent   bool
	deliveryMode uint8
	queryChan    chan pubQuery
	lock         sync.Mutex
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

func (p *Publisher) connLoop(c conn) {
	defer func() {
		c.wait.Done()
		log.Debug("connLoop exited")
	}()
	for {
		ch, err := c.amqp.Channel()
		// Most likely something is wrong with connection.
		if err != nil {
			c.amqp.Close()
			return
		} else if err = declareExchange(ch, &p.Exchange); err != nil {
			// Something is wrong with chanel or whole connection.
			// Or we are trying to redeclare exchange with incompatible settings, that should be logged.
			log.Errorf("failed to declare exchange %v: %v", p.Exchange.Name, err)
		} else if err = ch.Confirm(false); err != nil {
			// Probably we do not need to log errors from every chanel in case of troubles with connection
			// log.Error(err)
		} else {
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

func (p *Publisher) chanLoop(ch *amqp.Channel) {
	// @NOTICE I'm not sure about the way to handle troubles with chanel.
	// We could save all unconfirmed queries and try to send them again later.
	confirms := ch.NotifyPublish(make(chan amqp.Confirmation))
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
				p.Immediate,
				amqp.Publishing{
					ContentType:  "application/json",
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
			}
			p.handleConfirm(&waiters, confirm)
		case err := <-closes:
			if confirms != nil {
				// Be sure to read all conforms.
				for confirm := range confirms {
					log.Debug("confirm on close: %v", confirm)
					p.handleConfirm(&waiters, confirm)
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
	// @TODO nack
	if !waiters.ContainsTag(confirm.DeliveryTag) {
		// wtf? Something went really wrong.
		log.Errorf("amqp: got ack with unexpected tag %v", confirm.DeliveryTag)
		return
	}
	if waiters.FirstTag() == confirm.DeliveryTag {
		waiter := waiters.Dequeue().(pubWaiter)
		waiter.query.replyChan <- nil
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
		waiter.query.replyChan <- nil
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
	defer pub.lock.Unlock()

	select {
	case <-global.conn.stopper.Chan():
		return errors.New("connection stopped")
	default:
	}
	replyChan := make(chan error)
	pub.queryChan <- pubQuery{
		key:       routeKey,
		data:      bytes,
		replyChan: replyChan,
	}
	return <-replyChan
}

func AddPublishers(pubs ...Publisher) {
	for _, pub := range pubs {
		err := pub.Exchange.Args.Validate()
		if err != nil {
			log.Fatalf("rabbit: invalid arguments for exchange '%v': %v", pub.Exchange.Name, err)
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

type conn struct {
	amqp    *amqp.Connection
	wait    *sync.WaitGroup
	stopper *stopper.Stopper
}

var global = struct {
	conn       *conn
	publishers map[string]*Publisher
}{
	publishers: map[string]*Publisher{},
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
	defer func() {
		c.wait.Done()
		log.Debug("reconnectLoop exited")
	}()
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
