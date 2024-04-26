package pubsub

import (
	"errors"
	"sync"
)

type PubsubOption struct {
	// maximum message stores in in-memory
	MaxMessage int
	// maximum workerpool
	WorkerPool int
}

type PubsubFunc func(opt *PubsubOption)

func SetMaxMessage(maxMessage int) PubsubFunc {
	return func(opt *PubsubOption) {
		opt.MaxMessage = maxMessage
	}
}

func SetWorkerPool(wp int) PubsubFunc {
	return func(opt *PubsubOption) {
		if wp == 0 {
			wp = 1
		}
		opt.WorkerPool = wp
	}
}

type Pubsub struct {
	maxWorkerpool int
	maxMessage    int
	mu            sync.Mutex
	message       map[string]chan MessageHandler
	consumer      map[string]Consumer
}

func NewPubsub(opts ...PubsubFunc) *Pubsub {
	// default value
	qopt := PubsubOption{
		WorkerPool: 5,
		MaxMessage: 100,
	}

	for _, opt := range opts {
		opt(&qopt)
	}

	q := Pubsub{
		maxWorkerpool: qopt.WorkerPool,
		maxMessage:    qopt.MaxMessage,
		message:       make(map[string]chan MessageHandler, qopt.MaxMessage),
		consumer:      make(map[string]Consumer),
	}

	return &q
}

// Registering consumer based on its topic
// once it've registered the dispacher will dispach the message to the handler
func (q *Pubsub) ConsumerRegister(topic string, handler ConsumerHandler) error {
	consumer := newConsumer(handler)
	q.mu.Lock()
	defer q.mu.Unlock()
	q.consumer[topic] = consumer
	if _, exist := q.message[topic]; !exist {
		q.message[topic] = make(chan MessageHandler, q.maxMessage)
	}

	return nil
}

func (q *Pubsub) MessageLength(topic string) int {
	return len(q.message[topic])
}

func (q *Pubsub) MessageCapacity(topic string) int {
	return cap(q.message[topic])
}

func (q *Pubsub) Publish(topic string, message MessageHandler) error {
	if _, exist := q.message[topic]; !exist {
		q.message[topic] = make(chan MessageHandler, q.maxMessage)
	}
	if _, exist := q.consumer[topic]; !exist {
		return errors.New("err: publishing message to unregistered consumer")
	}

	q.mu.Lock()
	defer q.mu.Unlock()
	q.message[topic] <- message

	return nil
}

func (q *Pubsub) Listen() error {
	for topic, consumer := range q.consumer {
		for i := 0; i < q.maxWorkerpool; i++ {
			go consumer.dispatcher(q.message[topic])
		}
	}

	return nil
}
