package pubsub

import (
	"io"

	"github.com/google/uuid"
)

type ConsumerHandler func(id uuid.UUID, message io.Reader) error

type Consumer struct {
	handler ConsumerHandler
}

func newConsumer(handler ConsumerHandler) Consumer {
	return Consumer{
		handler: handler,
	}
}

func (s *Consumer) dispatcher(data chan MessageHandler) {
	for {
		message := <-data

		id := uuid.New()
		msg, err := message()
		if err != nil {
			continue
		}
		if err := s.handler(id, msg); err != nil {
			continue
		}
	}
}
