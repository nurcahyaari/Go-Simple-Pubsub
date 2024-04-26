package pubsub_test

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"pubsub"
	"time"

	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestPubsub(t *testing.T) {
	type Person struct {
		Id   string
		Name string
	}
	t.Run("test-send json", func(t *testing.T) {
		chanWaiter := make(chan bool)
		qw := pubsub.NewPubsub(
			pubsub.SetMaxMessage(1000),
			pubsub.SetWorkerPool(5),
		)
		p1 := Person{
			Id:   "1",
			Name: "tester1",
		}

		qw.ConsumerRegister("test-send-1", func(id uuid.UUID, message io.Reader) error {
			assert.NotEmpty(t, id, "id is not empty")
			p := Person{}
			err := json.NewDecoder(message).Decode(&p)
			assert.NoError(t, err)
			assert.NotEmpty(t, p)
			chanWaiter <- true
			return nil
		})

		qw.Listen()

		byt, err := json.Marshal(p1)
		assert.NoError(t, err)

		err = qw.Publish("test-send-1", pubsub.SendJSON(byt))
		assert.NoError(t, err)

		err = qw.Publish("test-send-2", pubsub.SendJSON(byt))
		assert.Error(t, err)

		<-chanWaiter
	})

	t.Run("test-send string", func(t *testing.T) {
		wg := sync.WaitGroup{}
		qw := pubsub.NewPubsub(
			pubsub.SetMaxMessage(2),
			pubsub.SetWorkerPool(1),
		)

		start := time.Now()

		qw.ConsumerRegister("test-send-1", func(id uuid.UUID, message io.Reader) error {
			assert.NotEmpty(t, id, "id is not empty")
			buf := new(strings.Builder)
			_, err := io.Copy(buf, message)
			assert.NoError(t, err)
			assert.NotEmpty(t, buf)
			wg.Done()
			return nil
		})

		qw.Listen()

		for i := 0; i < 5; i++ {
			wg.Add(1)
			err := qw.Publish("test-send-1", pubsub.SendString(fmt.Sprintf("%d", i)))
			assert.NoError(t, err)
		}
		wg.Wait()

		elapsed := time.Since(start)
		log.Printf("it took %s", elapsed)

	})

	t.Run("test-send string - you forget to register the consumer", func(t *testing.T) {
		qw := pubsub.NewPubsub(
			pubsub.SetMaxMessage(5),
			pubsub.SetWorkerPool(1),
		)

		start := time.Now()

		qw.Listen()

		for i := 0; i < 5; i++ {
			err := qw.Publish("test-send-1", pubsub.SendString(fmt.Sprintf("%d", i)))
			assert.Error(t, err)
		}

		fmt.Println(qw.MessageLength("test-send-1"))
		fmt.Println(qw.MessageCapacity("test-send-1"))

		elapsed := time.Since(start)
		log.Printf("it took %s", elapsed)

	})

}
