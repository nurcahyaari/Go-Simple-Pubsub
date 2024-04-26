package pubsub

import (
	"bytes"
	"encoding/json"
	"io"
	"strings"
)

type MessageHandler func() (io.Reader, error)

func SendJSON(data json.RawMessage) MessageHandler {
	return func() (io.Reader, error) {
		buff := bytes.Buffer{}

		err := json.NewEncoder(&buff).Encode(data)

		return &buff, err
	}
}

func SendString(data string) MessageHandler {
	return func() (io.Reader, error) {
		return strings.NewReader(data), nil
	}
}
