package natsll

import (
	"errors"
	"github.com/nats-io/gnatsd/server"
	gnatsd "github.com/nats-io/gnatsd/test"
	"github.com/nats-io/go-nats"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"net"
	"testing"
	"time"
)

const subj = "parallel"
const queue = "concurrent"

func TestSubscribe(t *testing.T) {
	s := runNatsServer()
	defer s.Shutdown()

	nc, err := nats.Connect(s.Addr().String())
	require.NoError(t, err)
	defer nc.Close()

	msgCh := make(chan string, 2)
	done := make(chan bool)

	sub, err := Subscribe(nc, subj, func(msg *nats.Msg) {
		msgCh <- string(msg.Data)
		// block
		done <- true
	})
	require.NoError(t, err)
	require.NotNil(t, sub)
	assert.Equal(t, sub.Subject, subj)

	// WHEN
	require.NoError(t, nc.Publish(subj, []byte("A")))
	require.NoError(t, nc.Publish(subj, []byte("B")))

	// THEN
	receivedAll := hasReceived([]string{"A", "B"}, msgCh)
	assert.True(t, receivedAll, "Hasn't received messages within timeout")

	// unblock
	assert.NoError(t, wait(done))
	assert.NoError(t, wait(done))
}

func TestQueueSubscribe(t *testing.T) {
	s := runNatsServer()
	defer s.Shutdown()

	nc, err := nats.Connect(s.Addr().String())
	require.NoError(t, err)
	defer nc.Close()

	msgCh := make(chan string, 2)
	done := make(chan bool)

	sub, err := QueueSubscribe(nc, subj, queue, func(msg *nats.Msg) {
		msgCh <- string(msg.Data)
		// block
		done <- true
	})
	require.NoError(t, err)
	require.NotNil(t, sub)
	assert.Equal(t, sub.Subject, subj)
	assert.Equal(t, sub.Queue, queue)

	// WHEN
	require.NoError(t, nc.Publish(subj, []byte("A")))
	require.NoError(t, nc.Publish(subj, []byte("B")))

	// THEN
	receivedAll := hasReceived([]string{"A", "B"}, msgCh)
	assert.True(t, receivedAll, "Hasn't received messages within timeout")

	// unblock
	assert.NoError(t, wait(done))
	assert.NoError(t, wait(done))
}

func TestSubscribeEc(t *testing.T) {
	s := runNatsServer()
	defer s.Shutdown()

	ec, err := NewEncodedJsonConn(s.Addr().String())
	require.NoError(t, err)
	defer ec.Close()

	msgCh := make(chan string, 2)
	done := make(chan bool)

	sub, err := SubscribeEc(ec, subj, func(s string) {
		msgCh <- s
		// block
		done <- true
	})
	require.NoError(t, err)
	require.NotNil(t, sub)
	assert.Equal(t, sub.Subject, subj)

	// WHEN
	require.NoError(t, ec.Publish(subj, "A"))
	require.NoError(t, ec.Publish(subj, "B"))

	// THEN
	receivedAll := hasReceived([]string{"A", "B"}, msgCh)
	assert.True(t, receivedAll, "Hasn't received messages within timeout")

	// unblock
	assert.NoError(t, wait(done))
	assert.NoError(t, wait(done))
}

func TestSubscribeEc2ArgHandler(t *testing.T) {
	s := runNatsServer()
	defer s.Shutdown()

	ec, err := NewEncodedJsonConn(s.Addr().String())
	require.NoError(t, err)
	defer ec.Close()

	msgCh := make(chan string, 2)
	done := make(chan bool)

	sub, err := SubscribeEc(ec, subj, func(sub, s string) {
		msgCh <- s
		// block
		done <- true
	})
	require.NoError(t, err)
	require.NotNil(t, sub)
	assert.Equal(t, sub.Subject, subj)

	// WHEN
	require.NoError(t, ec.Publish(subj, "A"))
	require.NoError(t, ec.Publish(subj, "B"))

	// THEN
	receivedAll := hasReceived([]string{"A", "B"}, msgCh)
	assert.True(t, receivedAll, "Hasn't received messages within timeout")

	// unblock
	assert.NoError(t, wait(done))
	assert.NoError(t, wait(done))
}

func TestSubscribeEc3ArgHandler(t *testing.T) {
	s := runNatsServer()
	defer s.Shutdown()

	ec, err := NewEncodedJsonConn(s.Addr().String())
	require.NoError(t, err)
	defer ec.Close()

	msgCh := make(chan string, 2)
	done := make(chan bool)

	sub, err := SubscribeEc(ec, subj, func(sub, reply, s string) {
		msgCh <- s
		// block
		done <- true
	})
	require.NoError(t, err)
	require.NotNil(t, sub)
	assert.Equal(t, sub.Subject, subj)

	// WHEN
	require.NoError(t, ec.Publish(subj, "A"))
	require.NoError(t, ec.Publish(subj, "B"))

	// THEN
	receivedAll := hasReceived([]string{"A", "B"}, msgCh)
	assert.True(t, receivedAll, "Hasn't received messages within timeout")

	// unblock
	assert.NoError(t, wait(done))
	assert.NoError(t, wait(done))
}

func TestQueueSubscribeEc(t *testing.T) {
	s := runNatsServer()
	defer s.Shutdown()

	ec, err := NewEncodedJsonConn(s.Addr().String())
	require.NoError(t, err)
	defer ec.Close()

	msgCh := make(chan string, 2)
	done := make(chan bool)

	sub, err := QueueSubscribeEc(ec, subj, queue, func(s string) {
		msgCh <- s
		// block
		done <- true
	})
	require.NoError(t, err)
	require.NotNil(t, sub)
	assert.Equal(t, sub.Subject, subj)

	// WHEN
	require.NoError(t, ec.Publish(subj, "A"))
	require.NoError(t, ec.Publish(subj, "B"))

	// THEN
	receivedAll := hasReceived([]string{"A", "B"}, msgCh)
	assert.True(t, receivedAll, "Hasn't received messages within timeout")

	// unblock
	assert.NoError(t, wait(done))
	assert.NoError(t, wait(done))
}

func TestQueueSubscribeEc2ArgHandler(t *testing.T) {
	s := runNatsServer()
	defer s.Shutdown()

	ec, err := NewEncodedJsonConn(s.Addr().String())
	require.NoError(t, err)
	defer ec.Close()

	msgCh := make(chan string, 2)
	done := make(chan bool)

	sub, err := QueueSubscribeEc(ec, subj, queue, func(sub, s string) {
		msgCh <- s
		// block
		done <- true
	})
	require.NoError(t, err)
	require.NotNil(t, sub)
	assert.Equal(t, sub.Subject, subj)

	// WHEN
	require.NoError(t, ec.Publish(subj, "A"))
	require.NoError(t, ec.Publish(subj, "B"))

	// THEN
	receivedAll := hasReceived([]string{"A", "B"}, msgCh)
	assert.True(t, receivedAll, "Hasn't received messages within timeout")

	// unblock
	assert.NoError(t, wait(done))
	assert.NoError(t, wait(done))
}

func TestQueueSubscribeEc3ArgHandler(t *testing.T) {
	s := runNatsServer()
	defer s.Shutdown()

	ec, err := NewEncodedJsonConn(s.Addr().String())
	require.NoError(t, err)
	defer ec.Close()

	msgCh := make(chan string, 2)
	done := make(chan bool)

	sub, err := QueueSubscribeEc(ec, subj, queue, func(sub, reply, s string) {
		msgCh <- s
		// block
		done <- true
	})
	require.NoError(t, err)
	require.NotNil(t, sub)
	assert.Equal(t, sub.Subject, subj)

	// WHEN
	require.NoError(t, ec.Publish(subj, "A"))
	require.NoError(t, ec.Publish(subj, "B"))

	// THEN
	receivedAll := hasReceived([]string{"A", "B"}, msgCh)
	assert.True(t, receivedAll, "Hasn't received messages within timeout")

	// unblock
	assert.NoError(t, wait(done))
	assert.NoError(t, wait(done))
}

func hasReceived(expected []string, ch chan string) bool {
	m := map[string]struct{}{}
	for _, s := range expected {
		m[s] = struct{}{}
	}
	for len(m) > 0 {
		select {
		case s := <-ch:
			delete(m, s)
		case <-time.After(5 * time.Second):
			return false
		}
	}
	return true
}

func wait(ch chan bool) error {
	select {
	case <-ch:
		return nil
	case <-time.After(2 * time.Second):
	}
	return errors.New("timeout exceeded")
}

func runNatsServer() *server.Server {
	p := getFreePort()
	opts := gnatsd.DefaultTestOptions
	opts.Port = p
	return gnatsd.RunServer(&opts)
}

func getFreePort() int {
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		panic(err)
	}

	defer func() {
		if err := listener.Close(); err != nil {
			panic(err)
		}
	}()
	return listener.Addr().(*net.TCPAddr).Port
}
