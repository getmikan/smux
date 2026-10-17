package smux

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// stallConn is a connection whose writes can be held at a gate. It records
// the bytes it is handed at the moment a write goes through, so a test can
// check what would have reached the wire.
type stallConn struct {
	armed   int32
	entered chan struct{} // receives once a write is held at the gate
	gate    chan struct{} // closed to let held writes through
	gateOne sync.Once

	closed    chan struct{}
	closeOnce sync.Once

	mu   sync.Mutex
	sent bytes.Buffer
}

func newStallConn() *stallConn {
	return &stallConn{
		entered: make(chan struct{}, 1),
		gate:    make(chan struct{}),
		closed:  make(chan struct{}),
	}
}

func (c *stallConn) hold() {
	if atomic.LoadInt32(&c.armed) == 0 {
		return
	}
	select {
	case c.entered <- struct{}{}:
	default:
	}
	<-c.gate
}

func (c *stallConn) release() { c.gateOne.Do(func() { close(c.gate) }) }

func (c *stallConn) Write(p []byte) (int, error) {
	c.hold()
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sent.Write(p)
	return len(p), nil
}

func (c *stallConn) Read(p []byte) (int, error) {
	<-c.closed
	return 0, io.EOF
}

// Close lets held writes through only a little later, like a socket that
// takes a moment to fail the write that is in progress.
func (c *stallConn) Close() error {
	c.closeOnce.Do(func() {
		close(c.closed)
		go func() {
			time.Sleep(50 * time.Millisecond)
			c.release()
		}()
	})
	return nil
}

// frames parses what was written into frames.
func (c *stallConn) frames(t *testing.T) []Frame {
	c.mu.Lock()
	defer c.mu.Unlock()
	b := c.sent.Bytes()
	var out []Frame
	for len(b) > 0 {
		if len(b) < headerSize {
			t.Fatalf("truncated header: %d bytes left", len(b))
		}
		n := int(binary.LittleEndian.Uint16(b[2:]))
		if len(b) < headerSize+n {
			t.Fatalf("truncated frame: want %d bytes, have %d", n, len(b)-headerSize)
		}
		out = append(out, Frame{ver: b[0], cmd: b[1], sid: binary.LittleEndian.Uint32(b[4:]), data: b[headerSize : headerSize+n]})
		b = b[headerSize+n:]
	}
	return out
}

// stallVecConn takes the scatter-gather path in sendLoop, which hands the
// caller's buffer itself to the connection.
type stallVecConn struct{ *stallConn }

func (c stallVecConn) WriteBuffers(v [][]byte) (int, error) {
	c.hold()
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, b := range v {
		c.sent.Write(b)
		n += len(b)
	}
	return n, nil
}

const poisonByte = 0xEE

// TestWriteBufferNotReadAfterReturn checks that once Stream.Write returns,
// sendLoop no longer reads the caller's buffer: the caller may hand it to
// someone else (a pool) right away. Writes are left in flight behind a stuck
// socket write, then abandoned by closing the session or by a deadline; every
// writer poisons its buffer as soon as Write returns, and no frame on the
// wire may carry the poison.
func TestWriteBufferNotReadAfterReturn(t *testing.T) {
	for _, version := range []int{1, 2} {
		for _, vectorised := range []bool{false, true} {
			for _, by := range []string{"close", "deadline"} {
				name := fmt.Sprintf("v%d/vectorised=%v/%s", version, vectorised, by)
				t.Run(name, func(t *testing.T) {
					testWriteBufferNotReadAfterReturn(t, version, vectorised, by == "deadline")
				})
			}
		}
	}
}

func testWriteBufferNotReadAfterReturn(t *testing.T, version int, vectorised, byDeadline bool) {
	sc := newStallConn()
	var conn io.ReadWriteCloser = sc
	if vectorised {
		conn = stallVecConn{sc}
	}

	config := DefaultConfig()
	config.Version = version
	config.KeepAliveDisabled = true
	session, err := Client(conn, config)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()

	const writers = 4
	streams := make([]*Stream, writers)
	fill := make(map[uint32]byte)
	for i := range streams {
		if streams[i], err = session.OpenStream(); err != nil {
			t.Fatal(err)
		}
		fill[streams[i].ID()] = byte(i + 1)
		if byDeadline {
			streams[i].SetWriteDeadline(time.Now().Add(300 * time.Millisecond))
		}
	}

	// From now on socket writes are held until the gate opens.
	atomic.StoreInt32(&sc.armed, 1)

	var returned int32
	var wg sync.WaitGroup
	write := func(i int) {
		defer wg.Done()
		buf := bytes.Repeat([]byte{byte(i + 1)}, 1000)
		streams[i].Write(buf)
		atomic.AddInt32(&returned, 1)
		// Write has returned: the buffer is ours again, e.g. back to a pool
		// and overwritten by another connection.
		for j := range buf {
			buf[j] = poisonByte
		}
	}

	// The first write gets stuck inside the socket write...
	wg.Add(1)
	go write(0)
	select {
	case <-sc.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("first write never reached the connection")
	}
	// ...and the rest queue up behind it.
	for i := 1; i < writers; i++ {
		wg.Add(1)
		go write(i)
	}
	waitFor(t, "queued writes", func() bool { return session.sq.Len() == writers-1 })

	if byDeadline {
		// The queued writers time out while the socket is still stuck.
		waitFor(t, "queued writers to time out", func() bool { return atomic.LoadInt32(&returned) >= writers-1 })
		sc.release()
	} else {
		session.Close()
	}
	wg.Wait()
	session.Close()
	sc.release()

	// sendLoop drains what it has popped before it notices the session is
	// gone; give it a moment so a late read of a returned buffer would land.
	time.Sleep(100 * time.Millisecond)

	for _, f := range sc.frames(t) {
		if f.cmd != cmdPSH {
			continue
		}
		want, ok := fill[f.sid]
		if !ok {
			t.Fatalf("data frame for unknown stream %d", f.sid)
		}
		for i, b := range f.data {
			if b != want {
				t.Fatalf("stream %d: byte %d on the wire is %#x, want %#x: sendLoop read the buffer after Write returned", f.sid, i, b, want)
			}
		}
	}
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}
