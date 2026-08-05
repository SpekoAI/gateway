package runtime

import (
	"context"
	"sync"
)

type inputKind uint8

const (
	inputAudio inputKind = iota
	inputAudioCommit
	inputTextAppend
	inputTextCommit
	inputCancel
)

type audioInput struct {
	data    []byte
	release func()
}

type inputMessage struct {
	kind  inputKind
	audio audioInput
	text  string
}

// inputQueue is a fixed-capacity ring. It never copies audio data and makes no
// per-message allocation after construction.
type inputQueue struct {
	mu       sync.Mutex
	items    []inputMessage
	head     int
	count    int
	bytes    int
	maxBytes int
	closed   bool
	notEmpty chan struct{}
}

func newInputQueue(maxMessages, maxBytes int) *inputQueue {
	return &inputQueue{
		items:    make([]inputMessage, maxMessages),
		maxBytes: maxBytes,
		notEmpty: make(chan struct{}, 1),
	}
}

func (q *inputQueue) tryPush(message inputMessage) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		return ErrSessionClosed
	}
	bytes := len(message.audio.data)
	if bytes > q.maxBytes {
		return ErrFrameTooLarge
	}
	if q.count == len(q.items) || q.bytes+bytes > q.maxBytes {
		return ErrBackpressure
	}
	index := (q.head + q.count) % len(q.items)
	q.items[index] = message
	q.count++
	q.bytes += bytes
	q.signalLocked()
	return nil
}

func (q *inputQueue) pop(ctx context.Context) (inputMessage, bool) {
	for {
		q.mu.Lock()
		if q.count > 0 {
			message := q.items[q.head]
			q.items[q.head] = inputMessage{}
			q.head = (q.head + 1) % len(q.items)
			q.count--
			q.bytes -= len(message.audio.data)
			if q.count > 0 {
				q.signalLocked()
			}
			q.mu.Unlock()
			return message, true
		}
		if q.closed {
			q.mu.Unlock()
			return inputMessage{}, false
		}
		notEmpty := q.notEmpty
		q.mu.Unlock()

		select {
		case <-ctx.Done():
			return inputMessage{}, false
		case <-notEmpty:
		}
	}
}

// close allows already queued work to drain in order.
func (q *inputQueue) close() {
	q.mu.Lock()
	if !q.closed {
		q.closed = true
		q.signalLocked()
	}
	q.mu.Unlock()
}

// abort drops queued audio and invokes each accepted buffer's release callback
// exactly once. It is used only on terminal failure, never on the hot path.
func (q *inputQueue) abort() {
	var releases []func()

	q.mu.Lock()
	if q.closed && q.count == 0 {
		q.mu.Unlock()
		return
	}
	q.closed = true
	for q.count > 0 {
		message := q.items[q.head]
		q.items[q.head] = inputMessage{}
		q.head = (q.head + 1) % len(q.items)
		q.count--
		q.bytes -= len(message.audio.data)
		if message.kind == inputAudio && message.audio.release != nil {
			releases = append(releases, message.audio.release)
		}
	}
	q.signalLocked()
	q.mu.Unlock()

	for _, release := range releases {
		release()
	}
}

func (q *inputQueue) stats() (int, int) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.count, q.bytes
}

func (q *inputQueue) signalLocked() {
	select {
	case q.notEmpty <- struct{}{}:
	default:
	}
}
