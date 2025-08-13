package pubsub

import (
	"context"
	"runtime"

	"datapotamus.com/internal/sublist"
	"github.com/oklog/ulid/v2"
)

// Simple pub-sub system, optimized for observability and ease of use.
// Based on the same subject structure as NATS's subject-based messaging system.
type PubSub struct {
	subs *sublist.Sublist
}

func NewPubSub() *PubSub {
	return &PubSub{subs: sublist.NewSublistWithCache()}
}

// Subscriber options.
type SubOptions struct {
	SkipCallers int  // Call stack depth to record caller information from for this subscription
	Debug       bool // Whether or not this is a debug subscription
}

// Core subscribe function.
// The handler code will be invoked synchronously on the goroutine which calls Pub.
// The handler can be one of two types:
// - func(subject string, message M) (see [Sub])
// - func(subject string, message any, *sublist.SublistResult) (see [DebugSub])
func sub(ps *PubSub, subj string, handler any, options ...SubOption) context.CancelFunc {
	// Determine the options for this subscription using the "functional options" pattern
	opts := SubOptions{SkipCallers: 1}
	for _, opt := range options {
		opt(&opts)
	}

	// Create the underlyng Subscription object, giving it a unique ID
	id := ulid.Make().String()
	sub := sublist.Subscription{Subject: []byte(subj), Value: handler, ID: id, Debug: opts.Debug}

	// Gather file and line information for subscription and include them
	// in the Subscription struct for debugging purposes if available
	if pc, file, line, ok := runtime.Caller(opts.SkipCallers); ok {
		sub.File = file
		sub.Line = line
		if fn := runtime.FuncForPC(pc); fn != nil {
			sub.FuncName = fn.Name()
		}
	}
	err := ps.subs.Insert(&sub)
	if err != nil {
		panic(err) // only possible error is "invalid subject" which is programmer error.
	}
	return func() {
		err := ps.subs.Remove(&sub)
		// `CancelFunc`s are required to be idempotent, so ignore not-found errors
		if err != nil && err != sublist.ErrNotFound {
			panic(err) // only possible error is "invalid subject" which is programmer error.
		}
	}
}

// The handler code will be run synchronously on the goroutine which calls Pub.
// The handler takes the subject as a first argument, and message as the second.
func Sub[M any](ps *PubSub, subj string, handler func(string, M), options ...SubOption) context.CancelFunc {
	options = append(options, WithSkip(1)) // Skip this stack frame when recording the subscriber for debug subs
	return sub(ps, subj, func(subj string, message any) {
		// The message might be nil, which we need to handle specially.
		// Or, later, we might mandate non-nil messages. But for now do this.
		if message == nil {
			var zero M
			handler(subj, zero)
		} else {
			handler(subj, message.(M))
		}
	}, options...)
}

// Debug subscriptions are marked as such in the Subscription object,
// and receive the full match result as a third argument.
// This might be highly useful for tracing.
func DebugSub(ps *PubSub, subj string, handler func(string, any, *sublist.SublistResult), options ...SubOption) context.CancelFunc {
	options = append(options, WithDebug(), WithSkip(1)) // Skip this stack frame when recording the subscriber for debug subs
	return sub(ps, subj, handler, options...)
}

// Returns a channel on which messages are sent.
// The user is NOT responsible for closing the channel.
// Both the subscription and channel will be closed once the context completes.
func SubChan[M any](ps *PubSub, ctx context.Context, subj string, bufSize int, options ...SubOption) <-chan M {
	options = append(options, WithSkip(1)) // Skip this stack frame when recording the subscriber
	ch := make(chan M, bufSize)
	cancel := Sub(ps, subj, func(subj string, message M) {
		select {
		case ch <- message:
		case <-ctx.Done():
		}
	}, options...)

	go func() {
		<-ctx.Done()
		cancel()
		close(ch)
	}()

	return ch
}

// Publish a message onto the given subject.
func Pub[M any](ps *PubSub, subj string, message M) {
	// Matches is a *sublist.SublistResult type from the NATS server.
	// - Psubs are plain subscribers
	// - Qsubs are queue group subscribers
	matches := ps.subs.Match(subj)
	for _, sub := range matches.Psubs {
		if sub.Debug {
			// DebugSub handlers are passed the subscriptions that matched this pub subject.
			handler := sub.Value.(func(string, any, *sublist.SublistResult))
			handler(subj, message, matches)
		} else {
			// Regular handlers are invoked with the subject and message.
			handler := sub.Value.(func(string, any))
			handler(subj, message)
		}
	}
	// TODO: Explore the "least loaded of 2 random options" idea:
	// > From https://danluu.com/2choices-eviction/:
	// > The Power of Two Random Choices: A Survey of Techniques and Results by Mitzenmacher, Richa, and Sitaraman
	// > (https://www.eecs.harvard.edu/~michaelm/postscripts/handbook2001.pdf)
	// > has a great explanation. The mathematical intuition is that if we (randomly) throw n balls into n bins,
	// > the maximum number of balls in any bin is O(log n / log log n) with high probability, which is pretty much
	// >  just O(log n). But if (instead of choosing randomly) we choose the least loaded of k random bins, the maximum
	// > is O(log log n / log k) with high probability, i.e., even with two random choices, it's basically O(log log n)
	// > and each additional choice only reduces the load by a constant factor.
	if len(matches.Qsubs) > 0 {
		panic("not designed for queue groups yet")
	}
}

// Represents an individual option using the "functional options" pattern
type SubOption func(*SubOptions)

// Used to increment the call depth so we can compos higher-level subscription functions like SubChan
func WithSkip(skip int) SubOption {
	return func(s *SubOptions) {
		s.SkipCallers += skip
	}
}

// Used to mark debug subscriptions, which are created with DebugSub
func WithDebug() SubOption {
	return func(s *SubOptions) {
		s.Debug = true
	}
}

// TODO: Batch pub. Would mean the whole batch is delivered to the first subscriber, then the second, then ...
