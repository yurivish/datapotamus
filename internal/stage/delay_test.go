package stage

import (
	"testing"
	"testing/synctest"
	"time"

	"datapotamus.com/internal/msg"
)

// todo
// - test with multiple messages
// - test for clean shutdown if the in/out channels close.
//   - i think in channel currently returns with a nil error
//   - i think out channel closing would crash if you try to send a message. not sure what the semantics should be.
// - add the ability to concurrently delay multiple messages, ie. launch a goroutine per incoming message
// - add a randomize flag to treat dur as a maximum duration
// - decide if we need to handle sends to a closed out channel

// Test that delay stage delays messages and preserves parent-child relationships
func testDelayStageWithDuration(t *testing.T, millis int64) {
	// Create the stage
	dur := time.Duration(millis) * time.Millisecond
	delay, err := NewDelay("test-delay", DelayConfig{Millis: millis})
	if err != nil {
		t.Fatalf("failed to create delay stage: %v", err)
	}

	// Create channels for communication
	in := make(chan msg.InMsg, 1)
	out := make(chan msg.OutMsg, 1)
	delay.Init(Config{In: in, Out: out})

	ctx := t.Context()
	errCh := make(chan error, 1)
	go func() {
		errCh <- delay.Serve(ctx)
	}()

	data := "test data"
	inMsg := msg.New(data).In(msg.NewAddr("test-delay", "in"))

	start := time.Now()
	in <- inMsg

	// Receive the delayed message
	select {
	case outMsg := <-out:
		elapsed := time.Since(start)

		if elapsed < dur {
			t.Errorf("message was not delayed enough: expected at least %v, got %v", dur, elapsed)
		}

		// Verify parent-child relationship
		if outMsg.ParentID != inMsg.ID {
			t.Errorf("parent-child relationship incorrect: expected ParentID=%s, got ParentID=%s",
				inMsg.ID, outMsg.ParentID)
		}

		// Verify data is preserved
		if outMsg.Data != data {
			t.Errorf("message data not preserved: expected %v, got %v", data, outMsg.Data)
		}

		// Verify the output address is correct
		if outMsg.Addr != msg.NewAddr("test-delay", "out") {
			t.Errorf("incorrect output address: expected stage=test-delay, port=out, got stage=%s, port=%s",
				outMsg.Addr.Stage, outMsg.Addr.Port)
		}

	case <-time.After(max(dur, 0) + 1*time.Millisecond):
		t.Fatal("timeout waiting for delayed message")
	}

	t.Cleanup(func() {
		err = <-errCh
		if err != nil {
			t.Fatalf("error shutting down stage: %v", err)
		}
	})
}

func TestDelayStage(t *testing.T) {
	t.Run("delays message and creates correct parent-child relationship", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			testDelayStageWithDuration(t, -100)
			testDelayStageWithDuration(t, 0)
			testDelayStageWithDuration(t, 100)
			testDelayStageWithDuration(t, 1000)

		})
	})
}
