package simulator

import (
	"context"
	"testing"
	"time"
)

func TestJoinGateBlocksUntilAllowed(t *testing.T) {
	g := NewJoinGate()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if g.Wait(ctx) {
		t.Fatal("paused gate wait returned before Allow")
	}

	g.Allow()
	if !g.Wait(context.Background()) {
		t.Fatal("allowed gate wait failed")
	}
	if !g.IsOpen() {
		t.Fatal("gate is not open after Allow")
	}
}

func TestJoinGateWaitIntervalReturnsWhenPaused(t *testing.T) {
	g := NewJoinGate()
	g.Allow()

	done := make(chan bool, 1)
	go func() {
		done <- g.WaitInterval(context.Background(), time.Hour)
	}()

	time.Sleep(20 * time.Millisecond)
	g.Pause()

	select {
	case ok := <-done:
		if !ok {
			t.Fatal("pause was reported as context cancellation")
		}
	case <-time.After(time.Second):
		t.Fatal("WaitInterval did not return when the gate was paused")
	}
	if g.IsOpen() {
		t.Fatal("gate still open after Pause")
	}
}

func TestJoinGateWaitIntervalHonorsDuration(t *testing.T) {
	g := NewJoinGate()
	g.Allow()

	start := time.Now()
	if !g.WaitInterval(context.Background(), 40*time.Millisecond) {
		t.Fatal("interval wait failed")
	}
	if time.Since(start) < 30*time.Millisecond {
		t.Fatal("interval wait returned before the duration")
	}
}
