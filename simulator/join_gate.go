package simulator

import (
	"context"
	"sync"
	"time"
)

// JoinGate allows or pauses OTAA join-requests for one device group.
// A new gate is paused. Allow opens it and Pause closes it again.
type JoinGate struct {
	mu     sync.Mutex
	open   bool
	notify chan struct{}
}

// NewJoinGate returns a paused gate.
func NewJoinGate() *JoinGate {
	return &JoinGate{notify: make(chan struct{})}
}

// Allow lets devices in this group send join-requests.
func (g *JoinGate) Allow() {
	g.set(true)
}

// Pause stops further join-requests until Allow is called again.
func (g *JoinGate) Pause() {
	g.set(false)
}

// IsOpen reports whether join-requests are currently allowed.
func (g *JoinGate) IsOpen() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.open
}

func (g *JoinGate) set(open bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.open == open {
		return
	}
	g.open = open
	close(g.notify)
	g.notify = make(chan struct{})
}

// Wait blocks until the gate is allowed or ctx is cancelled.
func (g *JoinGate) Wait(ctx context.Context) bool {
	for {
		g.mu.Lock()
		open := g.open
		notify := g.notify
		g.mu.Unlock()
		if open {
			return true
		}
		select {
		case <-ctx.Done():
			return false
		case <-notify:
		}
	}
}

// WaitInterval waits for d, or returns earlier when the gate is paused or
// ctx is cancelled. It returns false only when ctx is cancelled.
func (g *JoinGate) WaitInterval(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()

	for {
		g.mu.Lock()
		open := g.open
		notify := g.notify
		g.mu.Unlock()
		if !open {
			return true
		}
		select {
		case <-ctx.Done():
			return false
		case <-timer.C:
			return true
		case <-notify:
		}
	}
}
