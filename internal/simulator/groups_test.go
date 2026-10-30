package simulator

import (
	"context"
	"testing"
	"time"
)

func TestDeviceJoinGroups(t *testing.T) {
	cases := []struct {
		devices int
		size    int
		grouped bool
		groups  int
	}{
		{devices: 100, size: 100, grouped: false, groups: 1},
		{devices: 1, size: 100, grouped: false, groups: 1},
		{devices: 0, size: 100, grouped: false, groups: 0},
		{devices: 101, size: 100, grouped: true, groups: 2},
		{devices: 1000, size: 100, grouped: true, groups: 10},
		{devices: 10000, size: 100, grouped: true, groups: 100},
		{devices: 1000, size: 0, grouped: true, groups: 10},
	}

	for _, tc := range cases {
		grouped, groups := deviceJoinGroups(tc.devices, tc.size)
		if grouped != tc.grouped || groups != tc.groups {
			t.Fatalf("devices=%d size=%d: got grouped=%v groups=%d, want grouped=%v groups=%d",
				tc.devices, tc.size, grouped, groups, tc.grouped, tc.groups)
		}
	}
}

func TestWaitForActivations(t *testing.T) {
	ready := make(chan struct{})
	close(ready)
	waiting := make(chan struct{})

	ctx := context.Background()
	got := waitForActivations(ctx, []<-chan struct{}{ready, ready}, time.Second)
	if got != 2 {
		t.Fatalf("activated = %d, want 2", got)
	}

	start := time.Now()
	got = waitForActivations(ctx, []<-chan struct{}{waiting, ready}, 40*time.Millisecond)
	if got != 1 {
		t.Fatalf("activated = %d, want 1", got)
	}
	if time.Since(start) < 30*time.Millisecond {
		t.Fatal("wait returned before the group timeout")
	}

	cancelCtx, cancel := context.WithCancel(context.Background())
	cancel()
	got = waitForActivations(cancelCtx, []<-chan struct{}{waiting}, time.Hour)
	if got != 0 {
		t.Fatalf("cancelled wait activated = %d, want 0", got)
	}
}

func TestJoinGroupJitterStaysInsideWindow(t *testing.T) {
	for i := 0; i < 20; i++ {
		got := joinGroupJitter(time.Minute)
		if got < 0 || got >= 2*time.Second {
			t.Fatalf("jitter = %s, want 0s <= jitter < 2s", got)
		}
	}
	if joinGroupJitter(0) < 0 || joinGroupJitter(0) >= 2*time.Second {
		t.Fatal("zero activation time jitter left the 2s window")
	}
}
