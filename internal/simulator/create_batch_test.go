package simulator

import "testing"

func TestCreateBatchSizes(t *testing.T) {
	if got := createBatchSizes(0); got != nil {
		t.Fatalf("zero count = %v, want nil", got)
	}

	one := createBatchSizes(5000)
	if len(one) != 1 || one[0] != 5000 {
		t.Fatalf("5000 devices = %v, want a single batch", one)
	}

	got := createBatchSizes(10000)
	if len(got) != 10 {
		t.Fatalf("10000 devices produced %d batches, want 10", len(got))
	}
	for i, n := range got {
		if n != 1000 {
			t.Fatalf("batch %d = %d, want 1000", i, n)
		}
	}

	partial := createBatchSizes(5001)
	if len(partial) != 6 || partial[5] != 1 {
		t.Fatalf("5001 devices = %v, want five batches of 1000 and a final 1", partial)
	}
}

func TestPauseBetweenCreates(t *testing.T) {
	if pauseBetweenCreates(1000, 5000) {
		t.Fatal("5000 devices should be created without a pause")
	}
	if pauseBetweenCreates(0, 10000) {
		t.Fatal("paused before the first device")
	}
	if !pauseBetweenCreates(1000, 10000) || !pauseBetweenCreates(9000, 10000) {
		t.Fatal("expected a pause after each 1000 devices while more remain")
	}
	if pauseBetweenCreates(10000, 10000) {
		t.Fatal("paused after the last device")
	}
	if pauseBetweenCreates(1500, 10000) {
		t.Fatal("paused in the middle of a 1000-device batch")
	}
}

func TestDeviceCreateConcurrency(t *testing.T) {
	if got := deviceCreateConcurrency(6000, 1000); got != 1 {
		t.Fatalf("concurrency above 5000 = %d, want 1", got)
	}
	if got := deviceCreateConcurrency(5000, 5000); got != deviceCreateParallel {
		t.Fatalf("concurrency at 5000 = %d, want %d", got, deviceCreateParallel)
	}
	if got := deviceCreateConcurrency(2, 2); got != 2 {
		t.Fatalf("concurrency for 2 devices = %d, want 2", got)
	}
}
