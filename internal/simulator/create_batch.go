package simulator

import "time"

const (
	// createBatchSize is how many devices or gateways are created before a pause.
	createBatchSize = 1000
	// createBatchThreshold is the count above which creation is split into batches.
	createBatchThreshold = 5000
	// createBatchPause is the wait between those batches.
	createBatchPause = time.Second
	// deviceCreateParallel is the in-flight device creates for a count of 5000 or less.
	deviceCreateParallel = 4
	// deviceCreateTimeout bounds one ChirpStack device or device-key RPC.
	deviceCreateTimeout = 30 * time.Second
)

// deviceCreateConcurrency is how many device creates may run at once.
// Above the batch threshold, creates are serial so ChirpStack is not flooded.
func deviceCreateConcurrency(total, batch int) int {
	if batch < 1 {
		return 1
	}
	limit := deviceCreateParallel
	if total > createBatchThreshold {
		limit = 1
	}
	if batch < limit {
		return batch
	}
	return limit
}

// createBatchSizes splits count into batches of createBatchSize when count is
// greater than createBatchThreshold. Smaller counts stay one batch, so no
// pause is inserted.
func createBatchSizes(count int) []int {
	if count <= 0 {
		return nil
	}
	if count <= createBatchThreshold {
		return []int{count}
	}

	sizes := make([]int, 0, (count+createBatchSize-1)/createBatchSize)
	for remaining := count; remaining > 0; {
		n := createBatchSize
		if n > remaining {
			n = remaining
		}
		sizes = append(sizes, n)
		remaining -= n
	}
	return sizes
}

// pauseBetweenCreates reports whether creation should wait before starting
// the object at index created. created is how many objects already exist.
func pauseBetweenCreates(created, total int) bool {
	return total > createBatchThreshold && created > 0 && created%createBatchSize == 0 && created < total
}
