package simulator

import (
	"bytes"
	"context"
	mrand "math/rand"
	"sort"
	"sync/atomic"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/brocaar/chirpstack-simulator/simulator"
	"github.com/brocaar/lorawan"
)

type deviceCredential struct {
	devEUI lorawan.EUI64
	appKey lorawan.AES128Key
}

func (s *simulation) deviceCredentials() []deviceCredential {
	creds := make([]deviceCredential, 0, len(s.deviceAppKeys))
	for devEUI, appKey := range s.deviceAppKeys {
		creds = append(creds, deviceCredential{devEUI: devEUI, appKey: appKey})
	}
	sort.Slice(creds, func(i, j int) bool {
		return bytes.Compare(creds[i].devEUI[:], creds[j].devEUI[:]) < 0
	})
	return creds
}

// joinGroupJitter spreads the first join of a group over a short window so
// the group does not transmit in the same millisecond. The group still keeps
// the rest of activation_time for retries.
func joinGroupJitter(activationTime time.Duration) time.Duration {
	const maxJitter = 2 * time.Second
	window := maxJitter
	if activationTime > 0 && activationTime < window {
		window = activationTime
	}
	if window <= 0 {
		return 0
	}
	return time.Duration(mrand.Int63n(int64(window)))
}

const defaultDeviceGroupSize = 100

func normalizeGroupSize(groupSize int) int {
	if groupSize < 1 {
		return defaultDeviceGroupSize
	}
	return groupSize
}

// deviceJoinGroups reports whether OTAA is paced one group at a time.
// A device count at or below the group size stays a single staggered wave.
func deviceJoinGroups(deviceCount, groupSize int) (grouped bool, groups int) {
	groupSize = normalizeGroupSize(groupSize)
	if deviceCount <= 0 {
		return false, 0
	}
	if deviceCount <= groupSize {
		return false, 1
	}
	return true, (deviceCount + groupSize - 1) / groupSize
}

func waitForActivations(ctx context.Context, activated []<-chan struct{}, timeout time.Duration) int {
	if timeout <= 0 {
		timeout = time.Minute
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	var n int32
	done := make(chan struct{})
	for _, ch := range activated {
		go func(ch <-chan struct{}) {
			select {
			case <-ch:
				atomic.AddInt32(&n, 1)
			case <-ctx.Done():
			}
			done <- struct{}{}
		}(ch)
	}
	for range activated {
		<-done
	}
	return int(atomic.LoadInt32(&n))
}

func activatedCount(devices []*simulator.Device) int {
	n := 0
	for _, d := range devices {
		select {
		case <-d.Activated():
			n++
		default:
		}
	}
	return n
}

// runJoinGroups lets one group send join-requests at a time. The group
// retries until every device activates or activationTime elapses. Groups
// that are still waiting are repeated until the simulation context ends.
func (s *simulation) runJoinGroups(ctx context.Context, gates []*simulator.JoinGate, devices []*simulator.Device) {
	defer func() {
		for _, gate := range gates {
			gate.Pause()
		}
	}()

	size := normalizeGroupSize(s.deviceGroupSize)
	timeout := s.activationTime
	if timeout <= 0 {
		timeout = time.Minute
	}
	if s.duration > 0 {
		pass := time.Duration(len(gates)) * timeout
		if pass > s.duration {
			log.WithFields(log.Fields{
				"groups":          len(gates),
				"activation_time": timeout,
				"pass":            pass,
				"duration":        s.duration,
			}).Info("simulator: one join pass is longer than the simulation duration; later groups run only while the simulation is still active")
		}
	}

	for round := 1; ctx.Err() == nil; round++ {
		if activatedCount(devices) == len(devices) {
			log.WithFields(log.Fields{
				"devices": len(devices),
				"round":   round,
			}).Info("simulator: all device groups activated")
			return
		}

		for g := range gates {
			if ctx.Err() != nil {
				log.WithFields(log.Fields{
					"round":     round,
					"group":     g + 1,
					"groups":    len(gates),
					"activated": activatedCount(devices),
					"devices":   len(devices),
				}).Info("simulator: join groups stopped")
				return
			}

			begin := g * size
			end := begin + size
			if end > len(devices) {
				end = len(devices)
			}
			group := devices[begin:end]
			pending := len(group) - activatedCount(group)
			if pending == 0 {
				continue
			}

			log.WithFields(log.Fields{
				"round":   round,
				"group":   g + 1,
				"groups":  len(gates),
				"devices": len(group),
				"pending": pending,
			}).Info("simulator: starting join group")

			gates[g].Allow()
			activated := waitForActivations(ctx, activationChans(group), timeout)
			gates[g].Pause()

			log.WithFields(log.Fields{
				"round":     round,
				"group":     g + 1,
				"groups":    len(gates),
				"activated": activated,
				"devices":   len(group),
			}).Info("simulator: join group finished")
		}
	}
}

func activationChans(devices []*simulator.Device) []<-chan struct{} {
	out := make([]<-chan struct{}, len(devices))
	for i, d := range devices {
		out[i] = d.Activated()
	}
	return out
}
