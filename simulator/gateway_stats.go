package simulator

import (
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/chirpstack/chirpstack/api/go/v4/gw"
)

// intervalStats holds gateway counters for the current reporting period.
// ChirpStack adds each GatewayStats report into its metrics, so a report
// contains the packets since the previous successful publish, then the
// counters reset. Failed publishes keep the counters for the next report.
type intervalStats struct {
	rxReceived   uint32
	rxReceivedOK uint32
	txReceived   uint32
	txEmitted    uint32
	rxPerFreq    map[uint32]uint32
	txPerFreq    map[uint32]uint32
	txPerStatus  map[string]uint32
	rxPerMod     []*gw.PerModulationCount
	txPerMod     []*gw.PerModulationCount
}

func newIntervalStats() intervalStats {
	return intervalStats{
		rxPerFreq:   make(map[uint32]uint32),
		txPerFreq:   make(map[uint32]uint32),
		txPerStatus: make(map[string]uint32),
	}
}

func (s *intervalStats) add(o intervalStats) {
	s.rxReceived += o.rxReceived
	s.rxReceivedOK += o.rxReceivedOK
	s.txReceived += o.txReceived
	s.txEmitted += o.txEmitted
	for k, v := range o.rxPerFreq {
		s.rxPerFreq[k] += v
	}
	for k, v := range o.txPerFreq {
		s.txPerFreq[k] += v
	}
	for k, v := range o.txPerStatus {
		s.txPerStatus[k] += v
	}
	for _, m := range o.rxPerMod {
		addModulationCount(&s.rxPerMod, m.Modulation, m.Count)
	}
	for _, m := range o.txPerMod {
		addModulationCount(&s.txPerMod, m.Modulation, m.Count)
	}
}

func addModulationCount(dst *[]*gw.PerModulationCount, mod *gw.Modulation, n uint32) {
	if mod == nil || n == 0 {
		return
	}
	for _, c := range *dst {
		if proto.Equal(c.Modulation, mod) {
			c.Count += n
			return
		}
	}
	cloned, ok := proto.Clone(mod).(*gw.Modulation)
	if !ok || cloned == nil {
		return
	}
	*dst = append(*dst, &gw.PerModulationCount{
		Modulation: cloned,
		Count:      n,
	})
}

// StartStats publishes one GatewayStats event immediately and then every
// interval on the event topic template (event "stats"). Calling StartStats
// again does not start a second publisher. The loop keeps publishing across
// MQTT reconnects on the shared client and stops only when StopStats is called.
func (g *Gateway) StartStats(interval time.Duration) {
	if interval <= 0 {
		interval = 30 * time.Second
	}

	g.statsOnce.Do(func() {
		g.statsMu.Lock()
		g.statsStop = make(chan struct{})
		g.statsDone = make(chan struct{})
		g.statsMu.Unlock()

		log.WithFields(log.Fields{
			"gateway_id": g.gatewayID,
			"interval":   interval,
			"topic":      g.getEventTopic("stats"),
		}).Info("simulator: starting gateway stats publisher")

		go g.statsLoop(interval)
	})
}

// StopStats stops the stats publisher and waits for it to exit.
// It is safe to call when the publisher was never started, and safe to call twice.
func (g *Gateway) StopStats() {
	g.statsMu.Lock()
	stop := g.statsStop
	done := g.statsDone
	g.statsMu.Unlock()
	if stop == nil || done == nil {
		return
	}

	g.statsStopOnce.Do(func() {
		close(stop)
	})
	<-done
}

func (g *Gateway) statsLoop(interval time.Duration) {
	defer close(g.statsDone)

	g.publishStats()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-g.statsStop:
			return
		case <-ticker.C:
			g.publishStats()
		}
	}
}

func (g *Gateway) publishStats() {
	snapshot := g.swapStats()
	stats := snapshot.toProto(g.gatewayID.String())

	b, err := proto.Marshal(stats)
	if err != nil {
		g.mergeStats(snapshot)
		log.WithError(err).WithField("gateway_id", g.gatewayID).Error("simulator: marshal gateway stats error")
		return
	}

	topic := g.getEventTopic("stats")
	if err := g.publishEvent(topic, b); err != nil {
		g.mergeStats(snapshot)
		log.WithError(err).WithFields(log.Fields{
			"gateway_id": g.gatewayID,
			"topic":      topic,
		}).Error("simulator: publish gateway stats error")
		return
	}

	log.WithFields(log.Fields{
		"gateway_id":             stats.GatewayId,
		"topic":                  topic,
		"rx_packets_received":    stats.RxPacketsReceived,
		"rx_packets_received_ok": stats.RxPacketsReceivedOk,
		"tx_packets_received":    stats.TxPacketsReceived,
		"tx_packets_emitted":     stats.TxPacketsEmitted,
	}).Info("simulator: publish gateway stats")
}

// publishEvent publishes at QoS 0 and does not retain the message.
// Retained stats would replay stale counters to ChirpStack on reconnect.
func (g *Gateway) publishEvent(topic string, payload []byte) error {
	const qos byte = 0
	const retained = false

	if g.publishEventFn != nil {
		return g.publishEventFn(topic, qos, retained, payload)
	}
	if g.mqtt == nil {
		return errors.New("mqtt client is nil")
	}

	token := g.mqtt.Publish(topic, qos, retained, payload)
	if token.Wait() && token.Error() != nil {
		return errors.Wrap(token.Error(), "mqtt publish error")
	}
	return nil
}

func (g *Gateway) recordUplink(txInfo *gw.UplinkTxInfo) {
	g.counterMu.Lock()
	defer g.counterMu.Unlock()
	g.ensureCountersLocked()

	g.counters.rxReceived++
	g.counters.rxReceivedOK++
	if txInfo == nil {
		return
	}
	g.counters.rxPerFreq[txInfo.Frequency]++
	addModulationCount(&g.counters.rxPerMod, txInfo.Modulation, 1)
}

func (g *Gateway) recordDownlink(frame gw.DownlinkFrame, acks []*gw.DownlinkTxAckItem) {
	g.counterMu.Lock()
	defer g.counterMu.Unlock()
	g.ensureCountersLocked()

	for i, item := range frame.Items {
		g.counters.txReceived++

		status := gw.TxAckStatus_IGNORED
		if i < len(acks) && acks[i] != nil {
			status = acks[i].Status
		}
		g.counters.txPerStatus[status.String()]++

		if status != gw.TxAckStatus_OK {
			continue
		}

		g.counters.txEmitted++
		if item.GetTxInfo() == nil {
			continue
		}
		g.counters.txPerFreq[item.TxInfo.Frequency]++
		addModulationCount(&g.counters.txPerMod, item.TxInfo.Modulation, 1)
	}
}

func (g *Gateway) ensureCountersLocked() {
	if g.counters.rxPerFreq == nil {
		g.counters = newIntervalStats()
	}
}

func (g *Gateway) swapStats() intervalStats {
	g.counterMu.Lock()
	defer g.counterMu.Unlock()
	g.ensureCountersLocked()
	snap := g.counters
	g.counters = newIntervalStats()
	return snap
}

func (g *Gateway) mergeStats(snap intervalStats) {
	g.counterMu.Lock()
	defer g.counterMu.Unlock()
	g.ensureCountersLocked()
	g.counters.add(snap)
}

func (s intervalStats) toProto(gatewayID string) *gw.GatewayStats {
	return &gw.GatewayStats{
		GatewayId:              gatewayID,
		Time:                   timestamppb.New(time.Now().UTC()),
		RxPacketsReceived:      s.rxReceived,
		RxPacketsReceivedOk:    s.rxReceivedOK,
		TxPacketsReceived:      s.txReceived,
		TxPacketsEmitted:       s.txEmitted,
		RxPacketsPerFrequency:  copyUint32Map(s.rxPerFreq),
		TxPacketsPerFrequency:  copyUint32Map(s.txPerFreq),
		TxPacketsPerStatus:     copyStringUintMap(s.txPerStatus),
		RxPacketsPerModulation: s.rxPerMod,
		TxPacketsPerModulation: s.txPerMod,
	}
}

func copyUint32Map(in map[uint32]uint32) map[uint32]uint32 {
	if len(in) == 0 {
		return nil
	}
	out := make(map[uint32]uint32, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func copyStringUintMap(in map[string]uint32) map[string]uint32 {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]uint32, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
