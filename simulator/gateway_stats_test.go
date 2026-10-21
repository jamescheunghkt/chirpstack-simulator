package simulator

import (
	"sync"
	"testing"
	"text/template"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"

	"github.com/brocaar/lorawan"
	"github.com/chirpstack/chirpstack/api/go/v4/gw"
)

func testGateway(t *testing.T, id lorawan.EUI64) *Gateway {
	t.Helper()
	event, err := template.New("event").Parse("as923/gateway/{{ .GatewayID }}/event/{{ .Event }}")
	if err != nil {
		t.Fatal(err)
	}
	return &Gateway{
		gatewayID:          id,
		eventTopicTemplate: event,
		counters:           newIntervalStats(),
	}
}

func loraModulation(sf uint32) *gw.Modulation {
	return &gw.Modulation{
		Parameters: &gw.Modulation_Lora{
			Lora: &gw.LoraModulationInfo{
				Bandwidth:       125000,
				SpreadingFactor: sf,
				CodeRate:        gw.CodeRate_CR_4_5,
			},
		},
	}
}

func TestGatewayStatsPayload(t *testing.T) {
	id := lorawan.EUI64{0x2e, 0xac, 0x91, 0xaf, 0xf2, 0x5d, 0x70, 0xfe}
	g := testGateway(t, id)

	g.recordUplink(&gw.UplinkTxInfo{Frequency: 923200000, Modulation: loraModulation(7)})
	g.recordUplink(&gw.UplinkTxInfo{Frequency: 923200000, Modulation: loraModulation(7)})
	g.recordUplink(&gw.UplinkTxInfo{Frequency: 923400000, Modulation: loraModulation(8)})

	down := gw.DownlinkFrame{
		Items: []*gw.DownlinkFrameItem{
			{TxInfo: &gw.DownlinkTxInfo{Frequency: 923200000, Modulation: loraModulation(7)}},
			{TxInfo: &gw.DownlinkTxInfo{Frequency: 923200000, Modulation: loraModulation(7)}},
		},
	}
	g.recordDownlink(down, []*gw.DownlinkTxAckItem{
		{Status: gw.TxAckStatus_OK},
		{Status: gw.TxAckStatus_COLLISION_PACKET},
	})

	var published []publishedEvent
	g.publishEventFn = func(topic string, qos byte, retained bool, payload []byte) error {
		if qos != 0 {
			t.Errorf("qos = %d, want 0", qos)
		}
		if retained {
			t.Error("stats message was retained")
		}
		published = append(published, publishedEvent{topic: topic, payload: append([]byte(nil), payload...)})
		return nil
	}

	g.publishStats()
	if len(published) != 1 {
		t.Fatalf("publishes = %d, want 1", len(published))
	}

	wantTopic := "as923/gateway/" + id.String() + "/event/stats"
	if published[0].topic != wantTopic {
		t.Fatalf("topic = %q, want %q", published[0].topic, wantTopic)
	}

	var stats gw.GatewayStats
	if err := proto.Unmarshal(published[0].payload, &stats); err != nil {
		t.Fatalf("unmarshal gateway stats: %v", err)
	}
	if stats.GatewayId != id.String() {
		t.Fatalf("gateway id = %q, want %q", stats.GatewayId, id.String())
	}
	if len(stats.GatewayIdLegacy) != 0 {
		t.Fatalf("gateway_id_legacy = %x, want empty", stats.GatewayIdLegacy)
	}
	if stats.Time == nil || stats.Time.AsTime().IsZero() {
		t.Fatal("stats time is missing")
	}
	if time.Since(stats.Time.AsTime()) > time.Minute || time.Until(stats.Time.AsTime()) > time.Minute {
		t.Fatalf("stats time = %s", stats.Time.AsTime())
	}
	if stats.RxPacketsReceived != 3 || stats.RxPacketsReceivedOk != 3 {
		t.Fatalf("rx counters = %d/%d, want 3/3", stats.RxPacketsReceived, stats.RxPacketsReceivedOk)
	}
	if stats.RxPacketsPerFrequency[923200000] != 2 || stats.RxPacketsPerFrequency[923400000] != 1 {
		t.Fatalf("rx per frequency = %#v", stats.RxPacketsPerFrequency)
	}
	if stats.TxPacketsReceived != 2 || stats.TxPacketsEmitted != 1 {
		t.Fatalf("tx counters = received %d emitted %d, want 2 and 1", stats.TxPacketsReceived, stats.TxPacketsEmitted)
	}
	if stats.TxPacketsPerFrequency[923200000] != 1 {
		t.Fatalf("tx per frequency = %#v, want only the emitted downlink", stats.TxPacketsPerFrequency)
	}
	if stats.TxPacketsPerStatus["OK"] != 1 || stats.TxPacketsPerStatus["COLLISION_PACKET"] != 1 {
		t.Fatalf("tx per status = %#v", stats.TxPacketsPerStatus)
	}
	if len(stats.RxPacketsPerModulation) != 2 {
		t.Fatalf("rx modulation entries = %d, want 2", len(stats.RxPacketsPerModulation))
	}

	g.publishStats()
	if len(published) != 2 {
		t.Fatalf("publishes = %d, want 2", len(published))
	}
	var empty gw.GatewayStats
	if err := proto.Unmarshal(published[1].payload, &empty); err != nil {
		t.Fatal(err)
	}
	if empty.RxPacketsReceived != 0 || empty.TxPacketsReceived != 0 || empty.TxPacketsEmitted != 0 {
		t.Fatalf("counters were not reset: %+v", empty)
	}
	if empty.GatewayId != id.String() {
		t.Fatalf("reset stats gateway id = %q", empty.GatewayId)
	}
}

func TestGatewayStatsPublishFailureKeepsCounters(t *testing.T) {
	id := lorawan.EUI64{1, 2, 3, 4, 5, 6, 7, 8}
	g := testGateway(t, id)
	g.recordUplink(&gw.UplinkTxInfo{Frequency: 923200000, Modulation: loraModulation(7)})

	g.publishEventFn = func(topic string, qos byte, retained bool, payload []byte) error {
		return errors.New("connection lost")
	}
	g.publishStats()

	var payload []byte
	g.publishEventFn = func(topic string, qos byte, retained bool, pl []byte) error {
		payload = append([]byte(nil), pl...)
		return nil
	}
	g.publishStats()

	var stats gw.GatewayStats
	if err := proto.Unmarshal(payload, &stats); err != nil {
		t.Fatal(err)
	}
	if stats.RxPacketsReceived != 1 || stats.RxPacketsReceivedOk != 1 {
		t.Fatalf("rx after failed publish = %d, want 1", stats.RxPacketsReceived)
	}

	g.publishStats()
	var next gw.GatewayStats
	if err := proto.Unmarshal(payload, &next); err != nil {
		t.Fatal(err)
	}
	if next.RxPacketsReceived != 0 {
		t.Fatalf("rx after successful publish = %d, want 0", next.RxPacketsReceived)
	}
}

func TestGatewayStatsSinglePublisherShutdown(t *testing.T) {
	id := lorawan.EUI64{9, 8, 7, 6, 5, 4, 3, 2}
	g := testGateway(t, id)

	var mu sync.Mutex
	var count int
	g.publishEventFn = func(topic string, qos byte, retained bool, payload []byte) error {
		mu.Lock()
		count++
		mu.Unlock()
		return nil
	}

	g.StartStats(time.Hour)
	g.StartStats(time.Millisecond)

	deadline := time.Now().Add(time.Second)
	for {
		mu.Lock()
		n := count
		mu.Unlock()
		if n >= 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("stats publisher did not publish")
		}
		time.Sleep(5 * time.Millisecond)
	}
	time.Sleep(30 * time.Millisecond)

	mu.Lock()
	if count != 1 {
		t.Fatalf("publishes = %d, want 1 from a single publisher", count)
	}
	mu.Unlock()

	g.StopStats()
	g.StopStats()

	time.Sleep(30 * time.Millisecond)
	mu.Lock()
	if count != 1 {
		t.Fatalf("publishes after stop = %d, want 1", count)
	}
	mu.Unlock()
}

func TestGatewayStatsPeriodicAndReconnect(t *testing.T) {
	id := lorawan.EUI64{1, 1, 1, 1, 1, 1, 1, 1}
	g := testGateway(t, id)

	var mu sync.Mutex
	var calls int
	var successes int
	g.publishEventFn = func(topic string, qos byte, retained bool, payload []byte) error {
		mu.Lock()
		calls++
		n := calls
		mu.Unlock()
		if n <= 2 {
			return errors.New("connection lost")
		}
		mu.Lock()
		successes++
		mu.Unlock()
		return nil
	}

	g.StartStats(20 * time.Millisecond)
	g.StartStats(20 * time.Millisecond)

	deadline := time.Now().Add(time.Second)
	for {
		mu.Lock()
		ok := successes >= 2
		mu.Unlock()
		if ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("publisher stopped after publish failures")
		}
		time.Sleep(5 * time.Millisecond)
	}

	mu.Lock()
	before := calls
	mu.Unlock()
	time.Sleep(70 * time.Millisecond)
	mu.Lock()
	delta := calls - before
	mu.Unlock()
	if delta < 2 || delta > 6 {
		t.Fatalf("calls during 70ms after reconnect = %d, want one publisher", delta)
	}

	g.StopStats()
	mu.Lock()
	stopped := calls
	mu.Unlock()
	time.Sleep(60 * time.Millisecond)
	mu.Lock()
	if calls != stopped {
		t.Fatalf("calls after stop grew from %d to %d", stopped, calls)
	}
	mu.Unlock()
}

type publishedEvent struct {
	topic   string
	payload []byte
}
