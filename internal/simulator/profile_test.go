package simulator

import (
	"testing"
	"time"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	"github.com/chirpstack/chirpstack/api/go/v4/api"
	"github.com/chirpstack/chirpstack/api/go/v4/common"
)

func TestDeviceProfileRegionAS923(t *testing.T) {
	region, err := deviceProfileRegion("AS923")
	if err != nil {
		t.Fatal(err)
	}
	if region != common.Region_AS923 {
		t.Fatalf("region = %s (%d), want AS923 (%d)", region, region, common.Region_AS923)
	}

	fromEmpty, err := deviceProfileRegion("")
	if err != nil {
		t.Fatal(err)
	}
	if fromEmpty != common.Region_AS923 {
		t.Fatalf("empty region = %s, want AS923", fromEmpty)
	}

	lower, err := deviceProfileRegion("as923")
	if err != nil {
		t.Fatal(err)
	}
	if lower != common.Region_AS923 {
		t.Fatalf("as923 = %s, want AS923", lower)
	}

	offset, err := deviceProfileRegion("AS923_2")
	if err != nil {
		t.Fatal(err)
	}
	if offset != common.Region_AS923_2 {
		t.Fatalf("AS923_2 = %s, want AS923_2", offset)
	}

	if _, err := deviceProfileRegion("not-a-region"); err == nil {
		t.Fatal("expected error for unknown region")
	}

	profile := newDeviceProfile("tenant", "sim-profile", region, 300)
	if profile.Region != common.Region_AS923 {
		t.Fatalf("device profile region = %s, want AS923", profile.Region)
	}
	if profile.MacVersion != common.MacVersion_LORAWAN_1_0_3 {
		t.Fatalf("mac version = %s", profile.MacVersion)
	}
	if profile.RegParamsRevision != common.RegParamsRevision_B {
		t.Fatalf("reg params revision = %s", profile.RegParamsRevision)
	}
	if !profile.SupportsOtaa {
		t.Fatal("expected OTAA support to stay enabled")
	}
	if profile.AdrAlgorithmId != "default" {
		t.Fatalf("adr algorithm = %q", profile.AdrAlgorithmId)
	}
	if profile.TenantId != "tenant" || profile.Name != "sim-profile" {
		t.Fatalf("profile identity changed: %#v", profile)
	}
	if profile.UplinkInterval != 300 {
		t.Fatalf("uplink interval = %d, want 300", profile.UplinkInterval)
	}
}

func TestDurationSeconds(t *testing.T) {
	if got := durationSeconds(5*time.Minute, 300); got != 300 {
		t.Fatalf("5m = %d, want 300", got)
	}
	if got := durationSeconds(30*time.Second, 30); got != 30 {
		t.Fatalf("30s = %d, want 30", got)
	}
	if got := durationSeconds(0, 30); got != 30 {
		t.Fatalf("missing duration = %d, want fallback 30", got)
	}
}

func TestGatewayStatsIntervalWireField(t *testing.T) {
	gw := &api.Gateway{
		GatewayId: "0102030405060708",
		Name:      "gw",
		TenantId:  "tenant",
	}
	setGatewayStatsInterval(gw, 30)

	b, err := proto.Marshal(gw)
	if err != nil {
		t.Fatal(err)
	}
	got, ok := statsIntervalFromWire(b)
	if !ok {
		t.Fatal("stats_interval field 8 was not encoded")
	}
	if got != 30 {
		t.Fatalf("stats_interval = %d, want 30", got)
	}
}

func statsIntervalFromWire(b []byte) (uint32, bool) {
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return 0, false
		}
		b = b[n:]
		if num == gatewayStatsIntervalField && typ == protowire.VarintType {
			v, n := protowire.ConsumeVarint(b)
			if n < 0 {
				return 0, false
			}
			return uint32(v), true
		}
		n = protowire.ConsumeFieldValue(num, typ, b)
		if n < 0 {
			return 0, false
		}
		b = b[n:]
	}
	return 0, false
}
