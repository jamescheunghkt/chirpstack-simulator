package simulator

import (
	"fmt"
	"strings"
	"time"

	"google.golang.org/protobuf/encoding/protowire"

	"github.com/chirpstack/chirpstack/api/go/v4/api"
	"github.com/chirpstack/chirpstack/api/go/v4/common"
)

// ChirpStack marks a gateway online when last-seen is within twice this
// interval. Field 8 is stats_interval on the current Gateway API. The
// generated client in this repository predates that field, so it is appended
// to the protobuf message.
const gatewayStatsIntervalField protowire.Number = 8

// deviceProfileRegion resolves a ChirpStack common.Region name.
// The empty value defaults to AS923. Names are matched against the v4 API
// enum (AS923, AS923_2, AS923_3, AS923_4, ...). This is the device-profile
// region, not the MQTT region id used in gateway topics.
func deviceProfileRegion(name string) (common.Region, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		name = "AS923"
	}
	if v, ok := common.Region_value[name]; ok {
		return common.Region(v), nil
	}
	upper := strings.ToUpper(name)
	if v, ok := common.Region_value[upper]; ok {
		return common.Region(v), nil
	}
	return 0, fmt.Errorf("unknown device-profile region %q", name)
}

// newDeviceProfile builds the device profile created for a simulation run.
// LoRaWAN version and regional-parameter revision stay on the previous values.
// The API has no separate region-configuration id; that id is the MQTT topic prefix.
// uplinkIntervalSecs is the expected uplink period. ChirpStack treats a device
// as active while its last uplink is within 1.5 times this value.
func newDeviceProfile(tenantID, name string, region common.Region, uplinkIntervalSecs uint32) *api.DeviceProfile {
	return &api.DeviceProfile{
		Name:              name,
		TenantId:          tenantID,
		MacVersion:        common.MacVersion_LORAWAN_1_0_3,
		RegParamsRevision: common.RegParamsRevision_B,
		SupportsOtaa:      true,
		Region:            region,
		AdrAlgorithmId:    "default",
		UplinkInterval:    uplinkIntervalSecs,
	}
}

// durationSeconds converts a configured interval to whole seconds.
// A missing or sub-second value uses fallback.
func durationSeconds(d time.Duration, fallback uint32) uint32 {
	if d < time.Second {
		return fallback
	}
	secs := d / time.Second
	if secs > time.Duration(^uint32(0)) {
		return ^uint32(0)
	}
	return uint32(secs)
}

// setGatewayStatsInterval records the expected gateway stats period on a
// create or update request. ChirpStack stores this as stats_interval_secs and
// shows the gateway online while last-seen is newer than twice that period.
func setGatewayStatsInterval(gw *api.Gateway, seconds uint32) {
	if gw == nil || seconds == 0 {
		return
	}
	raw := gw.ProtoReflect()
	b := protowire.AppendTag(nil, gatewayStatsIntervalField, protowire.VarintType)
	b = protowire.AppendVarint(b, uint64(seconds))
	raw.SetUnknown(append(raw.GetUnknown(), b...))
}
