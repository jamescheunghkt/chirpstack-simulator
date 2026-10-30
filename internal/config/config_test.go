package config

import (
	"bytes"
	"io/ioutil"
	"strings"
	"testing"
	"time"

	"github.com/spf13/viper"
)

func TestApplyDefaults(t *testing.T) {
	c := loadConfig(t, `
[[simulator]]
[simulator.device]
frequency=923200000

[simulator.gateway]
event_topic_template="as923/gateway/{{ .GatewayID }}/event/{{ .Event }}"
`)
	c.ApplyDefaults()

	sim := c.Simulator[0]
	if sim.Device.Region != "AS923" {
		t.Fatalf("region = %q, want AS923", sim.Device.Region)
	}
	if sim.Device.Frequency != 923200000 {
		t.Fatalf("frequency changed to %d", sim.Device.Frequency)
	}
	if sim.Gateway.StatsEnabled == nil || !*sim.Gateway.StatsEnabled {
		t.Fatal("omitted stats_enabled should default to true")
	}
	if sim.Gateway.StatsInterval != 30*time.Second {
		t.Fatalf("stats interval = %s, want 30s", sim.Gateway.StatsInterval)
	}
	if sim.Device.GroupSize != 100 {
		t.Fatalf("omitted group size = %d, want 100", sim.Device.GroupSize)
	}

	explicit := loadConfig(t, `
[[simulator]]
[simulator.device]
region="AS923_2"
frequency=923200000
group_size=250

[simulator.gateway]
stats_enabled=false
stats_interval="10s"
`)
	explicit.ApplyDefaults()
	got := explicit.Simulator[0]
	if got.Gateway.StatsEnabled == nil || *got.Gateway.StatsEnabled {
		t.Fatal("explicit stats_enabled=false was overridden")
	}
	if got.Device.Region != "AS923_2" {
		t.Fatalf("explicit region overridden to %q", got.Device.Region)
	}
	if got.Device.GroupSize != 250 {
		t.Fatalf("explicit group size overridden to %d", got.Device.GroupSize)
	}
	if got.Gateway.StatsInterval != 10*time.Second {
		t.Fatalf("explicit stats interval overridden to %s", got.Gateway.StatsInterval)
	}
}

func loadConfig(t *testing.T, body string) Config {
	t.Helper()
	v := viper.New()
	v.SetConfigType("toml")
	if err := v.ReadConfig(strings.NewReader(body)); err != nil {
		t.Fatal(err)
	}
	var c Config
	if err := v.Unmarshal(&c); err != nil {
		t.Fatal(err)
	}
	return c
}

func TestRepositorySimulatorConfig(t *testing.T) {
	b, err := ioutil.ReadFile("../../chirpstack-simulator.toml")
	if err != nil {
		t.Fatal(err)
	}

	v := viper.New()
	v.SetConfigType("toml")
	if err := v.ReadConfig(bytes.NewBuffer(b)); err != nil {
		t.Fatal(err)
	}

	var c Config
	if err := v.Unmarshal(&c); err != nil {
		t.Fatal(err)
	}
	c.ApplyDefaults()

	if len(c.Simulator) != 1 {
		t.Fatalf("simulator blocks = %d, want 1", len(c.Simulator))
	}
	sim := c.Simulator[0]
	if sim.Device.Frequency != 923200000 {
		t.Fatalf("frequency = %d, want 923200000", sim.Device.Frequency)
	}
	if sim.Device.Region != "AS923" {
		t.Fatalf("region = %q, want AS923", sim.Device.Region)
	}
	if !strings.HasPrefix(sim.Gateway.EventTopicTemplate, "as923/gateway/") || !strings.Contains(sim.Gateway.EventTopicTemplate, "/event/") {
		t.Fatalf("event topic template = %q", sim.Gateway.EventTopicTemplate)
	}
	if !strings.HasPrefix(sim.Gateway.CommandTopicTemplate, "as923/gateway/") || !strings.Contains(sim.Gateway.CommandTopicTemplate, "/command/") {
		t.Fatalf("command topic template = %q", sim.Gateway.CommandTopicTemplate)
	}
	if sim.Gateway.StatsEnabled == nil || !*sim.Gateway.StatsEnabled {
		t.Fatal("stats_enabled is not true")
	}
	if sim.Gateway.StatsInterval != 30*time.Second {
		t.Fatalf("stats interval = %s, want 30s", sim.Gateway.StatsInterval)
	}
	if sim.Device.GroupSize != 100 {
		t.Fatalf("group size = %d, want 100", sim.Device.GroupSize)
	}
	if sim.Device.Count <= sim.Device.GroupSize {
		t.Fatalf("count = %d, group size = %d; grouped join is not exercised by this config", sim.Device.Count, sim.Device.GroupSize)
	}
}
