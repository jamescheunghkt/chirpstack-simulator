package config

import (
	"time"
)

// Version defines the version.
var Version string

// Config defines the configuration.
type Config struct {
	General struct {
		LogLevel int `mapstructure:"log_level"`
	}

	ChirpStack struct {
		API struct {
			APIKey   string `mapstructure:"api_key"`
			Server   string `mapstructure:"server"`
			Insecure bool   `mapstructure:"insecure"`
		} `mapstructure:"api"`

		Integration struct {
			MQTT struct {
				Server   string `mapstructure:"server"`
				Username string `mapstructure:"username"`
				Password string `mapstructure:"password"`
			} `mapstructure:"mqtt"`
		} `mapstructure:"integration"`

		Gateway struct {
			Backend struct {
				MQTT struct {
					Server   string `mapstructure:"server"`
					Username string `mapstructure:"username"`
					Password string `mapstructure:"password"`
				} `mapstructure:"mqtt"`
			} `mapstructure:"backend"`
		} `mapstructure:"gateway"`
	} `mapstructure:"chirpstack"`

	Simulator []struct {
		TenantID       string        `mapstructure:"tenant_id"`
		Duration       time.Duration `mapstructure:"duration"`
		ActivationTime time.Duration `mapstructure:"activation_time"`

		Device struct {
			Count           int           `mapstructure:"count"`
			UplinkInterval  time.Duration `mapstructure:"uplink_interval"`
			FPort           uint8         `mapstructure:"f_port"`
			Payload         string        `mapstructure:"payload"`
			Frequency       int           `mapstructure:"frequency"`
			Bandwidth       int           `mapstructure:"bandwidth"`
			SpreadingFactor int           `mapstructure:"spreading_factor"`
			// Region is the ChirpStack common.Region name stored on the
			// device profile (for example AS923). It is not the MQTT region id.
			Region string `mapstructure:"region"`
		} `mapstructure:"device"`

		Gateway struct {
			MinCount             int           `mapstructure:"min_count"`
			MaxCount             int           `mapstructure:"max_count"`
			EventTopicTemplate   string        `mapstructure:"event_topic_template"`
			CommandTopicTemplate string        `mapstructure:"command_topic_template"`
			StatsInterval        time.Duration `mapstructure:"stats_interval"`
			// StatsEnabled is nil when the key is omitted. ApplyDefaults treats that as enabled.
			StatsEnabled *bool `mapstructure:"stats_enabled"`
		} `mapstructure:"gateway"`
	} `mapstructure:"simulator"`

	Prometheus struct {
		Bind string `mapstructure:"bind"`
	} `mapstructure:"prometheus"`
}

type DeviceConfig struct {
	DevEUI         string        `mapstructure:"dev_eui"`
	AppKey         string        `mapstructure:"app_key"`
	UplinkInterval time.Duration `mapstructure:"uplink_interval"`
	FPort          uint8         `mapstructure:"f_port"`
	Payload        string        `mapstructure:"payload"`
}

// C holds the global configuration.
var C Config

// ApplyDefaults fills simulator settings that are omitted from the config file.
// An omitted device-profile region becomes AS923. Omitted gateway stats stay
// enabled and use a 30s interval. stats_enabled=false disables publishing.
// stats_interval of 0 uses 30s; disable stats with stats_enabled instead.
func (c *Config) ApplyDefaults() {
	for i := range c.Simulator {
		if c.Simulator[i].Device.Region == "" {
			c.Simulator[i].Device.Region = "AS923"
		}
		if c.Simulator[i].Gateway.StatsEnabled == nil {
			enabled := true
			c.Simulator[i].Gateway.StatsEnabled = &enabled
		}
		if c.Simulator[i].Gateway.StatsInterval <= 0 {
			c.Simulator[i].Gateway.StatsInterval = 30 * time.Second
		}
	}
}
