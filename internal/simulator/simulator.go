package simulator

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	mrand "math/rand"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/gofrs/uuid"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"

	"github.com/brocaar/chirpstack-simulator/internal/as"
	"github.com/brocaar/chirpstack-simulator/internal/config"
	"github.com/brocaar/chirpstack-simulator/internal/ns"
	"github.com/brocaar/chirpstack-simulator/simulator"
	"github.com/brocaar/lorawan"
	"github.com/chirpstack/chirpstack/api/go/v4/api"
	"github.com/chirpstack/chirpstack/api/go/v4/common"
	"github.com/chirpstack/chirpstack/api/go/v4/gw"
)

// Start starts the simulator.
func Start(ctx context.Context, wg *sync.WaitGroup, c config.Config) error {
	c.ApplyDefaults()

	for i, c := range c.Simulator {
		log.WithFields(log.Fields{
			"i": i,
		}).Info("simulator: starting simulation")

		wg.Add(1)

		pl, err := hex.DecodeString(c.Device.Payload)
		if err != nil {
			return errors.Wrap(err, "decode payload error")
		}

		sim := simulation{
			ctx:                  ctx,
			wg:                   wg,
			tenantID:             c.TenantID,
			deviceCount:          c.Device.Count,
			activationTime:       c.ActivationTime,
			uplinkInterval:       c.Device.UplinkInterval,
			fPort:                c.Device.FPort,
			payload:              pl,
			frequency:            c.Device.Frequency,
			bandwidth:            c.Device.Bandwidth,
			spreadingFactor:      c.Device.SpreadingFactor,
			deviceRegion:         c.Device.Region,
			duration:             c.Duration,
			gatewayMinCount:      c.Gateway.MinCount,
			gatewayMaxCount:      c.Gateway.MaxCount,
			deviceAppKeys:        make(map[lorawan.EUI64]lorawan.AES128Key),
			eventTopicTemplate:   c.Gateway.EventTopicTemplate,
			commandTopicTemplate: c.Gateway.CommandTopicTemplate,
			statsEnabled:         c.Gateway.StatsEnabled != nil && *c.Gateway.StatsEnabled,
			statsInterval:        c.Gateway.StatsInterval,
		}

		go sim.start()
	}

	return nil
}

type simulation struct {
	ctx             context.Context
	wg              *sync.WaitGroup
	tenantID        string
	deviceCount     int
	gatewayMinCount int
	gatewayMaxCount int
	duration        time.Duration

	fPort           uint8
	payload         []byte
	activationTime  time.Duration
	uplinkInterval  time.Duration
	frequency       int
	bandwidth       int
	spreadingFactor int
	deviceRegion    string

	statsEnabled  bool
	statsInterval time.Duration

	tenant               *api.Tenant
	deviceProfileID      uuid.UUID
	region               common.Region
	applicationID        string
	gatewayIDs           []lorawan.EUI64
	deviceAppKeysMutex   sync.Mutex
	deviceAppKeys        map[lorawan.EUI64]lorawan.AES128Key
	eventTopicTemplate   string
	commandTopicTemplate string
}

func (s *simulation) start() {
	region, err := deviceProfileRegion(s.deviceRegion)
	if err != nil {
		log.WithError(err).WithFields(log.Fields{
			"region":    s.deviceRegion,
			"frequency": s.frequency,
		}).Error("simulator: invalid device-profile region")
		s.wg.Done()
		return
	}
	s.region = region

	log.WithFields(log.Fields{
		"region":                 region.String(),
		"frequency_hz":           s.frequency,
		"event_topic_template":   s.eventTopicTemplate,
		"command_topic_template": s.commandTopicTemplate,
		"gateway_stats_enabled":  s.statsEnabled,
		"gateway_stats_interval": s.statsInterval,
	}).Info("simulator: effective configuration")

	if err := s.init(); err != nil {
		log.WithError(err).Error("simulator: init simulation error")
	}

	if err := s.runSimulation(); err != nil {
		log.WithError(err).Error("simulator: simulation error")
	}

	log.Info("simulator: simulation completed")

	if err := s.tearDown(); err != nil {
		log.WithError(err).Error("simulator: tear-down simulation error")
	}

	s.wg.Done()

	log.Info("simulation: tear-down completed")
}

func (s *simulation) init() error {
	log.Info("simulation: setting up")

	if err := s.setupTenant(); err != nil {
		return err
	}

	if err := s.setupGateways(); err != nil {
		return err
	}

	if err := s.setupDeviceProfile(); err != nil {
		return err
	}

	if err := s.setupApplication(); err != nil {
		return err
	}

	if err := s.setupDevices(); err != nil {
		return err
	}

	if err := s.setupApplicationIntegration(); err != nil {
		return err
	}

	return nil
}

func (s *simulation) tearDown() error {
	log.Info("simulation: cleaning up")

	if err := s.tearDownApplicationIntegration(); err != nil {
		return err
	}

	if err := s.tearDownDevices(); err != nil {
		return err
	}

	if err := s.tearDownApplication(); err != nil {
		return err
	}

	if err := s.tearDownDeviceProfile(); err != nil {
		return err
	}

	if err := s.tearDownGateways(); err != nil {
		return err
	}

	return nil
}

func (s *simulation) runSimulation() error {
	var gateways []*simulator.Gateway
	defer func() {
		for _, gw := range gateways {
			gw.StopStats()
		}
	}()

	for _, gatewayID := range s.gatewayIDs {
		gw, err := simulator.NewGateway(
			simulator.WithGatewayID(gatewayID),
			simulator.WithMQTTClient(ns.Client()),
			simulator.WithEventTopicTemplate(s.eventTopicTemplate),
			simulator.WithCommandTopicTemplate(s.commandTopicTemplate),
		)
		if err != nil {
			return errors.Wrap(err, "new gateway error")
		}
		gateways = append(gateways, gw)

		// Stats are independent of device joins so a gateway can be marked
		// seen while OTAA is still being debugged.
		if s.statsEnabled {
			gw.StartStats(s.statsInterval)
		}
	}

	if !s.statsEnabled {
		log.WithFields(log.Fields{
			"event_topic_template": s.eventTopicTemplate,
		}).Info("simulator: gateway stats disabled")
	}

	var devices []*simulator.Device
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(s.ctx)
	if s.duration != 0 {
		ctx, cancel = context.WithTimeout(ctx, s.duration)
	}
	defer cancel()

	for devEUI, appKey := range s.deviceAppKeys {
		devGateways := make(map[int]*simulator.Gateway)
		devNumGateways := s.gatewayMinCount + mrand.Intn(s.gatewayMaxCount-s.gatewayMinCount+1)

		for len(devGateways) < devNumGateways {
			// pick random gateway index
			n := mrand.Intn(len(gateways))
			devGateways[n] = gateways[n]
		}

		var gws []*simulator.Gateway
		for k := range devGateways {
			gws = append(gws, devGateways[k])
		}

		d, err := simulator.NewDevice(ctx, &wg,
			simulator.WithDevEUI(devEUI),
			simulator.WithAppKey(appKey),
			simulator.WithUplinkInterval(s.uplinkInterval),
			simulator.WithOTAADelay(time.Duration(mrand.Int63n(int64(s.activationTime)))),
			simulator.WithUplinkPayload(false, s.fPort, s.payload),
			simulator.WithGateways(gws),
			simulator.WithUplinkTXInfo(gw.UplinkTxInfo{
				Frequency: uint32(s.frequency),
				Modulation: &gw.Modulation{
					Parameters: &gw.Modulation_Lora{
						Lora: &gw.LoraModulationInfo{
							Bandwidth:       uint32(s.bandwidth),
							SpreadingFactor: uint32(s.spreadingFactor),
							CodeRate:        gw.CodeRate_CR_4_5,
						},
					},
				},
			}),
		)
		if err != nil {
			return errors.Wrap(err, "new device error")
		}

		devices = append(devices, d)
	}

	go func() {
		sigChan := make(chan os.Signal)
		signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

		select {
		case sig := <-sigChan:
			log.WithField("signal", sig).Info("signal received, stopping simulators")
			cancel()
		case <-ctx.Done():
		}
	}()

	wg.Wait()

	return nil
}

func (s *simulation) setupTenant() error {
	log.WithFields(log.Fields{
		"tenant_id": s.tenantID,
	}).Info("simulator: retrieving tenant")
	t, err := as.Tenant().Get(context.Background(), &api.GetTenantRequest{
		Id: s.tenantID,
	})
	if err != nil {
		return errors.Wrap(err, "get tenant error")
	}
	s.tenant = t.GetTenant()

	return nil
}

func (s *simulation) setupGateways() error {
	log.Info("simulator: creating gateways")

	for i := 0; i < s.gatewayMaxCount; i++ {
		var gatewayID lorawan.EUI64
		if _, err := rand.Read(gatewayID[:]); err != nil {
			return errors.Wrap(err, "read random bytes error")
		}

		statsIntervalSecs := durationSeconds(s.statsInterval, 30)
		gateway := &api.Gateway{
			GatewayId:   gatewayID.String(),
			Name:        gatewayID.String(),
			Description: gatewayID.String(),
			TenantId:    s.tenant.GetId(),
			Location:    &common.Location{},
		}
		setGatewayStatsInterval(gateway, statsIntervalSecs)

		_, err := as.Gateway().Create(context.Background(), &api.CreateGatewayRequest{
			Gateway: gateway,
		})
		if err != nil {
			return errors.Wrap(err, "create gateway error")
		}

		log.WithFields(log.Fields{
			"gateway_id":          gatewayID,
			"stats_interval_secs": statsIntervalSecs,
		}).Info("simulator: created gateway")

		s.gatewayIDs = append(s.gatewayIDs, gatewayID)
	}

	return nil
}

func (s *simulation) tearDownGateways() error {
	log.Info("simulator: tear-down gateways")

	for _, gatewayID := range s.gatewayIDs {
		_, err := as.Gateway().Delete(context.Background(), &api.DeleteGatewayRequest{
			GatewayId: gatewayID.String(),
		})
		if err != nil {
			return errors.Wrap(err, "delete gateway error")
		}
	}

	return nil
}

func (s *simulation) setupDeviceProfile() error {
	region, err := deviceProfileRegion(s.deviceRegion)
	if err != nil {
		return err
	}
	s.region = region

	log.WithFields(log.Fields{
		"region":       region.String(),
		"frequency_hz": s.frequency,
	}).Info("simulator: creating device-profile")

	dpName, _ := uuid.NewV4()
	// Each run creates a new profile. A profile left behind by an earlier
	// process is not updated or deleted here.
	uplinkIntervalSecs := durationSeconds(s.uplinkInterval, 300)
	profile := newDeviceProfile(s.tenant.GetId(), dpName.String(), region, uplinkIntervalSecs)

	resp, err := as.DeviceProfile().Create(context.Background(), &api.CreateDeviceProfileRequest{
		DeviceProfile: profile,
	})
	if err != nil {
		return errors.Wrap(err, "create device-profile error")
	}

	dpID, err := uuid.FromString(resp.Id)
	if err != nil {
		return err
	}
	s.deviceProfileID = dpID

	log.WithFields(log.Fields{
		"device_profile_id":    resp.Id,
		"region":               profile.Region.String(),
		"mac_version":          profile.MacVersion.String(),
		"reg_params_revision":  profile.RegParamsRevision.String(),
		"frequency_hz":         s.frequency,
		"uplink_interval_secs": profile.UplinkInterval,
		"event_topic_template": s.eventTopicTemplate,
	}).Info("simulator: created device-profile")

	return nil
}

func (s *simulation) tearDownDeviceProfile() error {
	log.Info("simulator: tear-down device-profile")

	_, err := as.DeviceProfile().Delete(context.Background(), &api.DeleteDeviceProfileRequest{
		Id: s.deviceProfileID.String(),
	})
	if err != nil {
		return errors.Wrap(err, "delete device-profile error")
	}

	return nil
}

func (s *simulation) setupApplication() error {
	log.Info("simulator: init application")

	appName, err := uuid.NewV4()
	if err != nil {
		return err
	}

	createAppResp, err := as.Application().Create(context.Background(), &api.CreateApplicationRequest{
		Application: &api.Application{
			Name:        appName.String(),
			Description: appName.String(),
			TenantId:    s.tenant.GetId(),
		},
	})
	if err != nil {
		return errors.Wrap(err, "create applicaiton error")
	}

	s.applicationID = createAppResp.Id
	return nil
}

func (s *simulation) tearDownApplication() error {
	log.Info("simulator: tear-down application")

	_, err := as.Application().Delete(context.Background(), &api.DeleteApplicationRequest{
		Id: s.applicationID,
	})
	if err != nil {
		return errors.Wrap(err, "delete application error")
	}
	return nil
}

func (s *simulation) setupDevices() error {
	log.Info("simulator: init devices")

	var wg sync.WaitGroup

	for i := 0; i < s.deviceCount; i++ {
		wg.Add(1)

		go func() {
			var devEUI lorawan.EUI64
			var appKey lorawan.AES128Key

			if _, err := rand.Read(devEUI[:]); err != nil {
				log.Fatal(err)
			}
			if _, err := rand.Read(appKey[:]); err != nil {
				log.Fatal(err)
			}

			_, err := as.Device().Create(context.Background(), &api.CreateDeviceRequest{
				Device: &api.Device{
					DevEui:          devEUI.String(),
					Name:            devEUI.String(),
					Description:     devEUI.String(),
					ApplicationId:   s.applicationID,
					DeviceProfileId: s.deviceProfileID.String(),
				},
			})
			if err != nil {
				log.Fatalf("create device error, error: %s", err)
			}

			_, err = as.Device().CreateKeys(context.Background(), &api.CreateDeviceKeysRequest{
				DeviceKeys: &api.DeviceKeys{
					DevEui: devEUI.String(),

					// yes, this is correct for LoRaWAN 1.0.x!
					// see the API documentation
					NwkKey: appKey.String(),
				},
			})
			if err != nil {
				log.Fatalf("create device keys error, error: %s", err)
			}

			s.deviceAppKeysMutex.Lock()
			s.deviceAppKeys[devEUI] = appKey
			s.deviceAppKeysMutex.Unlock()
			wg.Done()
		}()

	}

	wg.Wait()

	return nil
}

func (s *simulation) tearDownDevices() error {
	log.Info("simulator: tear-down devices")

	for k := range s.deviceAppKeys {
		_, err := as.Device().Delete(context.Background(), &api.DeleteDeviceRequest{
			DevEui: k.String(),
		})
		if err != nil {
			return errors.Wrap(err, "delete device error")
		}
	}

	return nil
}

func (s *simulation) setupApplicationIntegration() error {
	log.Info("simulator: setting up application integration")

	token := as.MQTTClient().Subscribe(fmt.Sprintf("application/%s/device/+/event/up", s.applicationID), 0, func(client mqtt.Client, msg mqtt.Message) {
		applicationUplinkCounter().Inc()
	})
	token.Wait()
	if token.Error() != nil {
		return errors.Wrap(token.Error(), "subscribe application integration error")
	}

	return nil
}

func (s *simulation) tearDownApplicationIntegration() error {
	log.Info("simulator: tear-down application integration")

	token := as.MQTTClient().Unsubscribe(fmt.Sprintf("application/%s/device/+/event/up", s.applicationID))
	token.Wait()
	if token.Error() != nil {
		return errors.Wrap(token.Error(), "unsubscribe application integration error")
	}

	return nil
}
