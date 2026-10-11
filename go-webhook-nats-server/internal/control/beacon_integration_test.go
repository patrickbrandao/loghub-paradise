package control

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/patrickbrandao/loghub-paradise/go-webhook-nats-server/internal/jlhcheck"
	"github.com/patrickbrandao/loghub-paradise/go-webhook-nats-server/internal/sink"
)

// The beacon against a real server, wired the way the app wires it: the NATS
// sink's connect hook kicks it, and it publishes by core NATS on the sink's own
// connection. Skips without WHS_TEST_NATS_URL:
//
//	go run github.com/nats-io/nats-server/v2@v2.14.6 -js -sd "$(mktemp -d)" -p 14222 &
//	WHS_TEST_NATS_URL=nats://127.0.0.1:14222 go test -race ./internal/control/

func TestBeaconOverARealServer(t *testing.T) {
	url := os.Getenv("WHS_TEST_NATS_URL")
	if url == "" {
		t.Skip("set WHS_TEST_NATS_URL to run the beacon against a real server")
	}
	ns := fmt.Sprintf("whs_test_ctl_%d", time.Now().UnixNano())

	watch, err := nats.Connect(url, nats.Timeout(5*time.Second))
	if err != nil {
		t.Fatalf("WHS_TEST_NATS_URL is not reachable: %v", err)
	}
	defer watch.Close()
	sub, err := watch.SubscribeSync(ns + ".beacon")
	if err != nil {
		t.Fatal(err)
	}
	if err := watch.Flush(); err != nil {
		t.Fatal(err)
	}

	m := metrics()
	// core mode: no stream to create, and the beacon never touches JetStream
	// in either mode (SPEC §8.7.10).
	s, err := sink.NewNATS(sink.NATSConfig{
		URLs: []string{url}, Mode: sink.NATSModeCore, Workspace: testIdentity.Space, CtlNamespace: ns,
		ClientName: "whs-test-beacon",
	}, discard(), m)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	cfg := testConfig()
	cfg.Namespace = ns
	b := New(s, cfg, discard(), m)
	s.OnConnect(b.Kick)

	// The boot order of SPEC §10.1: the sink connects (step 3), the beacon
	// starts later (step 4a) and still sends the connection's beacon.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := s.Health(ctx); err != nil {
		t.Fatalf("sink Health: %v", err)
	}
	b.Start(ctx)
	defer b.Stop()

	msg, err := sub.NextMsg(5 * time.Second)
	if err != nil {
		t.Fatalf("no beacon on %s.beacon: %v", ns, err)
	}
	if err := jlhcheck.ValidateEnvelopeSchema(msg.Data); err != nil {
		t.Fatalf("%v\n%s", err, msg.Data)
	}
	var e struct {
		Type    string `json:"type"`
		Payload struct {
			Address string `json:"address"`
		} `json:"payload"`
	}
	if err := json.Unmarshal(msg.Data, &e); err != nil || e.Type != "beacon" || e.Payload.Address == "" {
		t.Fatalf("beacon = %s (%v)", msg.Data, err)
	}
	if m.BeaconsOK.Load() != 1 {
		t.Errorf(`beacons_total{result="ok"} = %d`, m.BeaconsOK.Load())
	}
}
