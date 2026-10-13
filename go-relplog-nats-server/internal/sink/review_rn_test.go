package sink

import (
	"context"
	"errors"
	"testing"
	"time"
)

// Findings of the independent review of 2026-10-13 (docs/TESTING.md). Each
// test failed before its fix, or -- the last three -- kills a mutation the
// suite used to survive.

// A server that starts demanding TLS after the boot: nats.go never reports it
// through ReconnectErrCB (that is for a failed TCP dial), so Health answered
// "connection is RECONNECTING", network, until the fuses -- or forever.
func TestNATSReconnectToATLSOnlyServerIsAVerdict(t *testing.T) {
	for _, mode := range []string{NATSModeCore, NATSModeJetStream} {
		t.Run(mode, func(t *testing.T) {
			srv := startNATSFake(t, natsFakeINFO(""))
			n := fakeSink(t, srv, mode, nil, nil)
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			if _, _, err := n.connect(); err != nil {
				t.Fatal(err)
			}
			srv.set(func(f *natsFake) { f.info = natsFakeINFO(`,"tls_required":true`) })
			srv.drop()
			deadline := time.Now().Add(8 * time.Second)
			var err error
			for time.Now().Before(deadline) {
				if err = n.Health(ctx); IsFatal(err) {
					break
				}
				time.Sleep(100 * time.Millisecond)
			}
			if !IsFatal(err) || !errors.Is(err, errNATSServerRequiresTLS) {
				t.Fatalf("Health after reconnecting to a TLS-only server = %v (class %s), want the fatal TLS verdict",
					err, ClassOf(err))
			}
		})
	}
}

// A core flush that STARTS while the client reconnects is resolved by the new
// server's PONG, but the chunk's PUBs went down the old socket: it was
// confirmed although nothing says the server received it.
func TestNATSCoreFlushAcrossAReconnectionIsNotConfirmed(t *testing.T) {
	srv := startNATSFake(t, natsFakeINFO(""))
	n := fakeSink(t, srv, NATSModeCore, nil, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := n.Health(ctx); err != nil {
		t.Fatalf("control: Health = %v", err)
	}
	beforeCoreFlush = func() {
		srv.drop()
		deadline := time.Now().Add(5 * time.Second)
		for n.nc.IsConnected() && time.Now().Before(deadline) {
			time.Sleep(time.Millisecond)
		}
	}
	t.Cleanup(func() { beforeCoreFlush = nil })
	err := n.Write(ctx, batchOf(t, line5424))
	if err == nil || ClassOf(err) != ClassNetwork {
		t.Fatalf("a chunk whose flush crossed a reconnection = %v (class %s), want network", err, ClassOf(err))
	}
	// Control: with the connection steady, the same write is confirmed.
	beforeCoreFlush = nil
	waitConnected(t, n)
	if err := n.Write(ctx, batchOf(t, line5424)); err != nil {
		t.Fatalf("control: %v", err)
	}
}

// Core: a denial of the beacon subject that lands between the chunk's PUBs
// and the flush hides an earlier denial of the data path (nats.go keeps only
// the last -ERR). The ErrorHandler is held back, so only writeCore's own
// maskedDenial check can refuse to confirm the chunk.
func TestNATSCoreMaskedDenialIsNotConfirmed(t *testing.T) {
	srv := startNATSFake(t, natsFakeINFO(""))
	n := fakeSink(t, srv, NATSModeCore, nil, nil)
	holdAsyncErrors(n)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := n.Health(ctx); err != nil {
		t.Fatalf("control: Health = %v", err)
	}
	srv.set(func(f *natsFake) {
		f.replyFor = func(subject string) string {
			return "-ERR 'Permissions Violation for Publish to \"" + subject + "\"'\r\n"
		}
	})
	beforeCoreFlush = func() { _ = n.nc.Publish("loghub._ctl.beacon", []byte("{}")) }
	t.Cleanup(func() { beforeCoreFlush = nil })
	if err := n.Write(ctx, batchOf(t, line5424)); err == nil {
		t.Fatal("a chunk the server denied was confirmed: the beacon's -ERR masked the data path's")
	}
}

// Core Health reads LastError after its flush: a denial of the data path
// becomes the verdict there even with the ErrorHandler late.
func TestNATSCoreHealthReadsTheDenialItsFlushFollows(t *testing.T) {
	srv := startNATSFake(t, natsFakeINFO(""))
	n := fakeSink(t, srv, NATSModeCore, nil, nil)
	holdAsyncErrors(n)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := n.Health(ctx); err != nil {
		t.Fatalf("control: Health = %v", err)
	}
	srv.set(func(f *natsFake) {
		f.pubReply = "-ERR 'Permissions Violation for Publish to \"loghub.default.syslog\"'\r\n"
	})
	if err := n.nc.Publish("loghub.default.syslog", []byte("{}")); err != nil {
		t.Fatal(err)
	}
	if err := n.Health(ctx); !IsFatal(err) {
		t.Fatalf("Health after a denied publish = %v, want the fatal verdict", err)
	}
}

// JetStream: a publish that fails after another of the chunk is already in
// the air must taint the chunk (that future still points at its buffer, SPEC
// §15.2). A window of one makes the second publish stall and fail.
func TestNATSPublishErrorAfterTheFirstTaints(t *testing.T) {
	srv := startNATSFake(t, natsFakeINFO(`,"jetstream":true`))
	n := fakeSink(t, srv, NATSModeJetStream, nil, func(c *NATSConfig) { c.MaxPending = 1 })
	if _, _, err := n.connect(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := n.Write(ctx, batchOf(t, line5424, line5424))
	if err == nil || !taints(err) {
		t.Fatalf("Write = %v, taint=%v; want a tainted error", err, taints(err))
	}
}

// JetStream: a logic refusal in an ack ends the chunk with Delivered, and the
// error taints (SPEC §10.7 leaves the buffer of any failed write to the GC).
func TestNATSLogicRefusalInAnAckTaints(t *testing.T) {
	srv := startNATSFake(t, natsFakeINFO(`,"jetstream":true`))
	srv.set(func(f *natsFake) {
		f.acks = true
		f.ackErr = func(n int) string {
			if n == 2 {
				return `{"error":{"code":400,"err_code":10054,"description":"message size exceeds maximum allowed"}}`
			}
			return ""
		}
	})
	n := fakeSink(t, srv, NATSModeJetStream, nil, nil)
	if _, _, err := n.connect(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := n.Write(ctx, batchOf(t, line5424, line5424, line5424))
	var e *Error
	if !errors.As(err, &e) || e.Class != ClassLogic || !taints(err) || len(e.Delivered) != 2 {
		t.Fatalf("Write = %v (%+v), want a tainted logic refusal with 2 delivered", err, e)
	}
}

// WithRetryAttempts(0): a "no responders" answer must not make the library
// publish the message again on its own -- the retry would re-read msg.Data
// after Write returned (SPEC §15.2). Exactly one PUB reaches the server.
func TestNATSLibraryNeverRepublishes(t *testing.T) {
	srv := startNATSFake(t, natsFakeINFO(`,"jetstream":true`))
	srv.set(func(f *natsFake) { f.noResponders = true })
	n := fakeSink(t, srv, NATSModeJetStream, nil, func(c *NATSConfig) { c.OpTimeout = 2 * time.Second })
	if _, _, err := n.connect(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	before := srv.pubs.Load()
	if err := n.Write(ctx, batchOf(t, line5424)); err == nil {
		t.Fatal("a publish nobody acknowledged was confirmed")
	}
	time.Sleep(700 * time.Millisecond) // the library's retry wait is 250 ms
	if got := srv.pubs.Load() - before; got != 1 {
		t.Fatalf("%d PUBs for one message: the library republished it", got)
	}
}
