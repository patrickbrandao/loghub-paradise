package app

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/patrickbrandao/loghub-paradise/go-gelf-nats-server/internal/obs"
)

// SPEC §2.3, §9.2, §10.2, §14, invariant §20.2: a full ring is never
// acceptance followed by a drop. UDP drops and counts (ring_full); TCP stops
// reading and loses nothing; /readyz fails above ring.high_water.
//
// The ring's ceilings count every message accepted and not yet committed,
// including the batch a worker holds while it retries (SPEC §9.2): the worker
// takes the first message and blocks on the destination, and three more fill
// the remaining slots.
func TestFullRingPerTransport(t *testing.T) {
	f := newFixture(t)
	fs := newFaultySink(true) // the destination does not confirm until the gate opens
	in := launch(t, f.id, f.args(t.TempDir(),
		"--ring.capacity_items=4", "--ring.high_water=0.5"), fs.wrap)
	in.waitReady()
	m := in.app.metrics
	tcp := in.app.TransportAddr("tcp")
	line := doc

	sendNUL(t, tcp, line("full-0"))
	waitFor(t, 10*time.Second, "a worker holding the first message", func() bool {
		return m.MessagesAcceptedTotal[obs.TransportTCP].Load() == 1 && fs.attempts.Load() > 0
	})
	sendNUL(t, tcp, line("full-1"), line("full-2"), line("full-3"))
	waitFor(t, 10*time.Second, "the four slots taken", func() bool { return m.MessagesAcceptedTotal[obs.TransportTCP].Load() == 4 })
	waitFor(t, 10*time.Second, "/readyz above high water", func() bool {
		code, body := in.get("/readyz")
		return code == 503 && strings.Contains(body, "ring above high water")
	})

	sendUDP(t, in.app.TransportAddr("udp"), line("udp-on-full"))
	waitFor(t, 10*time.Second, "the UDP drop", func() bool { return m.MessagesDroppedTotal[obs.DropRingFull].Load() == 1 })

	c := dialTCP(t, tcp)
	defer c.Close()
	writeNUL(t, c, line("tcp-on-full"))
	waitFor(t, 10*time.Second, "the held TCP message read", func() bool { return m.MessagesTotal[obs.TransportTCP].Load() == 5 })
	if a, d := m.MessagesAcceptedTotal[obs.TransportTCP].Load(), m.MessagesDroppedTotal[obs.DropRingFull].Load(); a != 4 || d != 1 {
		t.Fatalf("on a full ring: tcp accepted %d (want 4), ring_full drops %d (want only the UDP one)", a, d)
	}

	fs.open()
	msgs := f.waitMsgs(5)
	got := map[string]bool{}
	for _, m := range msgs {
		got[checkMsg(t, m).Payload.message()] = true
	}
	for _, want := range []string{"full-0", "full-1", "full-2", "full-3", "tcp-on-full"} {
		if !got[want] {
			t.Errorf("%s is missing", want)
		}
	}
	if got["udp-on-full"] {
		t.Error("the dropped datagram was delivered")
	}
	waitFor(t, 10*time.Second, "/readyz 200 once drained", func() bool { code, _ := in.get("/readyz"); return code == 200 })
}

// SPEC §9.2 ("dois tetos, avaliados juntos"; both count until Commit), §14.1:
// the ring's occupancy as reported -- gelf_ring_items and the "N/M items" of
// /readyz -- never exceeds ring.capacity_items, and a batch a worker holds
// counts against it. Found by this suite: the memory ring bounded only the
// channel, accepted a fifth message on a capacity of four and reported 5/4.
func TestRingItemsNeverExceedTheCapacity(t *testing.T) {
	f := newFixture(t)
	fs := newFaultySink(true)
	in := launch(t, f.id, f.args(t.TempDir(), "--ring.capacity_items=4"), fs.wrap)
	in.waitReady()
	m := in.app.metrics
	tcp := in.app.TransportAddr("tcp")
	sendNUL(t, tcp, doc("held"))
	waitFor(t, 10*time.Second, "a worker holding a message", func() bool { return fs.attempts.Load() > 0 })
	sendNUL(t, tcp, docs(4, "after-%d")...)
	waitFor(t, 10*time.Second, "all five read", func() bool {
		return m.MessagesTotal[obs.TransportTCP].Load() == 5 && m.MessagesAcceptedTotal[obs.TransportTCP].Load() >= 4
	})
	// The fifth would be decided within one Enqueue; it must stay held by
	// backpressure, not accepted.
	if eventually(500*time.Millisecond, func() bool { return m.MessagesAcceptedTotal[obs.TransportTCP].Load() == 5 }) {
		t.Error("a fifth message was accepted on a ring of four with one held")
	}
	items, capItems := in.metric("gelf_ring_items"), in.metric("gelf_ring_capacity_items")
	if n, _ := strconv.Atoi(items); n > 4 {
		t.Errorf("gelf_ring_items = %s above gelf_ring_capacity_items = %s", items, capItems)
	}
	if _, body := in.get("/readyz"); strings.Contains(body, "5/4 items") {
		t.Errorf("/readyz reports an occupancy above the capacity: %q", body)
	}
	fs.open()
	f.waitMsgs(5) // backpressure, not loss: the held one is delivered after
}

// SPEC §6.9 (decision 4): with the default convert.keep_raw=malformed, raw is
// absent from a well-formed envelope and present where it is mandatory -- a
// malformed document and one that is not UTF-8; keep_raw=always writes it for
// every document.
func TestKeepRawMalformedKeepsTheMandatoryRaw(t *testing.T) {
	f := newFixture(t)
	in := f.start(t.TempDir())
	sendNUL(t, in.app.TransportAddr("tcp"),
		doc("bem-formada"),
		"GET / HTTP/1.1",
		"{\"short_message\":\"latin \xe9 octet\"}",
	)
	msgs := f.waitMsgs(3)
	var good, bad, latin jlhEnv
	for _, m := range msgs {
		e := checkMsg(t, m)
		switch {
		case e.Payload.message() == "bem-formada":
			good = e
		case strings.Contains(string(e.Payload.rawBytes()), "GET /"):
			bad = e
		case strings.Contains(string(e.Payload.rawBytes()), "latin"):
			latin = e
		}
	}
	if good.UUID == "" || good.Payload.Raw != nil {
		t.Errorf("keep_raw=malformed left raw in a well-formed envelope: %s", good.bytes)
	}
	if bad.Payload.Format != "malformed" || string(bad.Payload.rawBytes()) != "GET / HTTP/1.1" ||
		bad.Payload.Parse == nil || bad.Payload.Parse.Error == "" {
		t.Errorf("malformed without its mandatory raw or parse.error: %s", bad.bytes)
	}
	if latin.Payload.RawEnc != "base64" || !strings.HasSuffix(string(latin.Payload.rawBytes()), "latin \xe9 octet\"}") {
		t.Errorf("non-UTF-8 without its mandatory base64 raw: %s", latin.bytes)
	}
}

// SPEC §16.3: a SIGHUP that reads an invalid file applies NOTHING; the
// running configuration stays as it was. The invalid file also carries a
// valid change to log.level, which must not be applied with it.
func TestReloadOfAnInvalidFileAppliesNothing(t *testing.T) {
	dir := t.TempDir()
	ini := filepath.Join(dir, "gelf.ini")
	if err := os.WriteFile(ini, []byte("[log]\nlevel = error\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	f := newFixture(t)
	in := launch(t, f.id, append(withoutFlag(f.args(dir), "--log.level="), "--config="+ini), nil)
	in.waitReady()
	if in.app.log.Enabled(context.Background(), slog.LevelInfo) {
		t.Fatal("the boot did not apply log.level=error from the file")
	}
	if err := os.WriteFile(ini, []byte("[log]\nlevel = debug\naccess_sample = 7\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if r := in.app.reload(); len(r.applied) != 0 || len(r.ignored) != 0 {
		t.Fatalf("an invalid file was applied: %+v", r)
	}
	if in.app.log.Enabled(context.Background(), slog.LevelInfo) {
		t.Error("log.level of the invalid file was applied")
	}
	// The control: the same change in a valid file is applied.
	if err := os.WriteFile(ini, []byte("[log]\nlevel = debug\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if r := in.app.reload(); strings.Join(r.applied, ",") != "log.level" {
		t.Fatalf("the valid file was not applied: %+v", r)
	}
	if !in.app.log.Enabled(context.Background(), slog.LevelDebug) {
		t.Error("log.level=debug is not in force after the valid reload")
	}
}
