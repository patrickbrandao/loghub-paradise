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

	"github.com/patrickbrandao/loghub-paradise/go-relplog-pg-server/internal/obs"
)

// SPEC §2.3, §5.7, §9.2, §14, invariant §20.2: a full ring is never acceptance
// followed by a drop, and never a 500. The transaction that finds it full is
// HELD -- read, not accepted, not answered -- and gets its "200 OK" when the
// ring drains; /readyz fails above ring.high_water meanwhile.
//
// The ring's ceilings count every message accepted and not yet committed,
// including the batch a worker holds while it retries (SPEC §9.2): the worker
// takes the first message and blocks on the destination, and three more fill
// the remaining slots.
func TestFullRingHoldsTheTransaction(t *testing.T) {
	fs := newFaultySink(true) // the destination does not confirm until the gate opens
	in := launch(t, testID, append(baseArgs(t, t.TempDir()),
		"--ring.capacity_items=4", "--ring.high_water=0.5"), fs.wrap)
	in.waitReady()
	m := in.app.metrics
	tcp := in.app.TransportAddr("tcp")
	line := func(s string) string { return "<13>1 2026-09-12T14:33:01Z host app - - - " + s }

	sendRELP(t, tcp, line("full-0"))
	waitFor(t, 10*time.Second, "a worker holding the first message", func() bool { return fs.attempts.Load() > 0 })
	sendRELP(t, tcp, line("full-1"), line("full-2"), line("full-3"))
	if a := m.MessagesAcceptedTotal[obs.TransportTCP].Load(); a != 4 {
		t.Fatalf("%d accepted after four answers of 200", a)
	}
	waitFor(t, 10*time.Second, "/readyz above high water", func() bool {
		code, body := in.get("/readyz")
		return code == 503 && strings.Contains(body, "ring above high water")
	})

	rc := dialRELP(t, tcp)
	ids := rc.syslog(line("held-on-full"))
	waitFor(t, 10*time.Second, "the held transaction read", func() bool { return m.MessagesTotal[obs.TransportTCP].Load() == 5 })
	// Held: no answer at all -- a 500 would make librelp destroy the message.
	rc.expectNothing(500 * time.Millisecond)
	if a := m.MessagesAcceptedTotal[obs.TransportTCP].Load(); a != 4 {
		t.Fatalf("on a full ring: %d accepted, want 4", a)
	}
	noDrops(t, m)

	fs.open()
	if got := rc.responses(ids); got[0] != relpRSPOK {
		t.Fatalf("the held transaction was answered %q once the ring drained, want %q", got[0], relpRSPOK)
	}
	rc.close()
	rows := in.waitRows(5)
	got := map[string]bool{}
	for _, r := range rows {
		got[checkRow(t, r).Payload.message()] = true
	}
	for _, want := range []string{"full-0", "full-1", "full-2", "full-3", "held-on-full"} {
		if !got[want] {
			t.Errorf("%s is missing", want)
		}
	}
	noDrops(t, m)
	waitFor(t, 10*time.Second, "/readyz 200 once drained", func() bool { code, _ := in.get("/readyz"); return code == 200 })
}

// SPEC §9.2 ("dois tetos, avaliados juntos"; both count until Commit), §14.1:
// the ring's occupancy as reported -- relplog_ring_items and the "N/M items" of
// /readyz -- never exceeds ring.capacity_items, and a batch a worker holds
// counts against it. Found by the syslog sibling's suite: the memory ring
// bounded only the channel, accepted a fifth message on a capacity of four and
// reported 5/4.
func TestRingItemsNeverExceedTheCapacity(t *testing.T) {
	fs := newFaultySink(true)
	in := launch(t, testID, append(baseArgs(t, t.TempDir()), "--ring.capacity_items=4"), fs.wrap)
	in.waitReady()
	m := in.app.metrics
	tcp := in.app.TransportAddr("tcp")
	sendRELP(t, tcp, "<13>held")
	waitFor(t, 10*time.Second, "a worker holding a message", func() bool { return fs.attempts.Load() > 0 })

	// Four at once on one session: the window decides them concurrently, so
	// WHICH one finds the ring full is not fixed -- but exactly three fit.
	rc := dialRELP(t, tcp)
	ids := rc.syslog("<13>a", "<13>b", "<13>c", "<13>d")
	waitFor(t, 10*time.Second, "all five read", func() bool { return m.MessagesTotal[obs.TransportTCP].Load() == 5 })
	answered := map[int64]bool{}
	for len(answered) < 3 {
		f := rc.read()
		if f.cmd != "rsp" || f.data != relpRSPOK {
			t.Fatalf("got %v, want a 200 for one of the three that fit", f)
		}
		answered[f.txnr] = true
	}
	// The fourth would be decided within one Enqueue; it must stay held by
	// backpressure, not accepted.
	rc.expectNothing(500 * time.Millisecond)
	if a := m.MessagesAcceptedTotal[obs.TransportTCP].Load(); a != 4 {
		t.Errorf("%d accepted on a ring of four with one held", a)
	}
	items, capItems := in.metric("relplog_ring_items"), in.metric("relplog_ring_capacity_items")
	if n, _ := strconv.Atoi(items); n > 4 {
		t.Errorf("relplog_ring_items = %s above relplog_ring_capacity_items = %s", items, capItems)
	}
	if _, body := in.get("/readyz"); strings.Contains(body, "5/4 items") {
		t.Errorf("/readyz reports an occupancy above the capacity: %q", body)
	}
	fs.open()
	var last []int64
	for _, id := range ids {
		if !answered[id] {
			last = append(last, id)
		}
	}
	if got := rc.responses(last); got[0] != relpRSPOK {
		t.Errorf("the held one was answered %q", got[0])
	}
	in.waitRows(5) // backpressure, not loss: the held one is delivered after
	noDrops(t, m)
}

// SPEC §6.6, §6.8: with syslog.payload_raw=false, raw leaves the envelopes
// where it is optional, and stays where it is mandatory -- a malformed message
// (accepted by default) and a message that is not UTF-8.
func TestPayloadRawFalseKeepsTheMandatoryRaw(t *testing.T) {
	in := start(t, t.TempDir(), "--syslog.payload_raw=false")
	sendRELP(t, in.app.TransportAddr("tcp"),
		"<13>1 2026-09-12T14:33:01Z host app - - - bem-formada",
		"GET / HTTP/1.1",
		"<13>1 2026-09-12T14:33:02Z host app - - - latin \xe9 octet",
	)
	envs := byMessage(t, in.waitRows(3))
	if e := messageWith(t, envs, "bem-formada"); e.Payload.Raw != nil {
		t.Errorf("payload_raw=false left raw in a well-formed envelope: %s", e.bytes)
	}
	e := messageWith(t, envs, "GET / HTTP/1.1")
	if e.Payload.Format != "malformed" || string(e.Payload.rawBytes()) != "GET / HTTP/1.1" ||
		e.Payload.Parse == nil || e.Payload.Parse.Error == "" {
		t.Errorf("malformed without its mandatory raw or parse.error: %s", e.bytes)
	}
	e = messageWith(t, envs, "latin")
	if e.Payload.RawEnc != "base64" || !strings.HasSuffix(string(e.Payload.rawBytes()), "latin \xe9 octet") ||
		!e.Payload.hasWarning("encoding-invalid") {
		t.Errorf("non-UTF-8 without its mandatory base64 raw: %s", e.bytes)
	}
}

// SPEC §16.3: a SIGHUP that reads an invalid file applies NOTHING; the
// running configuration stays as it was. The invalid file also carries a
// valid change to log.level, which must not be applied with it.
func TestReloadOfAnInvalidFileAppliesNothing(t *testing.T) {
	dir := t.TempDir()
	ini := filepath.Join(dir, "relplog.ini")
	if err := os.WriteFile(ini, []byte("[log]\nlevel = error\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	in := launch(t, testID, append(withoutFlag(baseArgs(t, dir), "--log.level="), "--config="+ini), nil)
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
