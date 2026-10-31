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

	"github.com/patrickbrandao/loghub-paradise/go-gelf-pg-server/internal/obs"
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
	fs := newFaultySink(true) // the destination does not confirm until the gate opens
	in := launch(t, testID, append(baseArgs(t, t.TempDir()),
		"--ring.capacity_items=4", "--ring.high_water=0.5"), fs.wrap)
	in.waitReady()
	m := in.app.metrics
	tcp := in.app.TransportAddr("tcp")
	line := func(s string) string { return doc(s) }

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

	code, hdr := postGELF(t, nil, "http://"+in.app.TransportAddr("http")+"/gelf", "", []byte(line("http-on-full")))
	if code != 503 || hdr.Get("Retry-After") == "" {
		t.Errorf("POST on a full ring = %d Retry-After=%q, want 503 with Retry-After", code, hdr.Get("Retry-After"))
	}

	c := dialTCP(t, tcp)
	defer c.Close()
	writeNUL(t, c, line("tcp-on-full"))
	waitFor(t, 10*time.Second, "the held TCP message read", func() bool { return m.MessagesTotal[obs.TransportTCP].Load() == 5 })
	if a, d := m.MessagesAcceptedTotal[obs.TransportTCP].Load(), m.MessagesDroppedTotal[obs.DropRingFull].Load(); a != 4 || d != 2 {
		t.Fatalf("on a full ring: tcp accepted %d (want 4), ring_full drops %d (want the UDP and the HTTP ones)", a, d)
	}

	fs.open()
	rows := in.waitRows(5)
	got := map[string]bool{}
	for _, r := range rows {
		got[checkRow(t, r).Payload.message()] = true
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
	fs := newFaultySink(true)
	in := launch(t, testID, append(baseArgs(t, t.TempDir()), "--ring.capacity_items=4"), fs.wrap)
	in.waitReady()
	m := in.app.metrics
	tcp := in.app.TransportAddr("tcp")
	sendNUL(t, tcp, doc("held"))
	waitFor(t, 10*time.Second, "a worker holding a message", func() bool { return fs.attempts.Load() > 0 })
	sendNUL(t, tcp, doc("a"), doc("b"), doc("c"), doc("d"))
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
	in.waitRows(5) // backpressure, not loss: the held one is delivered after
}

// SPEC §6.1, §6.9: with convert.keep_raw=malformed (the default) raw is
// written only where it is mandatory -- a malformed document, invalid UTF-8
// included (base64) -- and with convert.keep_raw=always it is written for every
// document, byte for byte.
func TestKeepRawDecidesWhereRawGoes(t *testing.T) {
	latin := "{\"version\":\"1.1\",\"host\":\"h\",\"short_message\":\"latin \xe9 octet\"}"
	sent := []string{doc("bem-formada"), "GET / HTTP/1.1", latin}
	for _, always := range []bool{false, true} {
		args := []string{}
		if always {
			args = append(args, "--convert.keep_raw=always")
		}
		in := start(t, t.TempDir(), args...)
		sendNUL(t, in.app.TransportAddr("tcp"), sent...)
		seen := map[string]bool{}
		for _, r := range in.waitRows(len(sent)) {
			e := checkRow(t, r)
			raw := string(e.Payload.rawBytes())
			switch {
			case e.Payload.Format == "gelf":
				seen[sent[0]] = true
				if always && raw != sent[0] || !always && e.Payload.Raw != nil {
					t.Errorf("keep_raw always=%v, well-formed: %s", always, e.bytes)
				}
			case raw == sent[1]:
				seen[raw] = true
				if e.Payload.Parse == nil || e.Payload.Parse.Error == "" || e.Payload.RawEnc == "base64" {
					t.Errorf("malformed without its mandatory raw or parse.error: %s", e.bytes)
				}
			case raw == latin:
				seen[raw] = true
				if e.Payload.RawEnc != "base64" || e.Payload.Format != "malformed" {
					t.Errorf("invalid UTF-8 without its mandatory base64 raw: %s", e.bytes)
				}
			default:
				t.Errorf("unexpected row %s", e.bytes)
			}
		}
		if len(seen) != len(sent) {
			t.Errorf("keep_raw always=%v: rows %v", always, seen)
		}
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
