package syslog

import "time"

// months is indexed by the lowercased three-letter abbreviation. A map keyed on
// a [3]byte array avoids allocating a string per message just to look the month
// up, which on this path happens once per received line.
var months = map[[3]byte]time.Month{
	{'j', 'a', 'n'}: time.January, {'f', 'e', 'b'}: time.February,
	{'m', 'a', 'r'}: time.March, {'a', 'p', 'r'}: time.April,
	{'m', 'a', 'y'}: time.May, {'j', 'u', 'n'}: time.June,
	{'j', 'u', 'l'}: time.July, {'a', 'u', 'g'}: time.August,
	{'s', 'e', 'p'}: time.September, {'o', 'c', 't'}: time.October,
	{'n', 'o', 'v'}: time.November, {'d', 'e', 'c'}: time.December,
}

const tagMaxBytes = 32 // RFC 3164 §4.1.3

// parse3164 implements SPEC §6.3.
//
// RFC 3164 is from 2001 and documents what BSD happened to do, not a grammar.
// So every step here is INDEPENDENT and optional: a step that fails does not
// abandon the ones after it, it just leaves its field empty and lets MSG start
// where the last successful step stopped.
//
// It reports whether the message should be classified as 3164 at all. Having a
// valid PRI is enough on its own -- that already makes it syslog, even when the
// body is free text.
func parse3164(p *Parsed, raw []byte, hdr int, now time.Time) bool {
	b := raw[hdr:]
	off := hdr
	matched := false

	// --- 1. TIMESTAMP (optional) --------------------------------------------
	//
	// Either the BSD form or a full RFC 3339 timestamp in the same place. The
	// second is not in RFC 3164, and it is the DEFAULT output of rsyslog's
	// RSYSLOG_ForwardFormat: without it, the timestamp fails, no hostname is
	// read, and the tag becomes "2026-09-13T10" -- everything up to the first
	// colon of the time (SPEC §6.3).
	ts, n, tsOK := parse3164Time(b, now)
	src := TS3164
	if !tsOK {
		if tok, tn := token(b); tn > 0 {
			if t3339, ok := parse5424Time(tok); ok {
				// It carries a year and a zone, so nothing was invented, and
				// msg-ts-source says so: the field names the timestamp's
				// FORMAT, which is what tells a consumer how far to trust it.
				ts, n, tsOK, src = t3339, tn, true, TS5424
			}
		}
	}
	if tsOK {
		p.MsgTS = ts
		p.MsgTSSrc = src
		off += n
		b = b[n:]
		off, b = skipSpace(raw, off, b)
		p.setMsg(raw, off)
		matched = true

		// --- 2. HOSTNAME ----------------------------------------------------
		//
		// Read ONLY when the timestamp matched: that is the only positional
		// anchor this grammar has. Without it no hostname is guessed at all --
		// in "<14>mensagem solta" the first word would become the hostname of a
		// machine that does not exist, and a wrong value is worse than an
		// absent one.
		//
		// And even after the timestamp, a token that ENDS in ':' is the tag,
		// not the hostname. That is how syslog(3) writes to /dev/log -- glibc
		// and journald's ForwardToSyslog both omit the hostname entirely:
		//
		//	<30>Sep 13 10:00:00 systemd[1]: Started Session 42.
		//
		// Treating "systemd[1]:" as a hostname put the wrong value in a
		// promoted column for every local message on every Linux host. A real
		// hostname never ends in ':'; an IPv6 address has colons, but not last.
		if tok, n := token(b); n > 0 && tok[len(tok)-1] != ':' {
			p.Hostname = tok
			off += n
			b = b[n:]
			off, b = skipSpace(raw, off, b)
			p.setMsg(raw, off)
		}
	}

	// --- 3/4/5. TAG [PID] ':' -----------------------------------------------
	//
	// The mandatory ':' is what makes reading a tag without a positional anchor
	// safe. "Connection closed by 10.0.0.1" yields no tag, because "Connection"
	// is not followed by one.
	if n, ok := parse3164Tag(p, b); ok {
		off += n
		b = b[n:]
		// Exactly one optional space after the colon. More than one is part of
		// the message: the sender put it there.
		if len(b) > 0 && b[0] == ' ' {
			off++
		}
		p.setMsg(raw, off)
		matched = true
	}

	return matched || hdr > 0
}

// parse3164Tag reads "TAG:", "TAG[PID]:" and nothing else. It returns the bytes
// consumed up to and including the colon.
func parse3164Tag(p *Parsed, b []byte) (int, bool) {
	// A program name starts with a letter, an underscore or a slash
	// (/usr/sbin/cron). Without this, the colon rule alone accepts a number as a
	// tag: Cisco IOS writes "<189>1234: *Sep 13 ...", and "1234" would become
	// the app-name of every line it sends -- a sequence counter in a column
	// people GROUP BY (SPEC §6.3).
	if len(b) == 0 || !isTagStart(b[0]) {
		return 0, false
	}
	i := 0
	for i < len(b) && i < tagMaxBytes {
		c := b[i]
		if c == '[' || c == ':' || c == ' ' {
			break
		}
		i++
	}
	if i == 0 || i >= len(b) {
		return 0, false
	}
	name := b[:i]

	var pid []byte
	if b[i] == '[' {
		j := i + 1
		for j < len(b) && b[j] >= '0' && b[j] <= '9' {
			j++
		}
		// A bracket with no digits, or one that never closes, is not a pid.
		// Bailing out here rather than guessing keeps "kernel[oops" out of
		// procid.
		if j == i+1 || j >= len(b) || b[j] != ']' {
			return 0, false
		}
		pid = b[i+1 : j]
		i = j + 1
		if i >= len(b) {
			return 0, false
		}
	}
	if b[i] != ':' {
		return 0, false
	}

	p.AppName = name
	p.ProcID = pid
	return i + 1, true
}

// parse3164Time reads "Mmm dd hh:mm:ss", exactly fifteen bytes.
//
// The day is space-padded in the RFC ("Oct  1") and zero-padded by half the
// emitters in the field ("Oct 01"); both are accepted, because refusing one
// would drop the timestamp of every message from those senders.
//
// The year and the zone do not exist in this format and have to be invented.
// The rules are fixed and NOT configurable (SPEC §6.4):
//
//   - The zone is UTC. Using the server's local zone would make the same
//     message mean different instants on two nodes of one cluster.
//   - The year is the year of reception, except when that lands more than a
//     day in the future, which means the message crossed midnight on 31
//     December and belongs to the year before. Without the correction it would
//     jump twelve months forward.
//
// And then the result is used for nothing (SPEC §6.4). It is exported so an
// operator can measure emitter clock drift; this server's time is ts.
func parse3164Time(b []byte, now time.Time) (time.Time, int, bool) {
	const n = 15
	if len(b) < n {
		return time.Time{}, 0, false
	}
	var key [3]byte
	for i := 0; i < 3; i++ {
		key[i] = lowerByte(b[i])
	}
	mon, ok := months[key]
	if !ok || b[3] != ' ' {
		return time.Time{}, 0, false
	}

	day, ok := twoDigits(b[4], b[5], true)
	if !ok || day < 1 || day > 31 || b[6] != ' ' {
		return time.Time{}, 0, false
	}
	hh, ok1 := twoDigits(b[7], b[8], false)
	mm, ok2 := twoDigits(b[10], b[11], false)
	ss, ok3 := twoDigits(b[13], b[14], false)
	if !ok1 || !ok2 || !ok3 || b[9] != ':' || b[12] != ':' {
		return time.Time{}, 0, false
	}
	// 23:59:60 is a leap second and does occur. time.Date normalises it into
	// the next minute rather than rejecting it, which is the right outcome.
	if hh > 23 || mm > 59 || ss > 60 {
		return time.Time{}, 0, false
	}

	t := time.Date(now.Year(), mon, day, hh, mm, ss, 0, time.UTC)
	if t.Sub(now) > 24*time.Hour {
		t = t.AddDate(-1, 0, 0)
	}
	return t, n, true
}

func twoDigits(a, b byte, allowSpacePad bool) (int, bool) {
	if allowSpacePad && a == ' ' {
		a = '0'
	}
	if a < '0' || a > '9' || b < '0' || b > '9' {
		return 0, false
	}
	return int(a-'0')*10 + int(b-'0'), true
}

func lowerByte(c byte) byte {
	if c >= 'A' && c <= 'Z' {
		return c + 32
	}
	return c
}

// token returns the bytes up to the next space, and how many to consume.
func token(b []byte) ([]byte, int) {
	i := 0
	for i < len(b) && b[i] != ' ' {
		i++
	}
	return b[:i], i
}

// skipSpace consumes one space, which is all the grammar allows between
// fields. Consuming a run of them would silently accept a malformed message and
// shift every following field.
func skipSpace(raw []byte, off int, b []byte) (int, []byte) {
	if len(b) > 0 && b[0] == ' ' {
		return off + 1, b[1:]
	}
	return off, b
}

func isTagStart(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_' || c == '/'
}
