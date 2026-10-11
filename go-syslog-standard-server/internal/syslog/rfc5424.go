package syslog

import "time"

// RFC 5424 §6 field limits. They are applied as truncation, never as rejection.
const (
	hostnameMaxBytes = 255
	appNameMaxBytes  = 48
	procIDMaxBytes   = 128
	msgIDMaxBytes    = 32
)

// parse5424 implements SPEC §6.5.
//
//	<PRI>VERSION SP TIMESTAMP SP HOSTNAME SP APP-NAME SP PROCID SP MSGID SP SD [SP MSG]
//
// Unlike RFC 3164, this one IS a grammar: the six header fields are positional
// and separated by exactly one space. So a missing separator means the message
// is not RFC 5424 at all, and the caller falls back to 3164 -- there is nothing
// to salvage half-way, because every field after the break would be read from
// the wrong place.
func parse5424(p *Parsed, raw []byte, hdr int, opt Options) bool {
	b := raw[hdr:]
	off := hdr

	// --- VERSION ------------------------------------------------------------
	tok, n, ok := field(b)
	if !ok || len(tok) == 0 || len(tok) > 3 {
		return false
	}
	ver := 0
	for i, c := range tok {
		if c < '0' || c > '9' || (i == 0 && c == '0') {
			return false
		}
		ver = ver*10 + int(c-'0')
	}
	off, b = off+n, b[n:]

	// --- TIMESTAMP ----------------------------------------------------------
	tok, n, ok = field(b)
	if !ok {
		return false
	}
	var (
		ts    time.Time
		tsSrc = TSNone
		badTS bool
	)
	if v := nilIfDash(tok); v != nil {
		if t, ok := parse5424Time(v); ok {
			ts, tsSrc = t, TS5424
		} else {
			// A field that is neither NILVALUE nor a valid RFC 3339 timestamp
			// is a broken emitter, not a different format: the rest of the
			// header still parses positionally, so keep going and count it.
			badTS = true
		}
	}
	off, b = off+n, b[n:]

	// --- HOSTNAME, APP-NAME, PROCID, MSGID ----------------------------------
	host, n, ok := field(b)
	if !ok {
		return false
	}
	off, b = off+n, b[n:]

	app, n, ok := field(b)
	if !ok {
		return false
	}
	off, b = off+n, b[n:]

	proc, n, ok := field(b)
	if !ok {
		return false
	}
	off, b = off+n, b[n:]

	msgid, n, ok := field(b)
	if !ok {
		return false
	}
	off, b = off+n, b[n:]

	// --- STRUCTURED-DATA ----------------------------------------------------
	//
	// Everything above is committed only now. Parsing into p earlier would
	// leave half a header behind when a later field turns out to be missing
	// and the caller falls back to 3164.
	n = parseSD(p, b, opt)
	off, b = off+n, b[n:]

	p.Version = ver
	p.MsgTS = ts
	p.MsgTSSrc = tsSrc
	if badTS {
		p.Anomalies[AnomBadTimestamp]++
	}
	p.Hostname = truncate(nilIfDash(host), hostnameMaxBytes)
	p.AppName = truncate(nilIfDash(app), appNameMaxBytes)
	p.ProcID = truncate(nilIfDash(proc), procIDMaxBytes)
	p.MsgID = truncate(nilIfDash(msgid), msgIDMaxBytes)

	// --- MSG ----------------------------------------------------------------
	//
	// One optional space separates SD from MSG. A message may legitimately be
	// absent, in which case MsgSize is zero.
	//
	// A UTF-8 BOM at the start of MSG is NOT stripped. The raw line is
	// preserved byte for byte and this spec promises to remove nothing from it
	// (SPEC §6.5); a consumer that wants to skip it has three known bytes to
	// skip.
	if len(b) > 0 && b[0] == ' ' {
		off++
	}
	p.setMsg(raw, off)
	return true
}

// field reads one space-delimited header field and reports how many bytes to
// consume, separator included. It fails when there is no separator, which for
// a positional grammar means the header ended early.
func field(b []byte) (tok []byte, n int, ok bool) {
	i := 0
	for i < len(b) && b[i] != ' ' {
		i++
	}
	if i >= len(b) {
		return nil, 0, false
	}
	return b[:i], i + 1, true
}

// parse5424Time reads an RFC 3339 timestamp without allocating.
//
// time.Parse would be four lines instead of sixty, and would cost a string
// conversion per message -- roughly thirty bytes of garbage on a path that runs
// once for every line the server ingests. The layout is fixed-width apart from
// the optional fraction, so scanning it by hand is straightforward.
//
//	YYYY-MM-DDTHH:MM:SS[.frac](Z|(+|-)HH:MM)
func parse5424Time(b []byte) (time.Time, bool) {
	if len(b) < 20 {
		return time.Time{}, false
	}
	year, ok1 := digits(b[0:4])
	mon, ok2 := digits(b[5:7])
	day, ok3 := digits(b[8:10])
	if !ok1 || !ok2 || !ok3 || b[4] != '-' || b[7] != '-' {
		return time.Time{}, false
	}
	if b[10] != 'T' && b[10] != 't' {
		return time.Time{}, false
	}
	hh, ok1 := digits(b[11:13])
	mm, ok2 := digits(b[14:16])
	ss, ok3 := digits(b[17:19])
	if !ok1 || !ok2 || !ok3 || b[13] != ':' || b[16] != ':' {
		return time.Time{}, false
	}
	if mon < 1 || mon > 12 || day < 1 || day > 31 || hh > 23 || mm > 59 || ss > 60 {
		return time.Time{}, false
	}

	i := 19
	nsec := 0
	if i < len(b) && b[i] == '.' {
		i++
		start := i
		for i < len(b) && b[i] >= '0' && b[i] <= '9' {
			i++
		}
		if i == start {
			return time.Time{}, false
		}
		// Scale the fraction to nanoseconds, ignoring digits beyond the ninth:
		// RFC 5424 allows six, senders occasionally write more, and Go's clock
		// has no room for them anyway.
		scale := 100000000
		for _, c := range b[start:i] {
			if scale == 0 {
				break
			}
			nsec += int(c-'0') * scale
			scale /= 10
		}
	}

	if i >= len(b) {
		return time.Time{}, false
	}
	var loc *time.Location
	switch b[i] {
	case 'Z', 'z':
		if i+1 != len(b) {
			return time.Time{}, false
		}
		loc = time.UTC
	case '+', '-':
		if i+6 != len(b) || b[i+3] != ':' {
			return time.Time{}, false
		}
		oh, okh := digits(b[i+1 : i+3])
		om, okm := digits(b[i+4 : i+6])
		if !okh || !okm || oh > 23 || om > 59 {
			return time.Time{}, false
		}
		secs := oh*3600 + om*60
		if b[i] == '-' {
			secs = -secs
		}
		if secs == 0 {
			loc = time.UTC
		} else {
			loc = time.FixedZone("", secs)
		}
	default:
		return time.Time{}, false
	}

	return time.Date(year, time.Month(mon), day, hh, mm, ss, nsec, loc), true
}

func digits(b []byte) (int, bool) {
	v := 0
	for _, c := range b {
		if c < '0' || c > '9' {
			return 0, false
		}
		v = v*10 + int(c-'0')
	}
	return v, true
}
