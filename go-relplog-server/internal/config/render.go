package config

import (
	"bufio"
	"fmt"
	"io"
	"strings"
)

// redact renders a value for output that may reach a log or a terminal. The
// effective-config dump goes to stdout and the boot log line, and both end up
// in places a password should not.
func redact(p *Param, value string) string {
	switch p.Redact {
	case RedactAll:
		if value != "" {
			return "***"
		}
	case RedactURL:
		return redactURL(value)
	}
	return value
}

// InfoSection is an informative block appended to the dump, such as the
// [identity] the app resolves with go-loghub-ident (SPEC §16.2). Config does
// not import that library: the app reads the values and hands them over.
type InfoSection struct {
	Name    string      // section name, e.g. "identity"
	Note    string      // one line saying where the values come from
	Entries []InfoEntry // printed in this order
}

// InfoEntry is one key = value line of an InfoSection.
type InfoEntry struct {
	Key   string
	Value string
}

// Usage writes --help. It is generated from the registry, so a parameter cannot
// exist without being documented, and the documented default is by construction
// the one the code uses.
func Usage(w io.Writer) error {
	bw := bufio.NewWriter(w)
	cfg := &Config{}
	ix := newIndex(Registry(cfg))

	fmt.Fprint(bw, `go-relplog-server -- LogHub RELP server: rsyslog's RELP (omrelp) in, over TCP
or TLS, acknowledged per message; JLH type=syslog out (NATS JetStream by default)

Usage:
  go-relplog-server [--config FILE] [--section.key=value ...]

Precedence (later wins):
  defaults  <  ini file  <  environment  <  command line

Every parameter below has three equivalent spellings:
  flag        --sink.nats.urls=nats://10.0.0.5:4222
  environment RELPLOG_SINK_NATS_URLS=nats://10.0.0.5:4222
  ini         [sink.nats]
              urls = nats://10.0.0.5:4222

The environment name is RELPLOG_<SECTION>_<KEY>, except where a LogHub skill
fixed it: [syslog] keys are SYSLOG_<KEY> (SYSLOG_MAX_MESSAGE_BYTES), [loghub]
keys are LOGHUB_<KEY> (LOGHUB_EB_URL) and server.data_dir is DATADIR. Each
parameter shows its name.

The identity (AGENT_UUID, AGENT_NAME, WORKSPACE, HOSTNAME, MACHINE_ID,
MACHINE_ID_FILE) is read by go-loghub-ident from the environment, not from
this table; --dump-config shows the resolved values.

Durations use Go syntax (30s, 5ms); loghub.beacon_interval also takes whole
seconds. Sizes take KiB, MiB, GiB or a plain number of bytes.

RELP in clear listens on 20514, a high port: the container image runs as an
unprivileged user. RELP over TLS is off until listen.tls is set (20515 is a
suggestion) with tls.cert_file and tls.key_file.

Switches:
  --config FILE     read this ini file (also RELPLOG_CONFIG)
  --dump-config     print the effective configuration and exit
  --version         print version and exit
  --help, -h        print this help and exit

`)

	for _, sec := range ix.sections() {
		fmt.Fprintf(bw, "[%s]\n", sec)
		for _, p := range ix.inSection(sec) {
			def := p.Def
			if def == "" {
				def = `""`
			}
			fmt.Fprintf(bw, "  %-28s %-9s default %s\n", p.Key, p.Kind, def)
			fmt.Fprintf(bw, "  %-28s env %s\n", "", p.Env())
			if p.Kind == KEnum {
				shown := make([]string, len(p.Enum))
				for i, e := range p.Enum {
					if e == "" {
						e = `""`
					}
					shown[i] = e
				}
				fmt.Fprintf(bw, "  %-28s one of %s\n", "", strings.Join(shown, " | "))
			}
			for _, line := range wrap(p.Desc, 68) {
				fmt.Fprintf(bw, "  %-28s %s\n", "", line)
			}
			if p.Reload {
				fmt.Fprintf(bw, "  %-28s (reloadable with SIGHUP)\n", "")
			}
		}
		fmt.Fprintln(bw)
	}
	return bw.Flush()
}

// DumpINI writes the effective configuration as a valid ini file: feeding the
// output back in with --config reproduces this exact configuration, except for
// redacted secrets. Values filled by loghub.eb_url are shown as derived
// (SPEC §10.9), with passwords redacted, on commented lines.
//
// Provenance is annotated with whole-line comments rather than trailing ones,
// because this dialect treats '#' inside a value as literal (see parseINI). A
// trailing "# flag" would be read back as part of the value and the dump would
// not be re-loadable, which is the one property that makes it worth printing.
//
// Each InfoSection is appended as a COMMENTED block ("# [identity]"): it is
// shown, it is not configuration, and a copy pasted back into a file does not
// pretend to set anything.
func (l *Loaded) DumpINI(w io.Writer, info ...InfoSection) error {
	bw := bufio.NewWriter(w)
	ix := newIndex(l.Params)

	fmt.Fprintln(bw, "# effective configuration of go-relplog-server")
	if l.File != "" {
		fmt.Fprintf(bw, "# base file: %s\n", l.File)
	}
	fmt.Fprintln(bw, "# values without a comment are compiled-in defaults")
	for _, p := range l.Params {
		if v := formatValue(p); redact(p, v) != v {
			fmt.Fprintln(bw, "# WARNING: secrets are shown as *** -- this dump cannot be used as a config file as-is")
			break
		}
	}
	fmt.Fprintln(bw)

	for _, sec := range ix.sections() {
		fmt.Fprintf(bw, "[%s]\n", sec)
		for _, p := range ix.inSection(sec) {
			name := p.Name()
			if l.Source[name] == FromEBURL || l.ebOwned[name] {
				// Shown, but commented: read back, the eb_url in [loghub]
				// decides it again, and an explicit copy here would be the
				// second source of truth that SPEC §10.9 refuses. That holds
				// for an owned key the URL left at its default too (a NATS
				// URL without a token still owns sink.nats.token).
				if l.Source[name] == FromEBURL {
					fmt.Fprintln(bw, "# derived from loghub.eb_url (commented: the URL derives it again when this file is read)")
				} else {
					fmt.Fprintln(bw, "# decided by loghub.eb_url, which leaves it at its default (commented: an explicit value would conflict)")
				}
				fmt.Fprintf(bw, "# %-28s = %s\n", p.Key, quoteINI(redact(p, formatValue(p))))
				continue
			}
			if src := l.Source[name]; src != FromDefault {
				fmt.Fprintf(bw, "# from %s\n", l.Origin[name])
			}
			if note := l.Notes[name]; note != "" {
				fmt.Fprintf(bw, "# %s\n", note)
				if l.Source[name] == FromDefault && strings.HasPrefix(note, "derived") {
					// A derived value written as a key would be explicit when
					// the dump is read back: sink.postgres.pool_size would
					// stop following sink.workers, emergency.dir and the
					// sqlite paths stop following data_dir. Commented, it is
					// derived again (independent review of
					// go-relplog-pg-server, 2026-10-13).
					fmt.Fprintf(bw, "# %-28s = %s\n", p.Key, quoteINI(redact(p, formatValue(p))))
					continue
				}
			}
			fmt.Fprintf(bw, "%-30s = %s\n", p.Key, quoteINI(redact(p, formatValue(p))))
		}
		fmt.Fprintln(bw)
	}

	for _, s := range info {
		fmt.Fprintf(bw, "# [%s]\n", s.Name)
		if s.Note != "" {
			for _, line := range wrap(s.Note, 76) {
				fmt.Fprintf(bw, "# %s\n", line)
			}
		}
		for _, e := range s.Entries {
			fmt.Fprintf(bw, "# %-28s = %s\n", e.Key, e.Value)
		}
		fmt.Fprintln(bw)
	}
	return bw.Flush()
}

// quoteINI wraps a value whose surrounding whitespace or quotes the parser
// would otherwise strip. unquote removes exactly one layer, so the original
// comes back unchanged.
func quoteINI(v string) string {
	if v != strings.TrimSpace(v) {
		return `"` + v + `"`
	}
	if len(v) >= 2 && (v[0] == '"' || v[0] == '\'') && v[len(v)-1] == v[0] {
		return `"` + v + `"`
	}
	return v
}

// Effective returns the flat name/value map used by the boot log line. Secrets
// are redacted here too.
func (l *Loaded) Effective() map[string]string {
	out := make(map[string]string, len(l.Params))
	for _, p := range l.Params {
		out[p.Name()] = redact(p, formatValue(p))
	}
	return out
}

// Values returns the flat name/value map WITHOUT redaction. It exists for
// comparing two snapshots -- a changed password has to register as a change --
// and must never be logged; Effective is the loggable form.
func (l *Loaded) Values() map[string]string {
	out := make(map[string]string, len(l.Params))
	for _, p := range l.Params {
		out[p.Name()] = formatValue(p)
	}
	return out
}

func wrap(s string, width int) []string {
	if s == "" {
		return nil
	}
	words := strings.Fields(s)
	var (
		lines []string
		cur   strings.Builder
	)
	for _, w := range words {
		if cur.Len() > 0 && cur.Len()+1+len(w) > width {
			lines = append(lines, cur.String())
			cur.Reset()
		}
		if cur.Len() > 0 {
			cur.WriteByte(' ')
		}
		cur.WriteString(w)
	}
	if cur.Len() > 0 {
		lines = append(lines, cur.String())
	}
	return lines
}
