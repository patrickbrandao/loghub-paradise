package config

import (
	"fmt"
	"io"
	"strings"
)

// redact masks a value as its parameter says (Param.Redact). The
// effective-config dump goes to logs and to stdout, and both end up in places
// a password should not.
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

// isSecret reports whether p's value is masked in the dump: a line of the INI
// right after one is not quoted in an error either (lookupINI).
func isSecret(p *Param) bool { return p.Redact != RedactNone }

// Usage writes --help. It is generated from the registry, so a parameter cannot
// exist without being documented, and the documented default is by construction
// the one the code uses.
func Usage(w io.Writer) error {
	cfg := &Config{}
	ix := newIndex(Registry(cfg))

	fmt.Fprint(w, `go-webhook-nats-server -- LogHub HTTP bridge-in, fast mode, memory ring to NATS

Usage:
  go-webhook-nats-server [--config FILE] [--section.key=value ...]

Precedence (later wins):
  defaults  <  ini file  <  environment  <  command line

Every parameter below has three equivalent spellings:
  flag        --sink.nats.urls=nats://10.0.0.5:4222
  environment WHS_SINK_NATS_URLS=nats://10.0.0.5:4222
  ini         [sink.nats]
              urls = nats://10.0.0.5:4222

The [loghub] keys read the names the LogHub skill fixes instead:
LOGHUB_EB_URL, LOGHUB_BEACON_INTERVAL and LOGHUB_CTL_NAMESPACE.

Switches:
  --config FILE     read this ini file (also WHS_CONFIG)
  --dump-config     print the effective configuration and exit
  --version         print version and exit
  --help, -h        print this help and exit

`)

	for _, sec := range ix.sections() {
		fmt.Fprintf(w, "[%s]\n", sec)
		for _, p := range ix.inSection(sec) {
			def := p.Def
			if def == "" {
				def = `""`
			}
			fmt.Fprintf(w, "  %-28s %-9s default %s\n", p.Key, p.Kind, def)
			for _, line := range wrap(p.Desc, 68) {
				fmt.Fprintf(w, "  %-28s %s\n", "", line)
			}
		}
		fmt.Fprintln(w)
	}
	return nil
}

// DumpINI writes the effective configuration as a valid ini file: feeding the
// output back in with --config reproduces this exact configuration, except for
// redacted secrets.
//
// Provenance is annotated with whole-line comments rather than trailing ones,
// because this dialect treats '#' inside a value as literal (see parseINI). A
// trailing "# flag" would be read back as part of the value and the dump would
// not be re-loadable, which is the one property that makes it worth printing.
func (l *Loaded) DumpINI(w io.Writer) error {
	ix := newIndex(l.Params)

	fmt.Fprintln(w, "# effective configuration of go-webhook-nats-server")
	if l.File != "" {
		fmt.Fprintf(w, "# base file: %s\n", l.File)
	}
	fmt.Fprintln(w, "# values without a source comment are compiled-in defaults")
	for _, p := range l.Params {
		if v := formatValue(p.Kind, p.Ptr); redact(p, v) != v {
			fmt.Fprintln(w, "# WARNING: secrets are shown as *** -- this dump cannot be used as a config file as-is")
			break
		}
	}
	fmt.Fprintln(w)

	for _, sec := range ix.sections() {
		fmt.Fprintf(w, "[%s]\n", sec)
		for _, p := range ix.inSection(sec) {
			name := p.Name()
			if l.Source[name] == FromEBURL || l.ebOwned[name] {
				// Shown, but commented: read back, the eb_url in [loghub]
				// decides it again, and an explicit copy here would be the
				// second source of truth that SPEC §8.7.11 refuses. That holds
				// for an owned key the URL left at its default too (a NATS
				// URL without a token still owns sink.nats.token).
				if l.Source[name] == FromEBURL {
					fmt.Fprintln(w, "# derived from loghub.eb_url (commented: the URL derives it again when this file is read)")
				} else {
					fmt.Fprintln(w, "# decided by loghub.eb_url, which leaves it at its default (commented: an explicit value would conflict)")
				}
				fmt.Fprintf(w, "# %-28s = %s\n", p.Key, quoteINI(redact(p, formatValue(p.Kind, p.Ptr))))
				continue
			}
			if src := l.Source[name]; src != FromDefault {
				fmt.Fprintf(w, "# from %s\n", src)
			}
			if l.derived[name] {
				// Commented: read back, the value is derived again (from
				// sink.workers, from ring.data_dir) instead of fixed.
				fmt.Fprintln(w, "# derived (commented: it is derived again when this file is read)")
				fmt.Fprintf(w, "# %-28s = %s\n", p.Key, quoteINI(redact(p, formatValue(p.Kind, p.Ptr))))
				continue
			}
			fmt.Fprintf(w, "%-30s = %s\n", p.Key, quoteINI(redact(p, formatValue(p.Kind, p.Ptr))))
		}
		fmt.Fprintln(w)
	}
	return nil
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
		out[p.Name()] = redact(p, formatValue(p.Kind, p.Ptr))
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
