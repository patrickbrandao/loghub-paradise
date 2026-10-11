package config

import (
	"fmt"
	"io"
	"strings"
)

// secretKeys are never printed in full. The effective-config dump goes to logs
// and to stdout, and both end up in places a password should not.
var secretKeys = map[string]bool{
	"sink.redis.password":    true,
	"sink.postgres.password": true,
	"sink.mysql.password":    true,
	"sink.nats.password":     true,
	"sink.nats.token":        true,
}

func redact(name, value string) string {
	if secretKeys[name] && value != "" {
		return "***"
	}
	return value
}

// Usage writes --help. It is generated from the registry, so a parameter cannot
// exist without being documented, and the documented default is by construction
// the one the code uses.
func Usage(w io.Writer) error {
	cfg := &Config{}
	ix := newIndex(Registry(cfg))

	fmt.Fprint(w, `go-syslog-server -- syslog ingestion server (UDP, TCP, unix, RELP)

Usage:
  go-syslog-server [--config FILE] [--section.key=value ...]

Precedence (later wins):
  defaults  <  ini file  <  environment  <  command line

Every parameter below has three equivalent spellings:
  flag        --sink.redis.addrs=10.0.0.5:6379
  environment SYSLOG_SINK_REDIS_ADDRS=10.0.0.5:6379
  ini         [sink.redis]
              addrs = 10.0.0.5:6379

Listener defaults are 1514 and 20514, not 514: the container image runs as an
unprivileged user. Publish them as "514:1514" or grant CAP_NET_BIND_SERVICE.

Switches:
  --config FILE     read this ini file (also SYSLOG_CONFIG)
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
			if p.Reload {
				fmt.Fprintf(w, "  %-28s (reloadable with SIGHUP)\n", "")
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

	fmt.Fprintln(w, "# effective configuration of go-syslog-server")
	if l.File != "" {
		fmt.Fprintf(w, "# base file: %s\n", l.File)
	}
	fmt.Fprintln(w, "# values without a source comment are compiled-in defaults")
	for _, p := range l.Params {
		if secretKeys[p.Name()] && formatValue(p.Kind, p.Ptr) != "" {
			fmt.Fprintln(w, "# WARNING: secrets are shown as *** -- this dump cannot be used as a config file as-is")
			break
		}
	}
	fmt.Fprintln(w)

	for _, sec := range ix.sections() {
		fmt.Fprintf(w, "[%s]\n", sec)
		for _, p := range ix.inSection(sec) {
			name := p.Name()
			if src := l.Source[name]; src != FromDefault {
				fmt.Fprintf(w, "# from %s\n", src)
			}
			fmt.Fprintf(w, "%-30s = %s\n", p.Key, redact(name, formatValue(p.Kind, p.Ptr)))
		}
		fmt.Fprintln(w)
	}
	return nil
}

// Effective returns the flat name/value map used by the boot log line. Secrets
// are redacted here too.
func (l *Loaded) Effective() map[string]string {
	out := make(map[string]string, len(l.Params))
	for _, p := range l.Params {
		name := p.Name()
		out[name] = redact(name, formatValue(p.Kind, p.Ptr))
	}
	return out
}

// Values returns the flat name/value map WITHOUT redaction. It exists for
// comparing two snapshots -- a changed password has to register as a change --
// and must never be logged; Effective is the loggable form.
func (l *Loaded) Values() map[string]string {
	out := make(map[string]string, len(l.Params))
	for _, p := range l.Params {
		out[p.Name()] = formatValue(p.Kind, p.Ptr)
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
