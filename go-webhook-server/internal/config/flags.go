package config

import (
	"errors"
	"fmt"
	"strings"
)

// flagEntry is one assignment found on the command line.
type flagEntry struct {
	Name string
	Raw  string
}

// Meta holds the command-line switches that are not configuration parameters.
type Meta struct {
	ConfigFile string
	Help       bool
	Version    bool
	DumpConfig bool
}

// parseFlags accepts, for every registry parameter:
//
//	--sec.key=value
//	--sec.key value
//	--sec.key            (bool only; means true)
//
// A single leading dash is accepted as well. Unknown names are an error rather
// than being ignored, so a typo in a deploy script fails loudly at boot instead
// of silently running with a default.
func parseFlags(ix *index, args []string) ([]flagEntry, Meta, error) {
	var (
		out []flagEntry
		m   Meta
	)

	// loose names the flag just assigned an empty value or a bare "=": the
	// argument after it is most likely that flag's value, typed after a space
	// ("--sink.nats.password= x", "--sink.nats.password = x"), and it is never
	// printed -- not even as the name of an unknown flag, which is what such
	// a value starting with a dash looks like ("--sink.nats.password= -x").
	// after names the flag just assigned any value: a stray word after it may
	// be the tail of a value the shell split at a space
	// ("--sink.nats.password=$P"), and it is not printed either (SPEC §13.2).
	loose, after := "", ""
	looseErr := func(flag string) error {
		return fmt.Errorf("unexpected argument after --%s: a value goes right after the '=', "+
			"with no space", flag)
	}
	i := 0
	for i < len(args) {
		arg := args[i]
		i++

		if !strings.HasPrefix(arg, "-") || arg == "-" || arg == "--" {
			if loose != "" {
				return nil, m, looseErr(loose)
			}
			if after != "" {
				return nil, m, fmt.Errorf("unexpected argument after --%s: a value with a space goes "+
					"in quotes, and options are --name=value or --name value", after)
			}
			// Only what precedes a '=': "sink.nats.password=x" without its
			// dashes would otherwise print the password (SPEC §13.2).
			shown := arg
			if k, _, cut := strings.Cut(arg, "="); cut {
				shown = k + "=..."
			}
			return nil, m, fmt.Errorf("unexpected argument %q: options are --name=value or --name value", shown)
		}
		prev, prevAfter := loose, after
		loose, after = "", ""
		body := strings.TrimLeft(arg, "-")

		name, val, hasVal := strings.Cut(body, "=")
		assigned := func(v string) {
			after = name
			if v == "" || v == "=" {
				loose = name
			}
		}

		// takeValue consumes the next argument when the flag was written in the
		// separated form. A next argument that is itself a flag of this server
		// is refused, unquoted: "--sink.workers --sink.nats.password=x" -- a
		// $WORKERS that expanded to nothing -- would otherwise take the
		// password as the worker count and print it in the type error.
		takeValue := func() (string, error) {
			if hasVal {
				return val, nil
			}
			if i >= len(args) {
				return "", fmt.Errorf("flag --%s requires a value", name)
			}
			v := args[i]
			if next, ok := flagName(ix, v); ok {
				// Named only when it was written with two dashes: "-h" or
				// "-v" is also what a two-character value starting with a
				// dash looks like ("--sink.x.password -h"; ported from the
				// syslog/RELP family, 2026-10-13).
				if knownFlag(ix, next) && strings.HasPrefix(v, "--") {
					return "", fmt.Errorf("flag --%s requires a value; the next argument is the flag --%s",
						name, next)
				}
				// Not a flag of this server (or one written with one dash): a
				// typo, or a value that starts with a dash ("--sink.x.password
				// --x"). Either way it is not printed.
				return "", fmt.Errorf("flag --%s requires a value; the next argument starts with a dash "+
					"like a flag: a value like that goes right after the '='", name)
			}
			i++
			return v, nil
		}

		switch name {
		case "help", "h", "version", "v", "dump-config":
			if hasVal {
				// "--dump-config=false" dumped and exited 0: a switch takes
				// no value, and an ignored one may mean the opposite. The
				// value is not shown (ported from the syslog/RELP family,
				// 2026-10-13; SPEC §13.2).
				return nil, m, fmt.Errorf("flag --%s takes no value", name)
			}
			switch name {
			case "help", "h":
				m.Help = true
			case "version", "v":
				m.Version = true
			default:
				m.DumpConfig = true
			}
			continue
		case "config":
			v, err := takeValue()
			if err != nil {
				return nil, m, err
			}
			if v == "" {
				// An explicit empty --config fell back to WHS_CONFIG, the
				// opposite of what it says (ported from the syslog/RELP
				// family, 2026-10-13). The argument after it is not shown:
				// it may be the value typed after a space.
				return nil, m, errors.New("flag --config has an empty value: name the file right " +
					"after the '=', or leave --config out")
			}
			m.ConfigFile = v
			assigned(v)
			continue
		}

		p, err := ix.lookup(name)
		if err != nil {
			if prev != "" {
				// After an empty value, an unknown "-x" is that value.
				return nil, m, looseErr(prev)
			}
			if prevAfter != "" && !ix.knownSectionOf(name) {
				// After a value, an unknown "-x" may be the tail of that value
				// the shell split at a space ("--sink.x.password=abc
				// -hunter2", "... abc --x=y"): it is named only when it is a
				// typo inside a section this server has ("--sink.wrkers"),
				// never otherwise (SPEC §13.2; go-relplog-server's rule,
				// ported 2026-10-13).
				return nil, m, fmt.Errorf("unexpected argument after --%s: a value with a space goes "+
					"in quotes, and options are --name=value or --name value", prevAfter)
			}
			return nil, m, fmt.Errorf("%w (try --help)", err)
		}

		if !hasVal && p.Kind == KBool {
			// Bare --sec.key on a bool means true. Anything else must carry a
			// value: guessing would make "--ring.mode sqlite" ambiguous with a
			// positional argument.
			out = append(out, flagEntry{Name: p.Name(), Raw: "true"})
			continue
		}
		v, err := takeValue()
		if err != nil {
			return nil, m, err
		}
		assigned(v)
		out = append(out, flagEntry{Name: p.Name(), Raw: v})
	}
	return out, m, nil
}

// flagName reports whether arg is a flag rather than a value, and its name:
// anything spelled --name, known or not (a value never starts with two
// dashes, and an unknown flag swallowed as a value would print whatever
// follows its '='), or -name for a parameter of the registry or a switch.
func flagName(ix *index, arg string) (string, bool) {
	if !strings.HasPrefix(arg, "-") || arg == "-" || arg == "--" {
		return "", false
	}
	name, _, _ := strings.Cut(strings.TrimLeft(arg, "-"), "=")
	if strings.HasPrefix(arg, "--") && name != "" {
		return name, true
	}
	if knownFlag(ix, name) {
		return name, true
	}
	return "", false
}

// knownFlag reports whether name is a switch or a parameter of the registry:
// a name typed as a flag, safe to print. Any other name after a flag that
// wanted a value may be that value.
func knownFlag(ix *index, name string) bool {
	switch name {
	case "help", "h", "version", "v", "dump-config", "config":
		return true
	}
	_, err := ix.lookup(name)
	return err == nil
}
