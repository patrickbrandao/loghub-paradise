package config

import (
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

	i := 0
	for i < len(args) {
		arg := args[i]
		i++

		if !strings.HasPrefix(arg, "-") || arg == "-" || arg == "--" {
			return nil, m, fmt.Errorf("unexpected argument %q: this server takes only --name=value options", arg)
		}
		body := strings.TrimLeft(arg, "-")

		name, val, hasVal := strings.Cut(body, "=")

		// takeValue consumes the next argument when the flag was written in the
		// separated form.
		takeValue := func() (string, error) {
			if hasVal {
				return val, nil
			}
			if i >= len(args) {
				return "", fmt.Errorf("flag --%s requires a value", name)
			}
			v := args[i]
			i++
			return v, nil
		}

		switch name {
		case "help", "h":
			m.Help = true
			continue
		case "version", "v":
			m.Version = true
			continue
		case "dump-config":
			m.DumpConfig = true
			continue
		case "config":
			v, err := takeValue()
			if err != nil {
				return nil, m, err
			}
			m.ConfigFile = v
			continue
		}

		p, err := ix.lookup(name)
		if err != nil {
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
		out = append(out, flagEntry{Name: p.Name(), Raw: v})
	}
	return out, m, nil
}
