package config

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"
)

// identitySection is the informative block of the dump (SPEC §16.2). It is
// never read as configuration.
const identitySection = "identity"

// iniEntry is one assignment found in an INI file, kept with its line number so
// errors can point at the offending line.
type iniEntry struct {
	Name string // canonical dotted name
	Raw  string
	Line int
}

// parseINI reads the minimal INI dialect this server accepts:
//
//	[section]        section header; may contain dots, e.g. [sink.postgres]
//	key = value      assignment; the first '=' separates
//	# comment        whole-line comment
//	; comment        whole-line comment
//
// A '#' or ';' inside a value is literal. Inline comments are deliberately not
// supported: passwords, CIDR lists and stream names may legitimately contain
// those characters, and silently truncating a password at a '#' is a far worse
// failure than not supporting a convenience.
//
// Values may be wrapped in matching single or double quotes, which are stripped;
// this is the only way to write a value with meaningful leading or trailing
// whitespace.
func parseINI(r io.Reader) ([]iniEntry, error) {
	var (
		out     []iniEntry
		section string
		lineNo  int
	)
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)

	for sc.Scan() {
		lineNo++
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, ";") {
			continue
		}

		if strings.HasPrefix(line, "[") {
			if !strings.HasSuffix(line, "]") {
				// The line is not quoted: "[sink.postgres hunter2" is a header
				// someone typed a value into (SPEC §13.2).
				return nil, fmt.Errorf("line %d: malformed section header (no closing ']')", lineNo)
			}
			section = strings.TrimSpace(line[1 : len(line)-1])
			if section == "" {
				return nil, fmt.Errorf("line %d: empty section name", lineNo)
			}
			// The dump shows the identity as an informative block (SPEC §16.2),
			// and someone will copy it into a real file. Ignoring it would let
			// them believe WORKSPACE came from here; the generic "unknown
			// parameter" would not say where it does come from.
			if strings.EqualFold(section, identitySection) {
				return nil, fmt.Errorf("line %d: [%s] is not configuration: the identity is "+
					"resolved by go-loghub-ident from the environment (AGENT_UUID, AGENT_NAME, "+
					"WORKSPACE, HOSTNAME, MACHINE_ID, MACHINE_ID_FILE)", lineNo, section)
			}
			continue
		}

		eq := strings.Index(line, "=")
		if eq < 0 {
			// The line is not quoted: "password: hunter2" is exactly the
			// kind of line that lands here, and the error goes to stderr
			// (SPEC §13.2).
			return nil, fmt.Errorf("line %d: expected 'key = value'", lineNo)
		}
		key := strings.TrimSpace(line[:eq])
		if key == "" {
			return nil, fmt.Errorf("line %d: empty key", lineNo)
		}
		if section == "" {
			return nil, fmt.Errorf("line %d: key %q appears before any [section]", lineNo, key)
		}
		out = append(out, iniEntry{
			Name: section + "." + key,
			Raw:  unquote(strings.TrimSpace(line[eq+1:])),
			Line: lineNo,
		})
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func unquote(s string) string {
	if len(s) >= 2 {
		if (s[0] == '"' && s[len(s)-1] == '"') || (s[0] == '\'' && s[len(s)-1] == '\'') {
			return s[1 : len(s)-1]
		}
	}
	return s
}

func parseINIFile(path string) ([]iniEntry, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	entries, err := parseINI(f)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return entries, nil
}
