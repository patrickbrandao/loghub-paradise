package config

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"
)

// iniEntry is one assignment found in an INI file, kept with its line number so
// errors can point at the offending line.
type iniEntry struct {
	Name string // canonical dotted name
	Sec  string // the [section] it was under
	Key  string // what came before the '=', trimmed
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
		text := sc.Text()
		if lineNo == 1 {
			// A UTF-8 byte-order mark, which some Windows editors write, is
			// not part of the first line: kept, it made the line "expected
			// 'key = value'" with nothing visible to say why (ported from
			// the syslog/RELP family, 2026-10-13).
			text = strings.TrimPrefix(text, "\ufeff")
		}
		line := strings.TrimSpace(text)
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, ";") {
			continue
		}

		if strings.HasPrefix(line, "[") {
			if !strings.HasSuffix(line, "]") {
				return nil, fmt.Errorf("line %d: malformed section header (no closing ']')", lineNo)
			}
			section = strings.TrimSpace(line[1 : len(line)-1])
			if section == "" {
				return nil, fmt.Errorf("line %d: empty section name", lineNo)
			}
			continue
		}

		eq := strings.Index(line, "=")
		if eq < 0 {
			// The line is not quoted: "password: hunter2" is exactly the
			// kind of line that lands here, at boot and on every SIGHUP.
			return nil, fmt.Errorf("line %d: expected 'key = value'", lineNo)
		}
		key := strings.TrimSpace(line[:eq])
		if key == "" {
			return nil, fmt.Errorf("line %d: empty key", lineNo)
		}
		if section == "" {
			// Named only when it looks like a key, for the reason of
			// lookupINI: "password: hunter2 = x" puts the value in it.
			if !iniKeyRE.MatchString(key) {
				return nil, fmt.Errorf("line %d: a key appears before any [section]", lineNo)
			}
			return nil, fmt.Errorf("line %d: key %q appears before any [section]", lineNo, key)
		}
		out = append(out, iniEntry{
			Name: section + "." + key,
			Sec:  section,
			Key:  key,
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
