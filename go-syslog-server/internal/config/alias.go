package config

import (
	"fmt"
	"strings"
)

// The valkey aliases (SPEC §16.2.1, decision of 2026-11-07). Valkey speaks the
// Redis protocol, and an operator who runs one writes "valkey" where this
// server says "redis": in sink.type, in ring.mode, in the section names and in
// a LOGHUB_EB_URL (valkey://, valkeys://, see eburl.go). Each alias is the
// exact same parameter under a second name; the canonical name stays
// "redis", which names the protocol, and it is the only one the dump, the
// logs and the metrics show.
//
// The file is identical in go-syslog-server, go-relplog-server and
// go-gelf-server (the sibling rule): only the environment prefix differs, and
// it is derived from Param.Env(), not written here.

// sectionAliases maps an alias section onto its canonical one. Every key of
// the canonical section exists under the alias: [sink.valkey] addrs,
// --sink.valkey.addrs, <PREFIX>_SINK_VALKEY_ADDRS.
var sectionAliases = map[string]string{
	"sink.valkey": "sink.redis",
	"ring.valkey": "ring.redis",
}

// valueAliases maps, per enum parameter, an alias value onto the canonical
// one. It is applied before the enum is matched, so the stored value -- and
// every comparison, log line and metric label made from it -- is canonical.
var valueAliases = map[string]map[string]string{
	"sink.type": {"valkey": SinkTypeRedis},
	"ring.mode": {"valkey": RingModeRedis},
}

// canonicalValue returns the canonical spelling of an alias value of p, or raw
// unchanged.
func canonicalValue(p *Param, raw string) string {
	if m := valueAliases[p.Name()]; m != nil {
		if v, ok := m[strings.ToLower(strings.TrimSpace(raw))]; ok {
			return v
		}
	}
	return raw
}

// aliasSectionOf returns the alias section of a canonical section, or "".
func aliasSectionOf(sec string) string {
	for a, c := range sectionAliases {
		if c == sec {
			return a
		}
	}
	return ""
}

// aliasName is the alias spelling of p's dotted name ("sink.valkey.addrs"), or
// "" when p's section has no alias.
func aliasName(p *Param) string {
	if a := aliasSectionOf(p.Sec); a != "" {
		return a + "." + p.Key
	}
	return ""
}

// aliasEnv is the environment name of p under its alias section
// (SYSLOG_SINK_VALKEY_ADDRS), or "" when p has no alias. It is p.Env() with
// the section part swapped, so the prefix is whatever this server uses.
func aliasEnv(p *Param) string {
	a := aliasSectionOf(p.Sec)
	if a == "" || p.EnvName != "" {
		return ""
	}
	up := func(s string) string { return "_" + strings.ToUpper(strings.ReplaceAll(s, ".", "_")) + "_" }
	return strings.Replace(p.Env(), up(p.Sec), up(a), 1)
}

// aliasGuard enforces the conflict rule inside ONE source (SPEC §16.2.1): the
// same parameter given under both of its names is accepted when the values
// agree and refused, exit 1, when they differ. The message names the two
// spellings and where each came from, never the values: either may be a
// password. Between sources the precedence of SPEC §16.1 is unchanged; the
// alias counts as the same key in its source.
//
// What is compared is the LAST value of each spelling in the source, once,
// when the source is done (check): inside a source the last occurrence of a
// spelling wins, as it does for any key, so `--sink.valkey.db=1
// --sink.redis.db=2 --sink.valkey.db=2` agrees on 2 and passes. Comparing at
// every occurrence refused it, over a value the source itself had already
// replaced (independent review of 2026-11-07). The same holds for repeated
// INI sections; in the environment each name appears once.
type aliasGuard struct {
	// last: canonical name -> spelling -> {formatted value, origin} of the
	// last occurrence of that spelling.
	last map[string]map[string][2]string
	// order keeps the error deterministic: the canonical names in the order
	// they were first seen, and per name the spellings likewise.
	order    []string
	spelling map[string][]string
}

func newAliasGuard() *aliasGuard {
	return &aliasGuard{last: map[string]map[string][2]string{}, spelling: map[string][]string{}}
}

// note records that spelling set p (already parsed into p.Ptr) from origin.
func (g *aliasGuard) note(p *Param, spelling, origin string) {
	if aliasName(p) == "" {
		return
	}
	name := p.Name()
	m := g.last[name]
	if m == nil {
		m = map[string][2]string{}
		g.last[name] = m
		g.order = append(g.order, name)
	}
	if _, ok := m[spelling]; !ok {
		g.spelling[name] = append(g.spelling[name], spelling)
	}
	m[spelling] = [2]string{formatValue(p), origin}
}

// check compares, per parameter, the last value of each of its spellings.
func (g *aliasGuard) check() error {
	for _, name := range g.order {
		sp := g.spelling[name]
		m := g.last[name]
		for _, other := range sp[1:] {
			first, prev := sp[0], m[sp[0]]
			if cur := m[other]; cur[0] != prev[0] {
				return fmt.Errorf("%s (%s) and %s (%s) are the same parameter under its redis and valkey "+
					"names, set to different values; give it once (SPEC §16.2.1)",
					first, prev[1], other, cur[1])
			}
		}
	}
	return nil
}
