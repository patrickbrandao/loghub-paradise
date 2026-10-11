// Package ident is the process identity, frozen at boot (SPEC §3.5, §10.1).
//
// The values come from github.com/patrickbrandao/go-loghub-ident, whose
// Initialize() runs exactly once, in main. Everything else receives an Identity
// by value instead of calling the library's getters, for two reasons:
//
//   - The tests assemble the application dozens of times in one process, and a
//     second Initialize() exits with code 112. An injected struct lets them run
//     with any identity they like.
//   - A getter called before Initialize returns "", which would produce an
//     envelope with an empty from. An Identity that was validated once cannot.
package ident

import (
	"fmt"

	lhident "github.com/patrickbrandao/go-loghub-ident"
)

// Identity is who this server is on the eventbus.
type Identity struct {
	// From is the AgentUUID: the envelope's from (SPEC §3.1).
	From string
	// Space is the Workspace: the envelope's space and part of every subject.
	Space string
	// AgentName and Hostname name the connections to the destination; MachineID
	// is kept for the log (SPEC §3.5).
	AgentName string
	Hostname  string
	MachineID string
}

// FromLibrary reads the identity resolved by lhident.Initialize, which main must
// have called already. It validates what the library does not: the Workspace
// grammar of the subject, and the exact form of from.
func FromLibrary() (Identity, error) {
	if !lhident.IsInitialized() {
		return Identity{}, fmt.Errorf("ident: go-loghub-ident was not initialized")
	}
	id := Identity{
		From:      lhident.AgentUUID(),
		Space:     lhident.Workspace(),
		AgentName: lhident.AgentName(),
		Hostname:  lhident.Hostname(),
		MachineID: lhident.MachineID(),
	}
	return id, id.Validate()
}

// Validate checks the two values that go into every envelope header, which the
// JLH writer copies without escaping.
func (id Identity) Validate() error {
	if !ValidFrom(id.From) {
		return fmt.Errorf("ident: AgentUUID %q is not a lowercase UUIDv7", id.From)
	}
	if err := ValidateSpace(id.Space); err != nil {
		return err
	}
	return nil
}

// ValidFrom reports whether s is a from the webhook schema accepts: a
// lowercase UUIDv7,
// ^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$. The
// generic JLH rule takes any version 1-8, but envelope.ParseHead reads back
// only what this server can write, so a v4 here would make every envelope of
// the instance unreadable from ring.db and from a dump.
func ValidFrom(s string) bool {
	if len(s) != 36 {
		return false
	}
	for i := 0; i < 36; i++ {
		c := s[i]
		switch i {
		case 8, 13, 18, 23:
			if c != '-' {
				return false
			}
			continue
		case 14:
			if c != '7' {
				return false
			}
			continue
		case 19:
			if c != '8' && c != '9' && c != 'a' && c != 'b' {
				return false
			}
			continue
		}
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}

// ValidateSpace applies the intersection of the LogHub rules (SPEC §3.0):
// the schema's ^[a-z0-9.-]+$ with at most 64 characters, and no empty segment.
//
// The library and the schema both accept "a..b", ".x" and "x.", but those make
// an empty token in loghub.{space}.{type}, which NATS rejects. Refusing them at
// boot is what the skill asks of every producer.
func ValidateSpace(s string) error {
	if len(s) == 0 || len(s) > 64 {
		return fmt.Errorf("ident: workspace %q must have 1 to 64 characters", s)
	}
	seg := 0
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'a' && c <= 'z', c >= '0' && c <= '9', c == '-':
			seg++
		case c == '.':
			if seg == 0 {
				return fmt.Errorf("ident: workspace %q has an empty segment", s)
			}
			seg = 0
		default:
			return fmt.Errorf("ident: workspace %q may only use [a-z0-9.-]", s)
		}
	}
	if seg == 0 {
		return fmt.Errorf("ident: workspace %q has an empty segment", s)
	}
	return nil
}
