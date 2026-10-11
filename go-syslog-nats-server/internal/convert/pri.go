package convert

// PRIState is how the PRI of a message was found (SPEC §6.2).
type PRIState uint8

const (
	PRIOk      PRIState = iota // a valid <PRIVAL>
	PRIMissing                 // the message does not start with '<'
	PRIInvalid                 // it starts with '<' but the PRI is not valid
)

// ScanPRI reads the PRI at the start of a message. It returns the PRIVAL (only
// meaningful with PRIOk), the state, and how many bytes the PRI occupies.
//
// It is the ONE PRI reader of the code base. The receive path no longer reads
// the PRI (there is no filter, SPEC §1.2); the converter does, here.
//
// PRI = "<" PRIVAL ">", PRIVAL = 1 to 3 digits, 0..191, no leading zero except
// <0> itself (RFC 3164 §4.1.1, RFC 5424 §6.2.1).
func ScanPRI(b []byte) (pri int, state PRIState, n int) {
	if len(b) == 0 || b[0] != '<' {
		return 0, PRIMissing, 0
	}
	i := 1
	for i < len(b) && i <= 3 && b[i] >= '0' && b[i] <= '9' {
		i++
	}
	digits := i - 1
	if digits == 0 || i >= len(b) || b[i] != '>' {
		return 0, PRIInvalid, 0
	}
	if digits > 1 && b[1] == '0' {
		return 0, PRIInvalid, 0
	}
	v := 0
	for _, c := range b[1:i] {
		v = v*10 + int(c-'0')
	}
	if v > 191 {
		return 0, PRIInvalid, 0
	}
	return v, PRIOk, i + 1
}
