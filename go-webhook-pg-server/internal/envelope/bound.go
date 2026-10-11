package envelope

// MaxEventBytes is the largest envelope one accepted request can produce
// (SPEC §3.2), computed from the three ceilings the handler enforces before it
// serialises anything:
//
//	4 × ⌈B / 3⌉ + 14 × H + 13 × U + 8 KiB
//
// B is server.max_body_bytes: the body never weighs more than its base64,
// because PlanBody picks base64 whenever the escape would be larger. H is
// server.max_header_bytes, which bounds the whole head -- request line
// included: a header byte at most triples in the escape (obs-text becomes a
// three-byte U+FFFD), appears at most twice (in headers and in the field
// derived from it: host, content.type, fp*, the host of the url), and every
// header line adds the 23 bytes of {"name":"","value":""}, for a line of at
// least 5 counted bytes. U is server.max_uri_bytes, which appears in path, url
// and the decoded query, where a two-byte "a&" grows to `"a":"",`. The fixed
// 8 KiB is the header, the keys, the network object and the addresses.
//
// It is the ceiling of one line of the emergency dump (SPEC §9.2) and enters
// the batch warning of SPEC §8.5.4. TestWorstCaseEnvelopeIsBounded proves it
// with adversarial requests.
func MaxEventBytes(maxBody, maxHeader, maxURI int64) int64 {
	return 4*((maxBody+2)/3) + 14*maxHeader + 13*maxURI + 8<<10
}
