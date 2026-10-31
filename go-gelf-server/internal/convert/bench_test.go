package convert

import (
	"os"
	"testing"
)

func BenchmarkConvertDocker(b *testing.B) {
	doc, _ := os.ReadFile("testdata/corpus/docker-gelf-driver.gelf")
	c := New(DefaultConfig())
	h := udpHead()
	var dst []byte
	b.SetBytes(int64(len(doc)))
	b.ReportAllocs()
	for b.Loop() {
		dst, _ = c.Append(dst[:0], &h, doc)
	}
}

// BenchmarkLevelOf is what the reception pays per document with a filter on
// (SPEC §7).
func BenchmarkLevelOf(b *testing.B) {
	doc, _ := os.ReadFile("testdata/corpus/docker-gelf-driver.gelf")
	c := New(DefaultConfig())
	b.SetBytes(int64(len(doc)))
	b.ReportAllocs()
	for b.Loop() {
		c.LevelOf(doc)
	}
}
