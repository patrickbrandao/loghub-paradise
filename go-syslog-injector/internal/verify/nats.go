package verify

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// natsReader reads the JetStream stream the server publishes to (SPEC §8.5)
// with an ordered consumer: ephemeral, it leaves nothing on the server, and
// it is incremental -- each poll reads only what arrived since the last.
type natsReader struct {
	t    *Target
	nc   *nats.Conn
	cons jetstream.Consumer
	// last is the stream sequence of the last message counted: a reset of
	// the ordered consumer (a reconnection) may deliver from before it.
	last uint64
}

// natsSubjects is the consumer's filter: subjects=exact publishes on
// loghub.<space>.syslog, facility-severity below it. Both, always: the
// server's stream covers both, and filtering on the one that found the stream
// lost every message of facility-severity (SPEC §8.5).
func natsSubjects(space string) []string {
	return []string{"loghub." + space + ".syslog", "loghub." + space + ".syslog.>"}
}

func openNATS(ctx context.Context, t *Target) (reader, error) {
	// The list goes to nats.go as the server's sink.nats.urls does; every
	// element was parsed by Parse already, so nats.go has no parse error to
	// quote (SPEC §8.5).
	opts := []nats.Option{nats.Name("go-syslog-injector verify"), nats.MaxReconnects(2)}
	if t.TLSRoots != nil {
		opts = append(opts, nats.Secure(&tls.Config{RootCAs: t.TLSRoots, MinVersion: tls.VersionTLS12}))
	}
	nc, err := nats.Connect(strings.ReplaceAll(t.URL, " ", ""), opts...)
	if err != nil {
		return nil, t.clean("nats: connect", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, t.clean("nats: jetstream", err)
	}
	cfg := jetstream.OrderedConsumerConfig{DeliverPolicy: jetstream.DeliverByStartSequencePolicy, FilterSubjects: natsSubjects(t.Space)}
	stream := t.Key
	if stream == "" {
		// The lookup only finds the stream; the filter is both subjects.
		for _, subj := range cfg.FilterSubjects {
			name, err := js.StreamNameBySubject(ctx, subj)
			if err == nil {
				stream = name
				break
			}
			if !errors.Is(err, jetstream.ErrStreamNotFound) {
				nc.Close()
				return nil, t.clean("nats: stream lookup", err)
			}
		}
		if stream == "" {
			nc.Close()
			return nil, fmt.Errorf("nats: no JetStream stream holds loghub.%s.syslog (sink.nats.mode=core has none; or give --verify-key)", t.Space)
		}
	}
	// Open runs before the first send: everything of the run lands after
	// the stream's last sequence now. Starting there reads nothing of the
	// past and needs no clock (SPEC §8.5; the start time minus ClockSlack
	// re-read every message of the last minutes of a busy stream --
	// go-gelf-injector found ~250 000 of them, past --verify-timeout).
	st, err := js.Stream(ctx, stream)
	if err == nil {
		var info *jetstream.StreamInfo
		if info, err = st.Info(ctx); err == nil {
			cfg.OptStartSeq = info.State.LastSeq + 1
		}
	}
	if err != nil {
		nc.Close()
		return nil, t.clean("nats: stream "+stream, err)
	}
	cons, err := js.OrderedConsumer(ctx, stream, cfg)
	if err != nil {
		nc.Close()
		return nil, t.clean("nats: consumer on stream "+stream, err)
	}
	return &natsReader{t: t, nc: nc, cons: cons}, nil
}

func (r *natsReader) full() bool { return false }

func (r *natsReader) poll(ctx context.Context, c *Counter) error {
	until := time.Now().Add(readSlice)
	for ctx.Err() == nil {
		// Fetch, not FetchNoWait: only Fetch moves the ordered consumer's
		// cursor, and FetchNoWait reread the stream from the start at every
		// call (found against a real server, 2026-11-07).
		batch, err := r.cons.Fetch(1000, jetstream.FetchMaxWait(pollEvery))
		if err != nil {
			return r.t.clean("nats: fetch", err)
		}
		n := 0
		pending := uint64(1)
		for m := range batch.Messages() {
			n++
			if md, err := m.Metadata(); err == nil {
				pending = md.NumPending
				if md.Sequence.Stream <= r.last {
					continue
				}
				r.last = md.Sequence.Stream
			}
			countJLH(m.Data(), r.t, c)
		}
		if err := batch.Error(); err != nil && !errors.Is(err, nats.ErrTimeout) && !errors.Is(err, jetstream.ErrNoMessages) && !errors.Is(err, context.DeadlineExceeded) {
			return r.t.clean("nats: fetch", err)
		}
		// Caught up: an empty batch, or the last message says nothing is
		// left on the filter. Under constant foreign traffic neither may
		// come, so a read also ends after readSlice; the next poll goes on
		// from there (SPEC §8.5).
		if n == 0 || pending == 0 || !time.Now().Before(until) {
			return nil
		}
	}
	return ctx.Err()
}

func (r *natsReader) close() {
	r.nc.Close()
}
