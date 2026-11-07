package verify

import (
	"context"
	"errors"
	"fmt"

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

func openNATS(ctx context.Context, t *Target) (reader, error) {
	// The whole URL goes to nats.go, as the server's sink.nats.urls does:
	// a comma-separated list, tls:// turning TLS on, user and password in it.
	nc, err := nats.Connect(t.URL, nats.Name("go-syslog-injector verify"), nats.MaxReconnects(2))
	if err != nil {
		return nil, fmt.Errorf("nats: connect: %w", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("nats: jetstream: %w", err)
	}
	cfg := jetstream.OrderedConsumerConfig{DeliverPolicy: jetstream.DeliverByStartSequencePolicy}
	stream := t.Key
	if stream == "" {
		// subjects=exact publishes on loghub.<space>.syslog, and
		// facility-severity below it; the stream that holds either is the
		// one the server writes to.
		for _, subj := range []string{"loghub." + t.Space + ".syslog", "loghub." + t.Space + ".syslog.>"} {
			name, err := js.StreamNameBySubject(ctx, subj)
			if err == nil {
				stream, cfg.FilterSubjects = name, []string{subj}
				break
			}
			if !errors.Is(err, jetstream.ErrStreamNotFound) {
				nc.Close()
				return nil, fmt.Errorf("nats: stream lookup: %w", err)
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
		return nil, fmt.Errorf("nats: stream %s: %w", stream, err)
	}
	cons, err := js.OrderedConsumer(ctx, stream, cfg)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("nats: consumer on stream %s: %w", stream, err)
	}
	return &natsReader{t: t, nc: nc, cons: cons}, nil
}

func (r *natsReader) poll(ctx context.Context, c *Counter) error {
	for ctx.Err() == nil {
		// Fetch, not FetchNoWait: only Fetch moves the ordered consumer's
		// cursor, and FetchNoWait reread the stream from the start at every
		// call (found against a real server, 2026-11-07).
		batch, err := r.cons.Fetch(1000, jetstream.FetchMaxWait(pollEvery))
		if err != nil {
			return fmt.Errorf("nats: fetch: %w", err)
		}
		n := 0
		for m := range batch.Messages() {
			n++
			if md, err := m.Metadata(); err == nil {
				if md.Sequence.Stream <= r.last {
					continue
				}
				r.last = md.Sequence.Stream
			}
			countJLH(m.Data(), r.t, c)
		}
		if err := batch.Error(); err != nil && !errors.Is(err, nats.ErrTimeout) && !errors.Is(err, jetstream.ErrNoMessages) {
			return fmt.Errorf("nats: fetch: %w", err)
		}
		if n == 0 {
			return nil
		}
	}
	return ctx.Err()
}

func (r *natsReader) final() error { return nil }

func (r *natsReader) close() {
	r.nc.Close()
}
