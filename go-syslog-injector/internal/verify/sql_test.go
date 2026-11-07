package verify

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// TestPostgresRunPresent (SPEC §8.5, review finding 4): a loghub.inbox that
// already holds rows of --run in the window refuses the run at Open. Opt-in:
//
//	SI_TEST_VERIFY_PG=postgres://postgres:pw@127.0.0.1:39502/postgres
func TestPostgresRunPresent(t *testing.T) {
	u := os.Getenv("SI_TEST_VERIFY_PG")
	if u == "" {
		t.Skip("SI_TEST_VERIFY_PG not set")
	}
	ctx := context.Background()
	c, err := pgx.Connect(ctx, u)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close(ctx)
	table := fmt.Sprintf("si_inbox_%d", time.Now().UnixNano())
	if _, err := c.Exec(ctx, `CREATE TABLE `+table+` (type text, space text, event_at timestamptz, payload jsonb)`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Exec(context.Background(), `DROP TABLE `+table) })
	insert := func(run string, seq int) {
		msg := fmt.Sprintf("seq=%d run=%s", seq, run)
		if _, err := c.Exec(ctx, `INSERT INTO `+table+` VALUES ('syslog', 'default', now(), jsonb_build_object('message', $1::text))`, msg); err != nil {
			t.Fatal(err)
		}
	}
	insert("used", 1)
	insert("usedX", 1)
	open := func(run string) error {
		tg, err := Parse(u, "", "default", "public."+table, run, "")
		if err != nil {
			t.Fatal(err)
		}
		tg.Since = time.Now().Add(-ClockSlack)
		chk, err := Open(ctx, tg)
		if err == nil {
			defer chk.Close()
			insert(run, 1)
			insert(run, 1)
			if v := chk.Wait(ctx, seqs(2), time.Second); v.Found != 1 || v.Lost != 1 || v.Duplicated != 1 || v.Error != "" {
				t.Fatalf("%+v", v)
			}
		}
		return err
	}
	if err := open("used"); !errors.Is(err, ErrRunPresent) {
		t.Fatalf("reused run: %v", err)
	}
	if err := open("use"); err != nil {
		t.Fatalf("a new run: %v", err)
	}
}
