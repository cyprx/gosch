package schedule

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNewSchedulerRejectsInvalidOptions(t *testing.T) {
	for _, tc := range []struct {
		name   string
		option Option
	}{
		{"zero concurrency", WithConcurrency(0)},
		{"negative concurrency", WithConcurrency(-1)},
		{"zero scan interval", WithScanInterval(0)},
		{"negative scan interval", WithScanInterval(-time.Second)},
		{"zero lock TTL", WithLockTTL(0)},
		{"negative lock TTL", WithLockTTL(-time.Second)},
		{"submillisecond lock TTL", WithLockTTL(time.Nanosecond)},
		{"nil option", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sch, err := NewScheduler("options", nil, tc.option)
			require.Error(t, err)
			require.Nil(t, sch, "invalid options must not construct a scheduler")
		})
	}
}

func TestNewSchedulerAcceptsValidOptions(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts []Option
	}{
		{"defaults", nil},
		{"positive options", []Option{WithConcurrency(1), WithScanInterval(time.Millisecond), WithLockTTL(time.Millisecond)}},
		{"last option wins", []Option{WithConcurrency(0), WithConcurrency(1)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sch, err := NewScheduler("options", nil, tc.opts...)
			require.NoError(t, err)
			require.NotNil(t, sch)
		})
	}
}
