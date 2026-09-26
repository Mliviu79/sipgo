package sip

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestMain(m *testing.M) {
	SIPDebug = os.Getenv("SIP_DEBUG") == "true"
	TransactionFSMDebug = os.Getenv("TRANSACTION_DEBUG") == "true"

	var lvl slog.Level
	if err := lvl.UnmarshalText([]byte(os.Getenv("LOG_LEVEL"))); err != nil {
		lvl = slog.LevelInfo
	}
	slog.SetLogLoggerLevel(lvl)

	timers := loadTimers()
	code := m.Run()
	if got := loadTimers(); got != timers {
		fmt.Fprintf(os.Stderr, "the tests left the SIP timers changed: %+v, want %+v\n", got, timers)
		code = 1
	}
	os.Exit(code)
}

// testTimers are the package-wide timers, which transactions read from their
// own goroutines.
type testTimers struct {
	t1, t2, t4                               time.Duration
	a, b, d, e, f, g, h, i, j, k, l, m, t1xx time.Duration
}

func loadTimers() testTimers {
	return testTimers{
		t1: T1, t2: T2, t4: T4,
		a: Timer_A, b: Timer_B, d: Timer_D, e: Timer_E,
		f: Timer_F, g: Timer_G, h: Timer_H, i: Timer_I,
		j: Timer_J, k: Timer_K, l: Timer_L, m: Timer_M,
		t1xx: Timer_1xx,
	}
}

func (s testTimers) store() {
	T1, T2, T4 = s.t1, s.t2, s.t4
	Timer_A, Timer_B, Timer_D, Timer_E = s.a, s.b, s.d, s.e
	Timer_F, Timer_G, Timer_H, Timer_I = s.f, s.g, s.h, s.i
	Timer_J, Timer_K, Timer_L, Timer_M = s.j, s.k, s.l, s.m
	Timer_1xx = s.t1xx
}

// restoreTimers has the package-wide timers restored once t has finished and
// has ended the transactions it started. A test that changes them calls it
// first, and runs alone in a child process (runInChildProcess), where no
// transaction of another test reads them.
func restoreTimers(t *testing.T) {
	saved := loadTimers()
	t.Cleanup(saved.store)
}

// runInChildProcess runs the calling test again, alone, in a child process of
// the test binary, and reports true once it has passed there, so the caller
// returns. In the child it reports false and the caller runs its body.
func runInChildProcess(t *testing.T) bool {
	t.Helper()
	if os.Getenv("SIP_TEST_CHILD") == t.Name() {
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0],
		"-test.run=^"+t.Name()+"$",
		"-test.count=1",
		"-test.v",
		"-test.timeout=60s",
	)
	cmd.Env = append(os.Environ(), "SIP_TEST_CHILD="+t.Name())
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "child process failed:\n%s", out)
	return true
}

func BenchmarkGenerateBranch(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		val := GenerateBranch()
		if len(val) != 16+len(RFC3261BranchMagicCookie)+1 {
			b.Fatal("wrong number of bytes: " + val)
		}
	}
}

func BenchmarkGenerateBranch16(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		val := GenerateBranchN(16)
		if len(val) != 16+len(RFC3261BranchMagicCookie)+1 {
			b.Fatal("wrong number of bytes: " + val)
		}
	}
}

func BenchmarkGenerateBranchBufPool(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		val := GenerateBranchN(16)
		if len(val) != 32+len(RFC3261BranchMagicCookie)+1 {
			b.Fatal("wrong number of bytes")
		}
	}
}
