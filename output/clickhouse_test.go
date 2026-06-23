package output

import (
	"context"
	"testing"
	"time"
)

func TestSleepWithCtxCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if sleepWithCtx(ctx, time.Hour) {
		t.Fatal("sleepWithCtx should return false when ctx already canceled")
	}
}

func TestSleepWithCtxElapses(t *testing.T) {
	if !sleepWithCtx(context.Background(), 10*time.Millisecond) {
		t.Fatal("sleepWithCtx should return true when timer elapses")
	}
}
