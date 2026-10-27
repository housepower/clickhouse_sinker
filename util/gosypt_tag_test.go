package util

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// Fields tagged `gsypt:"-"` are passed through verbatim even when they hold an
// ENC(...) value: pass-through fields such as --http-headers must never be
// decrypted.
func TestGsyptSkipTag(t *testing.T) {
	const enc = "ENC(E310E892E56801CED9ED98AA177F18E6)" // "123456"

	cfg := struct {
		Password string
		Raw      string `gsypt:"-"`
	}{Password: enc, Raw: enc}
	require.NoError(t, Gsypt.Unmarshal(&cfg))
	require.Equal(t, "123456", cfg.Password)
	require.Equal(t, enc, cfg.Raw)
}
