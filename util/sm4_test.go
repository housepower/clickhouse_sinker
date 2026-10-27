package util

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSM4KeyFromHexEnv(t *testing.T) {
	t.Setenv(SM4KeyEnv, "6368656e79616e6368616e6702000205")
	key, err := sm4Key()
	require.NoError(t, err)
	require.Equal(t, []byte{'c', 'h', 'e', 'n', 'y', 'a', 'n', 'c', 'h', 'a', 'n', 'g', 0x02, 0x00, 0x02, 0x05}, key)
}

func TestSM4KeyRawEnv(t *testing.T) {
	t.Setenv(SM4KeyEnv, "1234567890abcdef")
	key, err := sm4Key()
	require.NoError(t, err)
	require.Equal(t, []byte("1234567890abcdef"), key)
}

func TestSM4KeyInvalid(t *testing.T) {
	t.Setenv(SM4KeyEnv, "wrong-length")
	_, err := sm4Key()
	require.Error(t, err)

	t.Setenv(SM4KeyEnv, "zz2345678930abcdefzz2345678930abcd") // 32 chars but not hex
	_, err = sm4Key()
	require.Error(t, err)

	t.Setenv(SM4KeyEnv, "")
	_, err = sm4Key()
	require.Error(t, err)
}

func TestSM4RoundTrip(t *testing.T) {
	t.Setenv(SM4KeyEnv, "1234567890abcdef")
	first, err := SM4Encrypt("secret-value")
	require.NoError(t, err)
	// random nonce: the same plaintext encrypts differently every time
	second, err := SM4Encrypt("secret-value")
	require.NoError(t, err)
	require.NotEqual(t, first, second)

	got, err := SM4Decrypt(first)
	require.NoError(t, err)
	require.Equal(t, "secret-value", got)

	_, err = SM4Decrypt("not-base64!!")
	require.Error(t, err)
}

func TestGsyptSM4(t *testing.T) {
	t.Setenv(SM4KeyEnv, "1234567890abcdef")
	ciphertext, err := SM4Encrypt("secret-value")
	require.NoError(t, err)

	g := &Gosypt{prefix: GosyptPrefixDefault, suffix: GosyptSuffxiDefault}
	require.NoError(t, g.SetAlgorithm(GosyptSM4Algorithm))
	cfg := struct{ Password string }{Password: "ENC(" + ciphertext + ")"}
	require.NoError(t, g.Unmarshal(&cfg))
	require.Equal(t, "secret-value", cfg.Password)

	cfg.Password = "ENC(corrupted)"
	require.Error(t, g.Unmarshal(&cfg))

	require.Error(t, g.SetAlgorithm("ROT13"))
}

// The default algorithm stays AES and old ciphertext keeps working.
func TestGsyptDefaultAlgorithmUnchanged(t *testing.T) {
	g := &Gosypt{prefix: GosyptPrefixDefault, suffix: GosyptSuffxiDefault}
	require.NoError(t, g.SetAlgorithm(""))
	cfg := struct{ Password string }{Password: "ENC(E310E892E56801CED9ED98AA177F18E6)"}
	require.NoError(t, g.Unmarshal(&cfg))
	require.Equal(t, "123456", cfg.Password)
}
