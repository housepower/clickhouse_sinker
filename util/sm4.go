package util

import (
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"os"

	"github.com/tjfoc/gmsm/sm4"
)

const SM4KeyEnv = "SINKER_SM4_KEY"

func sm4Key() ([]byte, error) {
	value := os.Getenv(SM4KeyEnv)
	if len(value) == 32 {
		key, err := hex.DecodeString(value)
		if err != nil {
			return nil, fmt.Errorf("%s must be valid hexadecimal: %w", SM4KeyEnv, err)
		}
		return key, nil
	}
	if len(value) == 16 {
		return []byte(value), nil
	}
	return nil, fmt.Errorf("%s must be 16 raw bytes or 32 hexadecimal characters", SM4KeyEnv)
}

func SM4Encrypt(data string) (string, error) {
	if data == "" {
		return "", nil
	}
	key, err := sm4Key()
	if err != nil {
		return "", err
	}
	block, err := sm4.NewCipher(key)
	if err != nil {
		return "", err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err = rand.Read(nonce); err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(gcm.Seal(nonce, nonce, []byte(data), nil)), nil
}

func SM4Decrypt(data string) (string, error) {
	if data == "" {
		return "", nil
	}
	key, err := sm4Key()
	if err != nil {
		return "", err
	}
	block, err := sm4.NewCipher(key)
	if err != nil {
		return "", err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}
	decoded, err := base64.StdEncoding.DecodeString(data)
	if err != nil {
		return "", err
	}
	if len(decoded) < gcm.NonceSize() {
		return "", fmt.Errorf("SM4 ciphertext is too short")
	}
	plain, err := gcm.Open(nil, decoded[:gcm.NonceSize()], decoded[gcm.NonceSize():], nil)
	if err != nil {
		return "", err
	}
	return string(plain), nil
}
