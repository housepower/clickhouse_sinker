package util

import (
	"crypto/aes"
	"encoding/hex"
	"strings"
)

var salt = "656f6974656b"

// select hex(aes_encrypt("123456", unhex("656f6974656b"))); => E310E892E56801CED9ED98AA177F18E6
func AesEncryptECB(origData string) string {
	if origData == "" {
		return origData
	}
	var encrypted []byte
	var o = []byte(origData)
	s, _ := hex.DecodeString(salt)
	cipher, _ := aes.NewCipher(generateKey(s))
	length := (len(o) + aes.BlockSize) / aes.BlockSize
	plain := make([]byte, length*aes.BlockSize)
	copy(plain, o)
	pad := byte(len(plain) - len(o))
	for i := len(o); i < len(plain); i++ {
		plain[i] = pad
	}
	encrypted = make([]byte, len(plain))
	for bs, be := 0, cipher.BlockSize(); bs <= len(o); bs, be = bs+cipher.BlockSize(), be+cipher.BlockSize() {
		cipher.Encrypt(encrypted[bs:be], plain[bs:be])
	}
	return strings.ToUpper(hex.EncodeToString(encrypted))
}

// select aes_decrypt(unhex("E310E892E56801CED9ED98AA177F18E6"), unhex("656f6974656b")); => 123456
func AesDecryptECB(encrypted string) string {
	if encrypted == "" {
		return encrypted
	}
	var decrypted []byte
	h, _ := hex.DecodeString(encrypted)
	s, _ := hex.DecodeString(salt)
	cipher, _ := aes.NewCipher(generateKey(s))
	decrypted = make([]byte, len(h))

	for bs, be := 0, cipher.BlockSize(); bs < len(h); bs, be = bs+cipher.BlockSize(), be+cipher.BlockSize() {
		cipher.Decrypt(decrypted[bs:be], h[bs:be])
	}

	bEnd := searchByteSliceIndex(decrypted, 32)
	return string(decrypted[:bEnd])
}
func generateKey(key []byte) (genKey []byte) {
	genKey = make([]byte, 16)
	copy(genKey, key)
	for i := 16; i < len(key); {
		for j := 0; j < 16 && i < len(key); j, i = j+1, i+1 {
			genKey[j] ^= key[i]
		}
	}
	return genKey
}

func searchByteSliceIndex(bSrc []byte, b byte) int {
	for i := 0; i < len(bSrc); i++ {
		if bSrc[i] < b {
			return i
		}
	}

	return len(bSrc)
}
