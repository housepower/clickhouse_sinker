package utils

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

var URL_ENCRYPT_KEY = []byte(`ec41effad5e14bad890bd0525e939048`)

const (
	URL_DECRYPT_TIMEOUT = 10
)

func Base64UrlEncode(src string) (dest string) {
	return base64.URLEncoding.EncodeToString([]byte(src))
}

func Base64UrlDecoder(src string) (dest []byte, err error) {
	var miss = (4 - len(src)%4) % 4
	for i := 0; i < miss; i++ {
		src += "="
	}
	return base64.URLEncoding.DecodeString(src)
}

func AesEncoder(key []byte, src string) (dest string, err error) {
	if len(src) < aes.BlockSize {
		err = fmt.Errorf("param src too short : %d :%s", len(src), string(src))
		return
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return
	}
	var buf = make([]byte, len(key))
	block.Encrypt(buf, []byte(src))
	dest = string(buf)
	return
}

func AesCBCDecoder(key, iv, src []byte) (dest string, err error) {
	if len(src) < aes.BlockSize {
		err = fmt.Errorf("param src too short : %d :%s", len(src), string(src))
		return
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return
	}

	mode := cipher.NewCBCDecrypter(block, iv)
	buf := make([]byte, len(src))
	mode.CryptBlocks(buf, src)

	dest = string(buf)
	return
}

func AesCBCEncoder(key, iv []byte, src string) (dest []byte, err error) {
	if len(src) < aes.BlockSize {
		err = fmt.Errorf("param src too short : %d :%s", len(src), string(src))
		return
	}
	if len(src)%aes.BlockSize != 0 {
		err = errors.New("src is not a multiple of the block size")
		return
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return
	}

	mode := cipher.NewCBCEncrypter(block, iv)
	dest = make([]byte, len(src))
	mode.CryptBlocks(dest, []byte(src))
	return
}

func AesDecoder(key, src []byte) (dest []byte, err error) {
	if len(src) < aes.BlockSize {
		err = fmt.Errorf("param src too short : %d :%s", len(src), string(src))
		return
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return
	}
	dest = make([]byte, len(src))
	block.Decrypt(dest, src)
	return
}

func HmacEncoder(key, src []byte) []byte {
	h := hmac.New(sha1.New, key)
	h.Write(src)
	return h.Sum(nil)
}

func HmacSha256Encoder(key, src []byte) []byte {
	h := hmac.New(sha256.New, key)
	h.Write(src)
	return h.Sum(nil)
}

func UrlDecryptWithTime(src string) (dest string, err error) {
	dest, err = UrlDecrypt(src)

	pos := strings.Index(dest, "_")
	if pos == -1 || pos+1 == len(dest) {
		return "", errors.New("bad encrypt buf with string")
	}
	queryTime, err := strconv.ParseInt(dest[:pos], 10, 64)
	if err != nil {
		return
	}
	if queryTime > Now().Unix() || Now().Unix()-queryTime > URL_DECRYPT_TIMEOUT {
		return "", fmt.Errorf("url decrypt timeout : now(%d), url_time(%d), timeout(%d)", Now().Unix(), queryTime, URL_DECRYPT_TIMEOUT)
	}

	dest = dest[pos+1:]
	return
}

func UrlEncrypt(src string) (dest string, err error) {
	query, err := AesEncoder(URL_ENCRYPT_KEY, src)
	if err != nil {
		return
	}

	dest = Base64UrlEncode(query)
	return
}

func UrlDecrypt(src string) (dest string, err error) {
	query, err := Base64UrlDecoder(src)
	if err != nil {
		return
	}

	tmp, err := AesDecoder(URL_ENCRYPT_KEY, query)
	if err != nil {
		return
	}

	dest = string(tmp)
	for p, b := range dest {
		if (b < '0' || b > '9') && b != '.' {
			dest = dest[0:p]
			break
		}
	}
	dest = strings.TrimRight(dest, "_")

	return
}
