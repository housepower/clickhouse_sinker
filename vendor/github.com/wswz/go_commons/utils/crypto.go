package utils

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"math/rand"
	"sort"
	"strings"
)

//WARN:Do not edit this function
func EncodePasswd(str string) string {
	t := md5.New()
	io.WriteString(t, str)
	io.WriteString(t, `这里的每一句话都是加密的盐,我也不知道写什么好,随便写点吧!!`)
	return hex.EncodeToString(t.Sum(nil))
}

func Md5(sign string) []byte {
	h := md5.New()
	h.Write([]byte(sign))
	return h.Sum(nil)
}

func Md5To32(sign string) string {
	return fmt.Sprintf("%x", Md5(sign))
}

func GetWechatSign(values map[string]string, key string) string {
	keys := []string{}
	for k, _ := range values {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	res := ""
	for _, k := range keys {
		res += "&" + k + values[k]
	}
	res = strings.Trim(res, "&")
	res += "&key=" + key
	res = strings.ToUpper(Md5To32(res))
	return res
}

func UUID() string {
	uuid := make([]byte, 36)
	for i := 8; i <= 23; i += 5 {
		uuid[i] = '-'
	}
	uuid[14] = '4'

	for i := 0; i < 36; i++ {
		if uuid[i] == 0 {
			r := rand.Intn(16)
			if i == 19 {
				uuid[i] = byte(allchars[(r&0x3)|0x8])
			} else {
				uuid[i] = byte(allchars[r])
			}
		}
	}
	return string(uuid)
}
