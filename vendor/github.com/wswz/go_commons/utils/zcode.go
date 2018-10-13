package utils

import (
	"strconv"

	"bytes"
)

var (
	Tokens    = make([]byte, 62)
	TokenDict = make(map[byte]int)
)

//Encode str => 0-9a-zA-Z
func init() {
	j := 0
	// 0-9
	for i := 0; i <= 9; i++ {
		Tokens[j] = byte(i) + byte('0')
		TokenDict[Tokens[j]] = j
		j++
	}

	//a-z
	for i := 0; i < 26; i++ {
		Tokens[j] = (byte('a') + byte(i))
		TokenDict[Tokens[j]] = j
		j++
	}
	//A-Z
	for i := 0; i < 26; i++ {
		Tokens[j] += (byte('A') + byte(i))
		TokenDict[Tokens[j]] = j
		j++
	}
}

func ZcodeStr(str string) string {
	code := MD5(str)
	id, _ := strconv.ParseInt(code[1:15], 16, 64)
	return ZcodeInt(id)
}

func ZcodeInt(id int64) string {
	var buffer bytes.Buffer
	for id > 0 {
		d := id % 62
		// println("write=>", string(Tokens[d]))
		buffer.WriteByte(Tokens[d])
		id /= 62
	}
	return buffer.String()
}
