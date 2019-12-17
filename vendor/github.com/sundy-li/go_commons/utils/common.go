package utils

import (
	"strings"
	"unsafe"
)

// Bytes2String直接转换底层指针，两者指向的相同的内存，改一个另外一个也会变。
// 效率是string([]byte{})的百倍以上，且转换量越大效率优势越明显。
func Bytes2String(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

// String2Bytes直接转换底层指针，两者指向的相同的内存，改一个另外一个也会变。
// 效率是string([]byte{})的百倍以上，且转换量越大效率优势越明显。
// 转换之后若没做其他操作直接改变里面的字符，则程序会崩溃。
// 如 b:=String2bytes("xxx"); b[1]='d'; 程序将panic。
func String2Bytes(s string) []byte {
	x := (*[2]uintptr)(unsafe.Pointer(&s))
	h := [3]uintptr{x[0], x[1], x[1]}
	return *(*[]byte)(unsafe.Pointer(&h))
}

func Suffix(url string) string {
	strs := strings.Split(url, ".")
	if len(strs) == 1 {
		return ""
	}
	return strs[len(strs)-1]
}
