package utils

import (
	"fmt"
	"math/rand"
	"os"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var (
	allchars    = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	allcharsLen = len(allchars)
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func CreateSign(sign string) int64 {
	md5Code := Md5(sign)
	res, _ := strconv.ParseInt(fmt.Sprintf("%x", md5Code)[1:15], 16, 64)
	return res
}

func ExcludeIntArray(src []int, exclude []int) (res []int) {
	res = make([]int, 0, len(src))

	var mp = make(map[int]bool)
	for _, id := range exclude {
		mp[id] = true
	}

	for _, id := range src {
		if _, ok := mp[id]; ok {
			continue
		}
		res = append(res, id)
	}
	return
}

//通用方法,反射效率不高
func Contains(target interface{}, obj interface{}) bool {
	targetValue := reflect.ValueOf(target)
	switch targetValue.Kind() {
	case reflect.Slice, reflect.Array:
		for i := 0; i < targetValue.Len(); i++ {
			if targetValue.Index(i).Interface() == obj {
				return true
			}
		}
	}
	return false
}

func UniqSlice64(a []int64) []int64 {
	var res = make([]int64, len(a))
	var mp = make(map[int64]bool)
	var index = 0
	for _, i := range a {
		if !mp[i] {
			res[index] = i
			index++
		}
		mp[i] = true
	}
	return res[:index]
}

func UniqSlice(a []int) []int {
	var res = make([]int, len(a))
	var mp = make(map[int]bool)
	var index = 0
	for _, i := range a {
		if !mp[i] {
			res[index] = i
			index++
		}
		mp[i] = true
	}
	return res[:index]
}

func UniqStringSlice(a []string) []string {
	var res = make([]string, len(a))
	var mp = make(map[string]bool)
	var index = 0
	for _, i := range a {
		if !mp[i] {
			res[index] = i
			index++
		}
		mp[i] = true
	}
	return res[:index]
}

func SortStringSlice(a []string) []string {
	var uniqSlice = UniqStringSlice(a)
	var length = len(uniqSlice)
	for i := 0; i < length; i++ {
		for j := 1; j < length; j++ {
			var s1 = CreateSign(uniqSlice[i])
			var s2 = CreateSign(uniqSlice[j])
			if s1 > s2 {
				var tmp = uniqSlice[i]
				uniqSlice[i] = uniqSlice[j]
				uniqSlice[j] = tmp
			}
		}
	}
	return uniqSlice
}

func IntArrayToStr(a []int) string {
	var res string = "["
	for i, item := range a {
		var str = strconv.FormatInt(int64(item), 10)
		if i == 0 {
			res = res + str
		} else {
			res = res + "," + str
		}
	}
	return res + "]"
}

func InIntArray(i int, ints []int) bool {
	for _, v := range ints {
		if i == v {
			return true
		}
	}
	return false
}

func InStringArray(s string, strs []string) bool {
	for _, v := range strs {
		if s == v {
			return true
		}
	}
	return false
}

//数组去重
func UniqueIntArray(a []int) []int {
	al := len(a)
	if al == 0 {
		return a
	}

	ret := make([]int, al)
	index := 0

loopa:
	for i := 0; i < al; i++ {
		for j := 0; j < index; j++ {
			if a[i] == ret[j] {
				continue loopa
			}
		}
		ret[index] = a[i]
		index++
	}

	return ret[:index]
}

func GetMapIntKeys(m map[int]interface{}) []int {
	keys := make([]int, 0)
	for k, _ := range m {
		keys = append(keys, k)
	}
	return keys
}

//(?i)\b((?:[a-z][\w-]+:(?:/{1,3}|[a-z0-9%])|www\d{0,3}[.]|[a-z0-9.\-]+[.][a-z]{2,4}/)(?:[^\s()<>]+|\(([^\s()<>]+|(\([^\s()<>]+\)))*\))+(?:\(([^\s()<>]+|(\([^\s()<>]+\)))*\)|[^\s`!()\[\]{};:\'".,<>?«»“”‘’]))
var urlPattern = regexp.MustCompile("^(http|https)://.*")

func IsValidURL(url string) bool {
	return urlPattern.MatchString(url)
}

var dateLayout = "2006-01-02"

func ParseDate(str string) int64 {
	t, err := time.Parse(dateLayout, str)
	if err != nil {
		return 0
	}
	return t.Unix()
}

//将idsStr转换为[]int格式  2,3,4 -> []int{2,3,4}
func StrToIntArray(str string) []int {
	var strs = strings.Split(strings.TrimSpace(str), ",")
	var ids = make([]int, 0, len(strs))
	for _, idStr := range strs {
		id, err := strconv.Atoi(idStr)
		if err != nil {
			continue
		}
		ids = append(ids, id)
	}
	return ids
}

//将strings转换为[]string格式  "a,b,c" -> []string{a,b,c}
func StrToStringArray(str string) []string {
	var strs = strings.Split(strings.TrimSpace(str), ",")
	var result = make([]string, 0, len(strs))
	for _, s := range strs {
		if len(s) > 0 {
			result = append(result, s)
		}
	}
	return result
}

func BytesEqual(a, b []byte) bool {
	al := len(a)
	if al != len(b) {
		return false
	}

	for i := 0; i < al; i++ {
		if a[i] != b[i] {
			return false
		}
	}

	return true
}

func SaveAsFile(b []byte, filepath string) (int, error) {
	f, err := os.Create(filepath)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	n, err := f.Write(b)
	if err != nil {
		os.Remove(filepath)
	}
	return n, err
}

func RandString(strlen int) string {
	b := make([]byte, strlen)
	for i := 0; i < strlen; i++ {
		b[i] = allchars[rand.Intn(allcharsLen)]
	}
	return string(b)
}
