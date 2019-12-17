//一些基本的辅助类
package utils

import (
	"crypto/md5"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"os"
	"os/signal"
	"path"
	"reflect"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"
)

var (
	typeTime = reflect.TypeOf(time.Time{})
)

//返回value的最基本type，如*Obj,[]Obj,[]*Obj都会返回Obj
func OriginTypeOf(value interface{}) reflect.Type {
	t := reflect.TypeOf(value)
	for k := t.Kind(); k == reflect.Ptr || k == reflect.Slice; {
		t = t.Elem()
		k = t.Kind()
	}
	return t
}

//加载json配置文件
func LoadJsonFile(filePath string, conf interface{}) error {
	fi, err := os.Stat(filePath)
	if err != nil {
		return err
	} else if fi.IsDir() {
		return errors.New(filePath + " is not a file.")
	}

	var b []byte
	b, err = ioutil.ReadFile(filePath)
	if err != nil {
		return err
	}
	err = json.Unmarshal(b, conf)
	if err != nil {
		return err
	}
	return err
}

// 对字符串进行md5哈希,
// 返回32位小写md5结果
func MD5(s string) string {
	h := md5.New()
	io.WriteString(h, s)
	return hex.EncodeToString(h.Sum(nil))
}

// 对字符串进行md5哈希,
// 返回32位大写写md5结果
func MD5ToUpper(s string) string {
	return strings.ToUpper(MD5(s))
}

// 对字符串进行md5哈希,
// 返回16位小写md5结果
func MD5_16(s string) string {
	return MD5(s)[8:24]
}

// match regexp with string, and return a named group map
// Example:
//   regexp: "(?P<name>[A-Za-z]+)-(?P<age>\\d+)"
//   string: "CGC-30"
//   return: map[string]string{ "name":"CGC", "age":"30" }
func NamedRegexpGroup(str string, reg *regexp.Regexp) (ng map[string]string, matched bool) {
	rst := reg.FindStringSubmatch(str)
	// fmt.Printf("\n%s => %s => %s\n\n", reg, str, rst)
	if len(rst) < 1 {
		return
	}
	ng = make(map[string]string)
	lenRst := len(rst)
	sn := reg.SubexpNames()
	for k, v := range sn {
		// SubexpNames contain the none named group,
		// so must filter v == ""
		if k == 0 || v == "" {
			continue
		}
		if k+1 > lenRst {
			break
		}
		ng[v] = rst[k]
	}
	matched = true
	return
}
func AllNamedRegexpGroup(str string, reg *regexp.Regexp) (ngs []map[string]string, matched bool) {
	rst := reg.FindAllStringSubmatch(str, 1000)
	if len(rst) < 1 {
		return
	}
	ngs = make([]map[string]string, 0)
	var ng map[string]string
	sn := reg.SubexpNames()
	for _, value := range rst {
		ng = make(map[string]string)
		for k, v := range value {
			// SubexpNames contain the none named group,
			// so must filter v == ""
			if k == 0 || v == "" {
				continue
			}
			ng[sn[k]] = value[k]
		}
		if len(ng) != 0 {
			ngs = append(ngs, ng)
		}
	}
	matched = true
	return
}

// 判断 id 是否在 ids 里面
// ids 必须是由小到大的有序 slice
func IsIn(id int, ids []int) bool {
	if len(ids) == 0 {
		return false
	}
	i := sort.Search(len(ids)-1, func(i int) bool {
		return ids[i] >= id
	})

	return ids[i] == id
}

func MatchArrayInt(id int64, ids []int64) bool {
	for _, _id := range ids {
		if id == _id {
			return true
		}
	}
	return false
}

func MatchArray(id string, ids []string) bool {
	if id == "" {
		return false
	}
	for i := 0; i < len(ids); i++ {
		if strings.Contains(id, ids[i]) {
			return true
		}
	}
	return false
}

func Sort(ids []int) []int {
	is := sort.IntSlice(ids)
	is.Sort()
	return is
}

var _level = -1
var once sync.Once

func curFile(addLevel int) string {
	once.Do(func() {
		var filename string
		for i := 0; i < 20; i++ {
			_, filename, _, _ = runtime.Caller(i)
			if strings.HasSuffix(filename, "github.com/sundy-li/go_commons/utils/util.go") {
				_level = i + 1
				break
			}
		}

	})
	_, filename, _, _ := runtime.Caller(_level + addLevel)
	return filename
}

// 获取调用者的当前文件名
func CurFile() string {
	return curFile(1)
}

// 获取调用者的当前文件DIR
func CurDir() string {
	return path.Dir(curFile(1))
}

func TryOpenFile(filePath string, flag int) (err error) {
	if filePath == "" {
		return
	}
	var fd *os.File
	if fd, err = os.OpenFile(filePath, flag, 0666); err != nil {
		return
	}
	defer fd.Close()
	return
}

// 当前的执行的是不是go test
func IsGoTest() bool {
	var filename string
	for i := 0; i < 20; i++ {
		_, filename, _, _ = runtime.Caller(i)
		if strings.HasSuffix(filename, "src/pkg/testing/testing.go") ||
			strings.HasSuffix(filename, "_test/_testmain.go") {
			return true
		}
	}
	return false
}

func Ip2long(ipstr string) uint32 {
	defer func() {
		if r := recover(); r != nil {
			// return uint32(0)
		}
	}()
	ip := net.ParseIP(ipstr)
	if ip == nil {
		return 0
	}
	ip = ip.To4()
	return binary.BigEndian.Uint32(ip)
}

func Long2ip(ip uint32) string {
	return fmt.Sprintf("%d.%d.%d.%d", ip>>24, ip<<8>>24, ip<<16>>24, ip<<24>>24)
	// ipByte := make([]byte, 4)
	// binary.BigEndian.PutUint32(ipByte, ipLong)
	// ip := net.IP(ipByte)
	// return ip.String()
}

func SendExitSign() error {
	proc, err := os.FindProcess(os.Getpid())
	if err != nil {
		return err
	}

	return proc.Signal(os.Kill)
}

func WaitForExitSign() {
	c := make(chan os.Signal, 1)
	//结束，收到ctrl+c 信号
	signal.Notify(c, os.Interrupt, os.Kill, syscall.SIGTERM, syscall.SIGHUP)
	<-c
}

func IsZero(v reflect.Value) bool {
	switch v.Kind() {
	case reflect.String:
		return len(v.String()) == 0
	case reflect.Ptr, reflect.Interface:
		return v.IsNil()
	case reflect.Slice:
		return v.Len() == 0
	case reflect.Map:
		return v.Len() == 0
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return v.Int() == 0
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return v.Uint() == 0
	case reflect.Float32, reflect.Float64:
		return v.Float() == 0
	case reflect.Bool:
		return !v.Bool()
	case reflect.Struct:
		if v.Type() == typeTime {
			return v.Interface().(time.Time).IsZero()
		}
		for i := v.NumField() - 1; i >= 0; i-- {
			if !IsZero(v.Field(i)) {
				return false
			}
		}
		return true
	}
	return false
}

func ToJsonStr(v interface{}) string {
	bytes, _ := json.Marshal(v)
	return string(bytes)
}

func IsDigit(x uint8) bool {
	return '0' <= x && '9' >= x
}

func IsXdigit(x uint8) bool {
	return ('0' <= x && '9' >= x) || ('a' <= x && 'z' >= x)
}
