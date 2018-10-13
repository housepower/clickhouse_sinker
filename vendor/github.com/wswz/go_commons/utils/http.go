package utils

import (
	"compress/gzip"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"strings"
)

var (
	forwardList = []string{"X-FORWARDED-FOR", "x-forwarded-for", "X-Forwarded-For"}
)

//请求返回data
func RequestData(r *http.Request) (response *http.Response, data []byte, err error) {
	response, err = http.DefaultClient.Do(r)
	if err != nil {
		return
	}
	data, err = ReadResponse(response)
	return
}

//返回data
func ReadResponse(resp *http.Response) (data []byte, err error) {
	defer resp.Body.Close()
	switch resp.Header.Get("Content-Encoding") {
	case "gzip":
		reader, _ := gzip.NewReader(resp.Body)
		data, err = ioutil.ReadAll(reader)
	default:
		data, err = ioutil.ReadAll(resp.Body)
	}
	return
}

func GetRequestIp(r *http.Request) string {
	var res = r.RemoteAddr

	for _, f := range forwardList {
		if ipProxy := r.Header.Get(f); len(ipProxy) > 0 {
			res = ipProxy
			break
		}
	}
	if ind := strings.Index(res, `,`); ind != -1 {
		res = res[:ind]
	}
	if ind := strings.Index(res, `:`); ind != -1 {
		res = res[:ind]
	}

	return res
}

var localIpSegment = []byte{192, 168}

// 如设置为 127.168，直接用 127, 168 作为变量传入就行
// SetLocalIPSegment(127, 168)
func SetLocalIPSegment(segs ...byte) {
	localIpSegment = []byte{}
	for i := 0; i < len(segs) && i < 4; i++ {
		localIpSegment = append(localIpSegment, segs[i])
	}
}

func GetLocalIPSegment() [2]byte {
	var segs [2]byte
	for i := 0; i < len(localIpSegment) && i < 2; i++ {
		segs[i] = localIpSegment[i]
	}
	return segs
}

// 获取本机 ip
// 默认本地的 ip 段为 192.168，如果不是，调用此方法前先调用 SetLocalIPSegment 方法设置本地 ip 段
func LocalIP() (net.IP, error) {
	tables, err := net.Interfaces()
	if err != nil {
		return nil, err
	}
	for _, t := range tables {
		addrs, err := t.Addrs()
		if err != nil {
			return nil, err
		}
		for _, a := range addrs {
			ipnet, ok := a.(*net.IPNet)
			if !ok {
				continue
			}
			if v4 := ipnet.IP.To4(); v4 != nil {
				var matchd = true
				for i := 0; i < len(localIpSegment); i++ {
					if v4[i] != localIpSegment[i] {
						matchd = false
					}
				}
				if matchd {
					return v4, nil
				}
			}
		}
	}
	return nil, fmt.Errorf("cannot find local IP address")
}
