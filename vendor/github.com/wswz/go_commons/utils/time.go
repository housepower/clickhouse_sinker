package utils

import (
	"strconv"
	"time"
)

var (
	ticker = time.NewTicker(time.Second)
	now    = time.Now()
)

func init() {
	go func() {
		for {
			select {
			case <-ticker.C:
				now = time.Now()
			}
		}
	}()
}

func Now() time.Time {
	return now
}

func FmtTimeAsHuman(unix int) string {
	n := int(now.Unix())
	seconds := int(n - unix)
	var interval = seconds / 31536000
	if interval > 1 {
		return strconv.Itoa(interval) + "年前"
	}
	interval = seconds / 2592000
	if interval > 1 {
		return strconv.Itoa(interval) + "月前"
	}
	interval = seconds / 86400
	if interval > 1 {
		return strconv.Itoa(interval) + "天前"
	}
	interval = seconds / 3600
	if interval > 1 {
		return strconv.Itoa(interval) + "小时前"
	}
	interval = seconds / 60
	if interval > 1 {
		return strconv.Itoa(interval) + "分钟前"
	}
	return strconv.Itoa(seconds) + "秒前"
}

func GetCurDate() int {
	i, _ := strconv.Atoi(Now().Format("20060102"))
	return i
}
