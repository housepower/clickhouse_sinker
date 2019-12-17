package utils

import "net"

func GetIp4Byname(host string) (ips []string, err error) {
	addrs, err := net.LookupIP(host)
	if err != nil {
		return
	}
	ips = make([]string, len(addrs))
	for i, addr := range addrs {
		if ipv4 := addr.To4(); ipv4 != nil {
			ips[i] = ipv4.String()
		}
	}
	return
}
