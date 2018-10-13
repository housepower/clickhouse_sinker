package utils

// url.QueryEscape 的RFC 2396版本，用于兼容PHP.
func QueryEscapeVRFC2396(s string) string {
	return escape(s)
}

// Return true if the specified character should be escaped when
// appearing in a URL string, according to RFC 2396.
// When 'all' is true the full range of reserved characters are matched.
func shouldEscape(c byte) bool {
	// §2.3 Unreserved characters (alphanum)
	if 'A' <= c && c <= 'Z' || 'a' <= c && c <= 'z' || '0' <= c && c <= '9' {
		return false
	}

	switch c {
	case '-', '_', '.', '!', '~', '*', '\'', '(', ')': // §2.3 Unreserved characters (mark)
		return false

	case ';', '/', '?', ':', '@', '&', '=', '+', '$', ',': // §2.2 Reserved characters (reserved)
		// The RFC reserves (so we must escape) everything.
		return true
	}

	// Everything else must be escaped.
	return true
}

func escape(s string) string {
	hexCount := 0
	for i := 0; i < len(s); i++ {
		c := s[i]
		if shouldEscape(c) {
			hexCount++
		}
	}

	if hexCount == 0 {
		return s
	}

	t := make([]byte, len(s)+2*hexCount)
	j := 0
	for i := 0; i < len(s); i++ {
		switch c := s[i]; {
		case shouldEscape(c):
			t[j] = '%'
			t[j+1] = "0123456789ABCDEF"[c>>4]
			t[j+2] = "0123456789ABCDEF"[c&15]
			j += 3
		default:
			t[j] = s[i]
			j++
		}
	}
	return string(t)
}
