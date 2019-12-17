package utils

func SortSwap(a, b int) (int, int) {
	if a > b {
		return b, a
	}
	return a, b
}
