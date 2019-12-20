package utils

import (
	"strings"

	"github.com/PuerkitoBio/goquery"
)

func FetchImage(html string) (image string, err error) {
	gq, err := goquery.NewDocumentFromReader(strings.NewReader(html))
	if err != nil {
		return
	}
	image, _ = gq.Find("body div p img").Eq(0).Attr("src")
	return
}
