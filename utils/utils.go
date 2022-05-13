package utils

import (
	"net/http"
	"strconv"
)

// 从Http头获取hash值
func GetHashFromHeader(h http.Header) string {
	digest := h.Get("digest")

	if len(digest) < 9 {
		return ""
	}
	if digest[:8] != "SHA-256=" {
		return ""
	}
	return digest[8:]
}

func GetSizeFromHeader(h http.Header) int64 {
	size, _ := strconv.ParseInt(h.Get("content-length"), 0, 64)
	return size
}
