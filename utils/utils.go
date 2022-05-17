package utils

import (
	"crypto/md5"
	"fmt"
	"io"
	"net/http"
	"strconv"
)

// 从Http头获取hash值
func GetHashFromHeader(h http.Header) string {
	digest := h.Get("digest")

	if len(digest) < 4 {
		return ""
	}
	if digest[:4] != "MD5=" {
		return ""
	}
	return digest[4:]
}

func GetSizeFromHeader(h http.Header) int64 {
	size, _ := strconv.ParseInt(h.Get("content-length"), 0, 64)
	return size
}

// 计算hash值
func CalculateHash(r io.Reader) string {
	h := md5.New()
	io.Copy(h, r)
	return fmt.Sprintf("%x", h.Sum(nil))

}
