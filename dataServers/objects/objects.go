package objects

import (
	"Distributed-object-storage-golang/dataServers/locate"
	"Distributed-object-storage-golang/utils"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
)

func Handler(w http.ResponseWriter, r *http.Request) {
	m := r.Method
	if m == http.MethodGet {
		get(w, r)
		return
	}
	w.WriteHeader(http.StatusMethodNotAllowed)
}

// 请求URL：GET/objects/<object_name>
// 获取对象 从$STORAGE_ROOT/objects/<object_name>获取
func get(w http.ResponseWriter, r *http.Request) {
	// 数据校验
	file := getFile(strings.Split(r.URL.EscapedPath(), "/")[2])

	if file == "" {
		w.WriteHeader(http.StatusNotFound)
		return
	}

}
func getFile(hash string) string {
	file := os.Getenv("STORAGE_ROOT") + "/objects/" + hash
	f, _ := os.Open(file)
	d := url.PathEscape(utils.CalculateHash(f))
	f.Close()
	if d != hash {
		log.Println("object hash mismatch,remove", file)
		locate.Delete(hash)
		os.Remove(file)
		return ""
	}
	return file
}
func sendFile(w io.Writer, file string) {
	f, _ := os.Open(file)
	defer f.Close()
	io.Copy(w, f)
}
