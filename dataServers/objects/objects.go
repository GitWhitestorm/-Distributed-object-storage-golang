package objects

import (
	"Distributed-object-storage-golang/dataServers/locate"
	"crypto/md5"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
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
	files,_ := filepath.Glob(os.Getenv("STORAGE_ROOT")+"/objects/"+ hash + ".*")
	if len(files) != 1{
		return ""
	}
	file := files[0]
	h := md5.New()
	sendFile(h,file)
	d := string(h.Sum(nil))
	hd := strings.Split(file,".")[2]
	if d != hd{
		log.Println("object hash mismatch,remove",file)
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
